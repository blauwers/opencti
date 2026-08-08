import { Readable } from 'node:stream';
import {
  execute,
  Kind,
  parse,
  print,
  type DocumentNode,
  type ExecutionResult,
  type FieldNode,
  type GraphQLSchema,
  type OperationDefinitionNode,
  type SelectionSetNode,
  validate,
} from 'graphql';
import Upload from 'graphql-upload/Upload.mjs';
import jsonCanonicalize from 'canonicalize';
import conf, { booleanConf, logApp } from '../../config/conf';
import { FUNCTIONAL_ERROR, FunctionalError, MISSING_REF_ERROR, VALIDATION_ERROR } from '../../config/errors';
import { lockResources } from '../../lock/master-lock';
import type { AuthContext } from '../../types/user';
import { hashSHA256 } from '../../utils/hash';
import {
  BatchEntityCreateCoordinator,
  getBatchEntityCreateCoordinatorGroupId,
  runWithBatchEntityCreateCoordinator,
  waitForBatchEntityCreateCoordinatorPromise,
} from './batch-entity-create-coordinator';
import { createBatchExecutionAdmissionGate } from './batch-execution-admission';
import {
  assertBatchDirectDeliveryContext,
  buildBatchExecutionReceiptLockId,
  buildBatchExecutionReceiptRequestMetadata,
  buildBatchExecutionReceiptRequiresReconciliationError,
  buildBatchExecutionReceiptTerminalFailureError,
  loadBatchExecutionReceipt,
  readBatchExecutionReceiptResultMetadata,
  recordBatchExecutionReceiptCompletion,
  recordBatchExecutionReceiptRequiresReconciliation,
  recordBatchExecutionReceiptStarted,
  recordBatchExecutionReceiptTerminalFailure,
  reserveBatchExecutionReceipt,
  type ReserveBatchExecutionReceiptInput,
} from './batch-execution-receipt-domain';
import { BatchMutationKind, executeBatchMutations, normalizeBatchExecutionOptions, type BatchExecutionOptions, type BatchExecutionResult } from './batch-executor';
import {
  BatchAdmissionErrorCode,
  type BatchDirectDeliveryContext,
  BatchExecutionMode,
  BatchExecutionReceiptFailureProof,
  BatchExecutionReceiptState,
  type BatchExecutionReceipt,
  type BatchExecutionReceiptOperationManifest,
  type BatchExecutionReceiptResultMetadata,
  type BatchGraphqlExecutionPlanInput,
  type BatchGraphqlFileInput,
  type BatchGraphqlOperationInput,
} from './batch-types';

type BatchGraphqlOperationResult = Record<string, unknown> | null | undefined;
export type BatchGraphqlOperationError = {
  code?: string;
  message: string;
  objectId?: string;
  operationIndex: number;
  retryable: boolean;
};
type BatchGraphqlResultBindings = Map<string, unknown>;
type BatchGraphqlRequiredResultPaths = Map<number, Set<string>>;
type BatchGraphqlRequiredResultPathNode = {
  children: Map<string, BatchGraphqlRequiredResultPathNode>;
  paths: Set<string>;
  terminalPaths: Set<string>;
};
type PreparedBatchGraphqlOperation = {
  coalesceCompletedAdd: boolean;
  dependencyOperationIndexes: Set<number>;
  document: DocumentNode;
  executionGroup?: number;
  executionPhase?: number;
  files?: BatchGraphqlFileInput[] | null;
  objectId?: string;
  operationIndex: number;
  operationName?: string | null;
  variables: Record<string, unknown>;
};
type PreparedBatchGraphqlOperationGroup = {
  declaredPhase: number;
  dependencyGroupIds: Set<number>;
  executionPhase: number;
  groupId: number;
  operations: PreparedBatchGraphqlOperation[];
};
export type BatchGraphqlExecutionOptions = BatchExecutionOptions & {
  bundlePlan?: BatchGraphqlExecutionPlanInput;
  directDeliveryContext?: BatchDirectDeliveryContext;
  pruneUnusedResultFields?: boolean;
};
type BatchGraphqlOperationExecution = {
  operationErrors: BatchGraphqlOperationError[];
  results: BatchGraphqlOperationResult[];
};
type BatchGraphqlExecutionResult = BatchExecutionResult<BatchGraphqlOperationResult> & {
  operationErrors: BatchGraphqlOperationError[];
};
type BatchGraphqlOperationExecutionState = {
  allowPartialFailures: boolean;
  coalescedOperationResults: Map<string, {
    coordinatorGroupId?: number;
    promise: Promise<BatchGraphqlOperationResult>;
  }>;
  failedGroupIds: Set<number>;
  failedGroupRetryableById: Map<number, boolean>;
  operationErrors: BatchGraphqlOperationError[];
};
type BatchGraphqlExecutionAdmissionStats = {
  encodedBytes: number;
  operationCount: number;
  weight: number;
};

const BATCH_RESULT_TOKEN_PREFIX = '__opencti_batch_result__';
const BATCH_RESULT_TOKEN_PATTERN = new RegExp(`^${BATCH_RESULT_TOKEN_PREFIX}:(\\d+):(.+)$`);
const BATCH_GRAPHQL_MAX_CONCURRENCY: number = conf.get('elasticsearch:max_concurrency') || 4;
const BATCH_GRAPHQL_DEFAULT_MAX_ACTIVE_EXECUTIONS = 4;
const BATCH_GRAPHQL_DEFAULT_MAX_ACTIVE_GROUPS = 64;
const BATCH_GRAPHQL_DEFAULT_MAX_COORDINATED_GROUPS_PER_WAVE = 1024;
const BATCH_GRAPHQL_ADMISSION_OPERATIONS_PER_WEIGHT = 2000;
const BATCH_GRAPHQL_ADMISSION_BYTES_PER_WEIGHT = 5 * 1024 * 1024;
const BATCH_GRAPHQL_MATERIALIZATION_RELEASED_WEIGHT = 1;
const BATCH_GRAPHQL_ADMISSION_LOG_MESSAGE = '[BATCH] GraphQL execution admission';
const BATCH_GRAPHQL_PHASE_LOG_MESSAGE = '[BATCH] GraphQL operation phase';
const BATCH_GRAPHQL_PERFORMANCE_LOG = booleanConf('app:performance_logger', false);
const configuredBatchGraphqlMaxActiveExecutions = Number(conf.get('app:concurrency:batch_max_active_executions'));
const BATCH_GRAPHQL_MAX_ACTIVE_EXECUTIONS = Number.isInteger(configuredBatchGraphqlMaxActiveExecutions) && configuredBatchGraphqlMaxActiveExecutions > 0
  ? configuredBatchGraphqlMaxActiveExecutions
  : BATCH_GRAPHQL_DEFAULT_MAX_ACTIVE_EXECUTIONS;
const configuredBatchGraphqlMaxActiveGroups = Number(conf.get('app:concurrency:batch_max_active_groups'));
const BATCH_GRAPHQL_MAX_ACTIVE_GROUPS = Number.isInteger(configuredBatchGraphqlMaxActiveGroups) && configuredBatchGraphqlMaxActiveGroups > 0
  ? configuredBatchGraphqlMaxActiveGroups
  : BATCH_GRAPHQL_DEFAULT_MAX_ACTIVE_GROUPS;
const configuredBatchGraphqlMaxCoordinatedGroupsPerWave = Number(conf.get('app:concurrency:batch_max_coordinated_groups_per_wave'));
const BATCH_GRAPHQL_MAX_COORDINATED_GROUPS_PER_WAVE = Number.isInteger(configuredBatchGraphqlMaxCoordinatedGroupsPerWave)
  && configuredBatchGraphqlMaxCoordinatedGroupsPerWave > 0
  ? configuredBatchGraphqlMaxCoordinatedGroupsPerWave
  : BATCH_GRAPHQL_DEFAULT_MAX_COORDINATED_GROUPS_PER_WAVE;
const batchGraphqlExecutionAdmissionGate = createBatchExecutionAdmissionGate(BATCH_GRAPHQL_MAX_ACTIVE_EXECUTIONS);
let batchGraphqlExecutionSequence = 0;
const BATCH_DEPENDENCY_FAILED_CODE = 'BATCH_DEPENDENCY_FAILED';
const BATCH_OPERATION_FAILED_CODE = 'BATCH_OPERATION_FAILED';
const BATCH_LOCK_ERROR_CODE = 'LOCK_ERROR';
const RETRYABLE_PARTIAL_OPERATION_ERROR_CODES = new Set([MISSING_REF_ERROR, BATCH_LOCK_ERROR_CODE]);
const NON_RETRYABLE_PARTIAL_OPERATION_ERROR_CODES = new Set([FUNCTIONAL_ERROR, VALIDATION_ERROR]);

const getStringByteLength = (value: string | null | undefined): number => {
  return value ? Buffer.byteLength(value, 'utf8') : 0;
};

export const getBatchGraphqlExecutionAdmissionWeight = (
  operations: BatchGraphqlOperationInput[],
  maxActiveWeight = BATCH_GRAPHQL_MAX_ACTIVE_EXECUTIONS,
): number => {
  return getBatchGraphqlExecutionAdmissionStats(operations, maxActiveWeight).weight;
};

export const getBatchGraphqlExecutionAdmissionStats = (
  operations: BatchGraphqlOperationInput[],
  maxActiveWeight = BATCH_GRAPHQL_MAX_ACTIVE_EXECUTIONS,
): BatchGraphqlExecutionAdmissionStats => {
  const encodedBytes = operations.reduce((total, operation) => {
    const fileBytes = operation.files?.reduce((filesTotal, file) => {
      return filesTotal
        + getStringByteLength(file.path)
        + getStringByteLength(file.name)
        + getStringByteLength(file.mimeType)
        + getStringByteLength(file.data);
    }, 0) ?? 0;
    return total
      + getStringByteLength(operation.query)
      + getStringByteLength(operation.variables)
      + getStringByteLength(operation.operationName)
      + getStringByteLength(operation.objectId)
      + fileBytes;
  }, 0);
  const operationWeight = Math.ceil(operations.length / BATCH_GRAPHQL_ADMISSION_OPERATIONS_PER_WEIGHT);
  const byteWeight = Math.ceil(encodedBytes / BATCH_GRAPHQL_ADMISSION_BYTES_PER_WEIGHT);
  return {
    encodedBytes,
    operationCount: operations.length,
    weight: Math.min(maxActiveWeight, Math.max(1, operationWeight, byteWeight)),
  };
};

export const buildBatchGraphqlResultToken = (operationIndex: number, path: string[]): string => {
  return `${BATCH_RESULT_TOKEN_PREFIX}:${operationIndex}:${path.join('.')}`;
};

const replaceResultTokens = (value: unknown, resultBindings: BatchGraphqlResultBindings): unknown => {
  if (typeof value === 'string' && resultBindings.has(value)) {
    return resultBindings.get(value);
  }
  if (Array.isArray(value)) {
    return value.map((item) => replaceResultTokens(item, resultBindings));
  }
  if (value !== null && typeof value === 'object') {
    return Object.fromEntries(Object.entries(value).map(([key, item]) => [key, replaceResultTokens(item, resultBindings)]));
  }
  return value;
};

const registerResultBindings = (
  value: unknown,
  operationIndex: number,
  resultBindings: BatchGraphqlResultBindings,
  path: string[] = [],
): void => {
  if (Array.isArray(value)) {
    value.forEach((item, index) => registerResultBindings(item, operationIndex, resultBindings, [...path, String(index)]));
    return;
  }
  if (value !== null && typeof value === 'object') {
    Object.entries(value).forEach(([key, item]) => registerResultBindings(item, operationIndex, resultBindings, [...path, key]));
    return;
  }
  if (path.length > 0) {
    resultBindings.set(buildBatchGraphqlResultToken(operationIndex, path), value);
  }
};

const setValueAtPath = (value: Record<string, unknown>, path: string, replacement: unknown, operationIndex: number): void => {
  const segments = path.split('.').filter((segment) => segment.length > 0);
  if (segments.length === 0) {
    throw FunctionalError('Invalid batch GraphQL file path', { operation_index: operationIndex, path });
  }
  let current: Record<string, unknown> | unknown[] = value;
  for (let index = 0; index < segments.length - 1; index += 1) {
    const segment = segments[index];
    const next = Array.isArray(current) ? current[Number(segment)] : current[segment];
    if (next === null || typeof next !== 'object') {
      throw FunctionalError('Invalid batch GraphQL file path', { operation_index: operationIndex, path });
    }
    current = next as Record<string, unknown> | unknown[];
  }
  const lastSegment = segments[segments.length - 1];
  if (Array.isArray(current)) {
    const listIndex = Number(lastSegment);
    if (!Number.isInteger(listIndex) || listIndex < 0 || listIndex >= current.length) {
      throw FunctionalError('Invalid batch GraphQL file path', { operation_index: operationIndex, path });
    }
    current[listIndex] = replacement;
    return;
  }
  if (!(lastSegment in current)) {
    throw FunctionalError('Invalid batch GraphQL file path', { operation_index: operationIndex, path });
  }
  current[lastSegment] = replacement;
};

const buildUploadValue = (file: BatchGraphqlFileInput) => {
  const upload = new Upload();
  upload.resolve({
    filename: file.name,
    mimetype: file.mimeType,
    encoding: 'base64',
    createReadStream: () => Readable.from(Buffer.from(file.data, 'base64')),
  });
  return upload;
};

const hydrateOperationFiles = (
  variables: Record<string, unknown>,
  files: BatchGraphqlFileInput[] | null | undefined,
  operationIndex: number,
): Record<string, unknown> => {
  if (!files || files.length === 0) {
    return variables;
  }
  files.forEach((file) => setValueAtPath(variables, file.path, buildUploadValue(file), operationIndex));
  return variables;
};

const validateOperationFiles = (
  variables: Record<string, unknown>,
  files: BatchGraphqlFileInput[] | null | undefined,
  operationIndex: number,
): void => {
  if (!files || files.length === 0) {
    return;
  }
  const candidateVariables = structuredClone(variables);
  files.forEach((file) => setValueAtPath(candidateVariables, file.path, null, operationIndex));
};

const collectOperationResultTokenDependencies = (
  value: unknown,
  operationIndex: number,
  dependencies: Set<number> = new Set(),
  requiredResultPaths: BatchGraphqlRequiredResultPaths = new Map(),
): Set<number> => {
  if (typeof value === 'string' && value.startsWith(BATCH_RESULT_TOKEN_PREFIX)) {
    const match = value.match(BATCH_RESULT_TOKEN_PATTERN);
    if (!match) {
      throw FunctionalError('Invalid batch GraphQL result token', { operation_index: operationIndex, token: value });
    }
    const dependencyIndex = Number(match[1]);
    if (dependencyIndex >= operationIndex) {
      throw FunctionalError('Batch GraphQL result token must reference a prior operation', {
        operation_index: operationIndex,
        dependency_operation_index: dependencyIndex,
      });
    }
    dependencies.add(dependencyIndex);
    const dependencyPaths = requiredResultPaths.get(dependencyIndex) ?? new Set<string>();
    dependencyPaths.add(match[2]);
    requiredResultPaths.set(dependencyIndex, dependencyPaths);
    return dependencies;
  }
  if (Array.isArray(value)) {
    value.forEach((item) => collectOperationResultTokenDependencies(item, operationIndex, dependencies, requiredResultPaths));
    return dependencies;
  }
  if (value !== null && typeof value === 'object') {
    Object.values(value).forEach((item) => collectOperationResultTokenDependencies(item, operationIndex, dependencies, requiredResultPaths));
  }
  return dependencies;
};

const invalidResultTokenPath = (operationIndex: number, path: string): never => {
  throw FunctionalError('Invalid batch GraphQL result token path', { operation_index: operationIndex, path });
};

const createRequiredResultPathNode = (): BatchGraphqlRequiredResultPathNode => ({
  children: new Map(),
  paths: new Set(),
  terminalPaths: new Set(),
});

const parseRequiredResultPath = (path: string, operationIndex: number): string[] => {
  const segments = path.split('.');
  if (segments.length === 0 || segments.some((segment) => segment.length === 0)) {
    return invalidResultTokenPath(operationIndex, path);
  }
  const fieldSegments = segments.filter((segment) => !/^\d+$/.test(segment));
  if (fieldSegments.length === 0) {
    return invalidResultTokenPath(operationIndex, path);
  }
  return fieldSegments;
};

const buildRequiredResultPathTree = (
  paths: Set<string>,
  operationIndex: number,
): BatchGraphqlRequiredResultPathNode => {
  const root = createRequiredResultPathNode();
  paths.forEach((path) => {
    let current = root;
    current.paths.add(path);
    parseRequiredResultPath(path, operationIndex).forEach((segment) => {
      const child = current.children.get(segment) ?? createRequiredResultPathNode();
      child.paths.add(path);
      current.children.set(segment, child);
      current = child;
    });
    current.terminalPaths.add(path);
  });
  return root;
};

const getFieldResponseKey = (field: FieldNode): string => field.alias?.value ?? field.name.value;

const getTopLevelMutationFieldName = (operation: PreparedBatchGraphqlOperation): string | undefined => {
  const operationDefinition = operation.document.definitions.find((definition): definition is OperationDefinitionNode => (
    definition.kind === Kind.OPERATION_DEFINITION
  ));
  const topLevelField = operationDefinition?.selectionSet.selections[0];
  return topLevelField?.kind === Kind.FIELD ? topLevelField.name.value : undefined;
};

const selectionSetContainsFragments = (selectionSet: SelectionSetNode): boolean => {
  return selectionSet.selections.some((selection) => {
    if (selection.kind !== Kind.FIELD) {
      return true;
    }
    return selection.selectionSet ? selectionSetContainsFragments(selection.selectionSet) : false;
  });
};

const buildTypenameSelectionSet = (): SelectionSetNode => ({
  kind: Kind.SELECTION_SET,
  selections: [{
    kind: Kind.FIELD,
    name: {
      kind: Kind.NAME,
      value: '__typename',
    },
  }],
});

const pruneRequiredSelectionSet = (
  selectionSet: SelectionSetNode,
  requiredPaths: BatchGraphqlRequiredResultPathNode,
  operationIndex: number,
): SelectionSetNode => {
  const selectedResponseKeys = new Set<string>();
  const selections: FieldNode[] = [];
  selectionSet.selections.forEach((selection) => {
    if (selection.kind !== Kind.FIELD) {
      return;
    }
    const responseKey = getFieldResponseKey(selection);
    selectedResponseKeys.add(responseKey);
    const fieldRequiredPaths = requiredPaths.children.get(responseKey);
    if (!fieldRequiredPaths) {
      return;
    }
    if (selection.selectionSet) {
      const invalidTerminalPath = fieldRequiredPaths.terminalPaths.values().next().value;
      if (invalidTerminalPath) {
        invalidResultTokenPath(operationIndex, invalidTerminalPath);
      }
      selections.push({
        ...selection,
        selectionSet: pruneRequiredSelectionSet(selection.selectionSet, fieldRequiredPaths, operationIndex),
      });
      return;
    }
    const invalidChildPath = fieldRequiredPaths.children.values().next().value?.paths.values().next().value;
    if (invalidChildPath) {
      invalidResultTokenPath(operationIndex, invalidChildPath);
    }
    selections.push(selection);
  });
  requiredPaths.children.forEach((childPaths, responseKey) => {
    if (!selectedResponseKeys.has(responseKey)) {
      invalidResultTokenPath(operationIndex, childPaths.paths.values().next().value as string);
    }
  });
  return {
    ...selectionSet,
    selections,
  };
};

const pruneUnusedResultFields = (
  document: DocumentNode,
  requiredPaths: Set<string> | undefined,
  operationIndex: number,
): DocumentNode => {
  const operationDefinition = document.definitions.find((definition): definition is OperationDefinitionNode => (
    definition.kind === Kind.OPERATION_DEFINITION
  ));
  if (!operationDefinition || document.definitions.some((definition) => definition.kind === Kind.FRAGMENT_DEFINITION)
    || selectionSetContainsFragments(operationDefinition.selectionSet)) {
    return document;
  }
  const topLevelField = operationDefinition.selectionSet.selections[0] as FieldNode;
  // OpenCTI edit mutations are accessor fields whose nested selection performs the
  // write, for example `stixCoreObjectEdit { restrictionOrganizationAdd { id } }`.
  // Replacing that selection with `__typename` would keep the accessor but skip the
  // actual mutation resolver.
  if ((!requiredPaths || requiredPaths.size === 0) && topLevelField.name.value.endsWith('Edit')) {
    return document;
  }
  let nextTopLevelField = topLevelField;
  if (!requiredPaths || requiredPaths.size === 0) {
    if (topLevelField.selectionSet) {
      nextTopLevelField = {
        ...topLevelField,
        selectionSet: buildTypenameSelectionSet(),
      };
    }
  } else {
    const requiredPathTree = buildRequiredResultPathTree(requiredPaths, operationIndex);
    const topLevelResponseKey = getFieldResponseKey(topLevelField);
    const topLevelRequiredPaths = requiredPathTree.children.get(topLevelResponseKey);
    if (!topLevelRequiredPaths) {
      return invalidResultTokenPath(operationIndex, requiredPaths.values().next().value as string);
    }
    if (topLevelField.selectionSet) {
      const invalidTerminalPath = topLevelRequiredPaths.terminalPaths.values().next().value;
      if (invalidTerminalPath) {
        invalidResultTokenPath(operationIndex, invalidTerminalPath);
      }
      nextTopLevelField = {
        ...topLevelField,
        selectionSet: pruneRequiredSelectionSet(topLevelField.selectionSet, topLevelRequiredPaths, operationIndex),
      };
    } else {
      const invalidChildPath = topLevelRequiredPaths.children.values().next().value?.paths.values().next().value;
      if (invalidChildPath) {
        invalidResultTokenPath(operationIndex, invalidChildPath);
      }
    }
    requiredPathTree.children.forEach((childPaths, responseKey) => {
      if (responseKey !== topLevelResponseKey) {
        invalidResultTokenPath(operationIndex, childPaths.paths.values().next().value as string);
      }
    });
  }
  const nextOperationDefinition: OperationDefinitionNode = {
    ...operationDefinition,
    selectionSet: {
      ...operationDefinition.selectionSet,
      selections: [nextTopLevelField],
    },
  };
  return {
    ...document,
    definitions: document.definitions.map((definition) => (
      definition === operationDefinition ? nextOperationDefinition : definition
    )),
  };
};

const parseExecutionCoordinate = (
  value: number | null | undefined,
  field: 'execution_group' | 'execution_phase',
  operationIndex: number,
): number | undefined => {
  if (value === null || value === undefined) {
    return undefined;
  }
  if (!Number.isInteger(value) || value < 0) {
    throw FunctionalError('Invalid batch GraphQL execution metadata', {
      operation_index: operationIndex,
      [field]: value,
    });
  }
  return value;
};

const parseVariables = (variables: string | null | undefined, operationIndex: number): Record<string, unknown> => {
  if (!variables) {
    return {};
  }
  try {
    const parsed = JSON.parse(variables);
    if (parsed === null || Array.isArray(parsed) || typeof parsed !== 'object') {
      throw new Error('variables must be an object');
    }
    return parsed;
  } catch (cause) {
    throw FunctionalError('Invalid batch GraphQL operation variables', { operation_index: operationIndex, cause });
  }
};

const parseObjectId = (objectId: string | null | undefined, operationIndex: number): string | undefined => {
  if (objectId === null || objectId === undefined) {
    return undefined;
  }
  if (typeof objectId !== 'string' || objectId.length === 0) {
    throw FunctionalError('Invalid batch GraphQL object id', { operation_index: operationIndex, object_id: objectId });
  }
  return objectId;
};

const validateOperationDocument = (
  schema: GraphQLSchema,
  operation: BatchGraphqlOperationInput,
  operationIndex: number,
): { document: DocumentNode; variables: Record<string, unknown> } => {
  if (typeof operation.query !== 'string' || operation.query.trim().length === 0) {
    throw FunctionalError('Batch GraphQL operation query cannot be empty', { operation_index: operationIndex });
  }

  let document: DocumentNode;
  try {
    document = parse(operation.query);
  } catch (cause) {
    throw FunctionalError('Invalid batch GraphQL operation document', { operation_index: operationIndex, cause });
  }

  const operationDefinitions = document.definitions.filter((definition) => definition.kind === Kind.OPERATION_DEFINITION);
  if (operationDefinitions.length !== 1 || operationDefinitions[0].operation !== 'mutation') {
    throw FunctionalError('Batch GraphQL operations must contain exactly one mutation operation', { operation_index: operationIndex });
  }
  const topLevelSelections = operationDefinitions[0].selectionSet.selections;
  if (topLevelSelections.length !== 1 || topLevelSelections[0].kind !== Kind.FIELD) {
    throw FunctionalError('Batch GraphQL operations must contain exactly one top-level mutation field', { operation_index: operationIndex });
  }
  if (topLevelSelections[0].name.value === 'batchMutationsExecute') {
    throw FunctionalError('Nested batch GraphQL operation execution is not supported', { operation_index: operationIndex });
  }

  const validationErrors = validate(schema, document);
  if (validationErrors.length > 0) {
    throw FunctionalError('Invalid batch GraphQL operation', {
      operation_index: operationIndex,
      errors: validationErrors.map((error) => error.message),
    });
  }

  return {
    document,
    variables: parseVariables(operation.variables, operationIndex),
  };
};

const executeOperation = async (
  schema: GraphQLSchema,
  context: AuthContext,
  operation: PreparedBatchGraphqlOperation,
  resultBindings: BatchGraphqlResultBindings,
  state: BatchGraphqlOperationExecutionState,
): Promise<BatchGraphqlOperationResult> => {
  const resolvedVariables = hydrateOperationFiles(
    replaceResultTokens(operation.variables, resultBindings) as Record<string, unknown>,
    operation.files,
    operation.operationIndex,
  );
  const executeResolvedOperation = async () => {
    const result = await execute({
      schema,
      document: operation.document,
      contextValue: context,
      operationName: operation.operationName ?? undefined,
      variableValues: resolvedVariables,
    }) as ExecutionResult<BatchGraphqlOperationResult>;
    if (result.errors && result.errors.length > 0) {
      throw FunctionalError('Batch GraphQL operation failed', {
        operation_index: operation.operationIndex,
        errors: result.errors.map((error) => error.message),
        operation_errors: result.errors.map((error) => ({
          extensions: error.extensions,
          message: error.message,
          path: error.path,
        })),
      });
    }
    return result.data;
  };
  const cacheKey = buildCoalescedOperationKey(operation, resolvedVariables);
  let resultPromise: Promise<BatchGraphqlOperationResult>;
  if (!cacheKey) {
    resultPromise = executeResolvedOperation();
  } else {
    const coordinatorGroupId = getBatchEntityCreateCoordinatorGroupId();
    const existing = state.coalescedOperationResults.get(cacheKey);
    if (existing) {
      if (coordinatorGroupId !== undefined && existing.coordinatorGroupId !== coordinatorGroupId) {
        resultPromise = waitForBatchEntityCreateCoordinatorPromise(existing.promise) ?? existing.promise;
      } else {
        resultPromise = existing.promise;
      }
    } else {
      resultPromise = executeResolvedOperation();
      const entry = { coordinatorGroupId, promise: resultPromise };
      state.coalescedOperationResults.set(cacheKey, entry);
      void resultPromise.catch(() => {
        if (state.coalescedOperationResults.get(cacheKey) === entry) {
          state.coalescedOperationResults.delete(cacheKey);
        }
      });
    }
  }
  const data = await resultPromise;
  registerResultBindings(data, operation.operationIndex, resultBindings);
  return data;
};

const buildPartialOperationError = (
  operation: PreparedBatchGraphqlOperation,
  error: any,
): BatchGraphqlOperationError | undefined => {
  const operationErrors = error?.extensions?.data?.operation_errors;
  if (!operation.objectId || !Array.isArray(operationErrors) || operationErrors.length === 0) {
    return undefined;
  }
  const errorCodes = operationErrors.map((operationError) => operationError?.extensions?.code);
  const supportedErrorCodes = new Set([
    ...RETRYABLE_PARTIAL_OPERATION_ERROR_CODES,
    ...NON_RETRYABLE_PARTIAL_OPERATION_ERROR_CODES,
  ]);
  if (errorCodes.some((code) => typeof code !== 'string' || !supportedErrorCodes.has(code))) {
    return undefined;
  }
  const distinctErrorCodes = Array.from(new Set(errorCodes));
  return {
    code: distinctErrorCodes.length === 1 ? distinctErrorCodes[0] : BATCH_OPERATION_FAILED_CODE,
    message: error.message,
    objectId: operation.objectId,
    operationIndex: operation.operationIndex,
    retryable: errorCodes.every((code) => RETRYABLE_PARTIAL_OPERATION_ERROR_CODES.has(code)),
  };
};

const buildDependencyFailedOperationError = (
  group: PreparedBatchGraphqlOperationGroup,
  retryable: boolean,
): BatchGraphqlOperationError => ({
  code: BATCH_DEPENDENCY_FAILED_CODE,
  message: 'Batch GraphQL operation dependency failed',
  objectId: group.operations[0]?.objectId,
  operationIndex: group.operations[0]?.operationIndex ?? 0,
  retryable,
});

const prepareBatchGraphqlOperations = (
  schema: GraphQLSchema,
  operations: BatchGraphqlOperationInput[],
  shouldPruneUnusedResultFields = false,
): PreparedBatchGraphqlOperation[] => {
  const preparedOperations = operations.map((operation, operationIndex) => {
    const prepared = validateOperationDocument(schema, operation, operationIndex);
    validateOperationFiles(prepared.variables, operation.files, operationIndex);
    return {
      ...prepared,
      coalesceCompletedAdd: false,
      dependencyOperationIndexes: new Set<number>(),
      executionGroup: parseExecutionCoordinate(operation.executionGroup, 'execution_group', operationIndex),
      executionPhase: parseExecutionCoordinate(operation.executionPhase, 'execution_phase', operationIndex),
      files: operation.files,
      objectId: parseObjectId(operation.objectId, operationIndex),
      operationIndex,
      operationName: operation.operationName,
    };
  });
  const requiredResultPaths: BatchGraphqlRequiredResultPaths = new Map();
  preparedOperations.forEach((operation) => {
    operation.dependencyOperationIndexes = collectOperationResultTokenDependencies(
      operation.variables,
      operation.operationIndex,
      new Set(),
      requiredResultPaths,
    );
  });
  if (!shouldPruneUnusedResultFields) {
    return preparedOperations;
  }
  return preparedOperations.map((operation) => ({
    ...operation,
    coalesceCompletedAdd: isCoordinatedAddOperation(operation) && !operation.files?.length,
    document: pruneUnusedResultFields(operation.document, requiredResultPaths.get(operation.operationIndex), operation.operationIndex),
  }));
};

const buildBundleExecutionPhaseMap = (bundlePlan: BatchGraphqlExecutionPlanInput | undefined): Map<string, number> | undefined => {
  if (!bundlePlan) {
    return undefined;
  }
  if (bundlePlan.version !== 1 || !Array.isArray(bundlePlan.executionPhases)) {
    throw FunctionalError('Invalid batch GraphQL bundle plan');
  }
  const phasesByObjectId = new Map<string, number>();
  bundlePlan.executionPhases.forEach((phase) => {
    if (!Number.isInteger(phase.phase) || phase.phase < 0 || !Array.isArray(phase.objectIds)) {
      throw FunctionalError('Invalid batch GraphQL bundle plan phase', { phase });
    }
    phase.objectIds.forEach((objectId) => {
      if (typeof objectId !== 'string' || objectId.length === 0 || phasesByObjectId.has(objectId)) {
        throw FunctionalError('Invalid batch GraphQL bundle plan object id', { object_id: objectId });
      }
      phasesByObjectId.set(objectId, phase.phase);
    });
  });
  return phasesByObjectId;
};

const buildSerialOperationGroups = (
  operations: PreparedBatchGraphqlOperation[],
): PreparedBatchGraphqlOperationGroup[] => {
  return operations.map((operation) => ({
    declaredPhase: operation.operationIndex,
    dependencyGroupIds: new Set(),
    executionPhase: operation.operationIndex,
    groupId: operation.operationIndex,
    operations: [operation],
  }));
};

const buildDeclaredOperationGroups = (
  operations: PreparedBatchGraphqlOperation[],
): PreparedBatchGraphqlOperationGroup[] => {
  const groups: PreparedBatchGraphqlOperationGroup[] = [];
  const groupsById = new Map<number, PreparedBatchGraphqlOperationGroup>();
  const operationGroupIds = new Map<number, number>();
  let currentGroup: PreparedBatchGraphqlOperationGroup | undefined;
  let lastDeclaredPhase = -1;

  operations.forEach((operation) => {
    const groupId = operation.executionGroup as number;
    const declaredPhase = operation.executionPhase as number;
    if (!currentGroup || currentGroup.groupId !== groupId) {
      if (groupsById.has(groupId)) {
        throw FunctionalError('Batch GraphQL execution groups must be contiguous', {
          operation_index: operation.operationIndex,
          execution_group: groupId,
        });
      }
      if (declaredPhase < lastDeclaredPhase) {
        throw FunctionalError('Batch GraphQL execution phases must be non-decreasing', {
          operation_index: operation.operationIndex,
          execution_phase: declaredPhase,
        });
      }
      currentGroup = {
        declaredPhase,
        dependencyGroupIds: new Set(),
        executionPhase: declaredPhase,
        groupId,
        operations: [],
      };
      groups.push(currentGroup);
      groupsById.set(groupId, currentGroup);
      lastDeclaredPhase = declaredPhase;
    } else if (currentGroup.declaredPhase !== declaredPhase) {
      throw FunctionalError('Batch GraphQL execution group operations must share one phase', {
        operation_index: operation.operationIndex,
        execution_group: groupId,
      });
    }
    currentGroup.operations.push(operation);
    operationGroupIds.set(operation.operationIndex, groupId);
  });

  groups.forEach((group) => {
    group.operations.forEach((operation) => {
      operation.dependencyOperationIndexes.forEach((dependencyOperationIndex) => {
        const dependencyGroupId = operationGroupIds.get(dependencyOperationIndex);
        if (dependencyGroupId !== undefined && dependencyGroupId !== group.groupId) {
          group.dependencyGroupIds.add(dependencyGroupId);
        }
      });
    });
    group.dependencyGroupIds.forEach((dependencyGroupId) => {
      const dependencyGroup = groupsById.get(dependencyGroupId);
      if (dependencyGroup) {
        group.executionPhase = Math.max(group.executionPhase, dependencyGroup.executionPhase + 1);
      }
    });
  });

  return groups;
};

const collectBundleOperationReferenceObjectIds = (
  value: unknown,
  objectId: string,
  phasesByObjectId: Map<string, number>,
  referencedObjectIds: Set<string> = new Set(),
): Set<string> => {
  if (typeof value === 'string') {
    if (value !== objectId) {
      if (phasesByObjectId.has(value)) {
        referencedObjectIds.add(value);
      }
    }
    return referencedObjectIds;
  }
  if (Array.isArray(value)) {
    value.forEach((item) => collectBundleOperationReferenceObjectIds(item, objectId, phasesByObjectId, referencedObjectIds));
    return referencedObjectIds;
  }
  if (value !== null && typeof value === 'object') {
    Object.values(value).forEach((item) => collectBundleOperationReferenceObjectIds(item, objectId, phasesByObjectId, referencedObjectIds));
  }
  return referencedObjectIds;
};

const buildBundlePlannedOperationGroups = (
  operations: PreparedBatchGraphqlOperation[],
  phasesByObjectId: Map<string, number>,
): PreparedBatchGraphqlOperationGroup[] => {
  const groups: PreparedBatchGraphqlOperationGroup[] = [];
  const latestGroupIdByObjectId = new Map<string, number>();
  const latestPhaseByObjectId = new Map<string, number>();
  const operationGroupIds = new Map<number, number>();
  const completedObjectIds = new Set<string>();
  let nextGroupId = 0;
  let currentGroup: PreparedBatchGraphqlOperationGroup | undefined;
  let currentObjectId: string | undefined;

  operations.forEach((operation) => {
    if (!operation.objectId) {
      throw FunctionalError('Batch GraphQL bundle plan requires object ids for every operation', {
        operation_index: operation.operationIndex,
      });
    }
    const executionPhase = phasesByObjectId.get(operation.objectId);
    if (executionPhase === undefined) {
      throw FunctionalError('Batch GraphQL operation object is missing from bundle plan', {
        operation_index: operation.operationIndex,
        object_id: operation.objectId,
      });
    }

    if (currentObjectId !== undefined && currentObjectId !== operation.objectId) {
      completedObjectIds.add(currentObjectId);
    }
    if (completedObjectIds.has(operation.objectId)) {
      throw FunctionalError('Batch GraphQL bundle object operations must be contiguous', {
        operation_index: operation.operationIndex,
        object_id: operation.objectId,
      });
    }

    const referencedObjectIds = collectBundleOperationReferenceObjectIds(operation.variables, operation.objectId, phasesByObjectId);
    const referencedPhases = Array.from(referencedObjectIds).map((referencedObjectId) => {
      return latestPhaseByObjectId.get(referencedObjectId) ?? phasesByObjectId.get(referencedObjectId) ?? 0;
    });
    const dependencyPhase = Array.from(referencedPhases).reduce((phase, referencedPhase) => Math.max(phase, referencedPhase + 1), executionPhase);
    const effectivePhase = Math.max(dependencyPhase, latestPhaseByObjectId.get(operation.objectId) ?? executionPhase);
    if (!currentGroup || currentObjectId !== operation.objectId || currentGroup.declaredPhase !== effectivePhase) {
      const previousGroupId = latestGroupIdByObjectId.get(operation.objectId);
      currentGroup = {
        declaredPhase: effectivePhase,
        dependencyGroupIds: previousGroupId === undefined ? new Set() : new Set([previousGroupId]),
        executionPhase: effectivePhase,
        groupId: nextGroupId,
        operations: [],
      };
      groups.push(currentGroup);
      latestGroupIdByObjectId.set(operation.objectId, nextGroupId);
      nextGroupId += 1;
    }
    referencedObjectIds.forEach((referencedObjectId) => {
      const dependencyGroupId = latestGroupIdByObjectId.get(referencedObjectId);
      if (dependencyGroupId !== undefined && dependencyGroupId !== currentGroup?.groupId) {
        currentGroup?.dependencyGroupIds.add(dependencyGroupId);
      }
    });
    currentGroup.operations.push({
      ...operation,
      executionGroup: currentGroup.groupId,
      executionPhase: effectivePhase,
    });
    operationGroupIds.set(operation.operationIndex, currentGroup.groupId);
    latestPhaseByObjectId.set(operation.objectId, effectivePhase);
    currentObjectId = operation.objectId;
  });

  const groupsById = new Map(groups.map((group) => [group.groupId, group]));
  groups.forEach((group) => {
    group.operations.forEach((operation) => {
      operation.dependencyOperationIndexes.forEach((dependencyOperationIndex) => {
        const dependencyGroupId = operationGroupIds.get(dependencyOperationIndex);
        if (dependencyGroupId !== undefined && dependencyGroupId !== group.groupId) {
          group.dependencyGroupIds.add(dependencyGroupId);
        }
      });
    });
    group.dependencyGroupIds.forEach((dependencyGroupId) => {
      const dependencyGroup = groupsById.get(dependencyGroupId);
      if (dependencyGroup) {
        group.executionPhase = Math.max(group.executionPhase, dependencyGroup.executionPhase + 1);
      }
    });
  });

  return groups;
};

const buildOperationGroups = (
  operations: PreparedBatchGraphqlOperation[],
  bundlePlan: BatchGraphqlExecutionPlanInput | undefined,
): PreparedBatchGraphqlOperationGroup[] => {
  const phasesByObjectId = buildBundleExecutionPhaseMap(bundlePlan);
  if (phasesByObjectId) {
    return buildBundlePlannedOperationGroups(operations, phasesByObjectId);
  }
  const operationsWithMetadata = operations.filter((operation) => operation.executionGroup !== undefined || operation.executionPhase !== undefined);
  if (operationsWithMetadata.length === 0) {
    return buildSerialOperationGroups(operations);
  }
  if (operationsWithMetadata.length !== operations.length
    || operations.some((operation) => operation.executionGroup === undefined || operation.executionPhase === undefined)) {
    throw FunctionalError('Batch GraphQL execution metadata must be provided for every operation');
  }
  return buildDeclaredOperationGroups(operations);
};

const executeOperationGroup = async (
  schema: GraphQLSchema,
  context: AuthContext,
  group: PreparedBatchGraphqlOperationGroup,
  resultBindings: BatchGraphqlResultBindings,
  results: BatchGraphqlOperationResult[],
  state: BatchGraphqlOperationExecutionState,
): Promise<void> => {
  for (let index = 0; index < group.operations.length; index += 1) {
    const operation = group.operations[index];
    try {
      results[operation.operationIndex] = await executeOperation(schema, context, operation, resultBindings, state);
    } catch (error) {
      const operationError = state.allowPartialFailures ? buildPartialOperationError(operation, error) : undefined;
      if (!operationError) {
        throw error;
      }
      state.failedGroupIds.add(group.groupId);
      state.failedGroupRetryableById.set(group.groupId, operationError.retryable);
      state.operationErrors.push(operationError);
      group.operations.slice(index).forEach((remainingOperation) => {
        results[remainingOperation.operationIndex] = null;
      });
      return;
    }
  }
};

const executeOperationGroupsWithConcurrency = async (
  schema: GraphQLSchema,
  context: AuthContext,
  groups: PreparedBatchGraphqlOperationGroup[],
  resultBindings: BatchGraphqlResultBindings,
  results: BatchGraphqlOperationResult[],
  state: BatchGraphqlOperationExecutionState,
): Promise<void> => {
  let nextGroupIndex = 0;
  const workerCount = Math.min(BATCH_GRAPHQL_MAX_CONCURRENCY, groups.length);
  // Batch writes are stored in AsyncLocalStorage, so keep this boundary on native promises.
  await Promise.all(Array.from({ length: workerCount }, async () => {
    while (nextGroupIndex < groups.length) {
      const groupIndex = nextGroupIndex;
      nextGroupIndex += 1;
      await executeOperationGroup(schema, context, groups[groupIndex], resultBindings, results, state);
    }
  }));
};

const isCoordinatedAddOperation = (operation: PreparedBatchGraphqlOperation): boolean => {
  const fieldName = getTopLevelMutationFieldName(operation);
  return operation.objectId !== undefined
    && fieldName !== undefined
    && fieldName.endsWith('Add')
    && !fieldName.endsWith('RelationsAdd');
};

const buildCoalescedOperationKey = (
  operation: PreparedBatchGraphqlOperation,
  resolvedVariables: Record<string, unknown>,
): string | undefined => {
  if (!operation.coalesceCompletedAdd) {
    return undefined;
  }
  try {
    return jsonCanonicalize({
      document: print(operation.document),
      operationName: operation.operationName ?? null,
      variables: resolvedVariables,
    }) as string;
  } catch {
    return undefined;
  }
};

const containsCoordinatedAddOperation = (group: PreparedBatchGraphqlOperationGroup): boolean => {
  return group.operations.some(isCoordinatedAddOperation);
};

const executeOperationGroupWaveWithEntityCreateCoordinator = async (
  schema: GraphQLSchema,
  context: AuthContext,
  groups: PreparedBatchGraphqlOperationGroup[],
  resultBindings: BatchGraphqlResultBindings,
  results: BatchGraphqlOperationResult[],
  state: BatchGraphqlOperationExecutionState,
): Promise<void> => {
  // Coordinated entity creates do not issue their Elasticsearch writes until the
  // enclosing batch boundary commits. Let every group reach the shared lookup
  // barrier so request-local loaders resolve the phase set-wise, then bound the
  // number of resumed groups so a large phase does not build an unbounded
  // follow-on conflict graph in memory.
  const coordinator = new BatchEntityCreateCoordinator(
    context,
    groups.map((group) => group.groupId),
    BATCH_GRAPHQL_MAX_ACTIVE_GROUPS,
  );
  let firstError: unknown;
  await Promise.all(groups.map(async (group) => {
    try {
      await runWithBatchEntityCreateCoordinator(
        coordinator,
        group.groupId,
        () => executeOperationGroup(schema, context, group, resultBindings, results, state),
      );
    } catch (error) {
      firstError ??= error;
    }
  }));
  await coordinator.close();
  if (firstError) {
    throw firstError;
  }
};

const executeOperationGroupsWithEntityCreateCoordinator = async (
  schema: GraphQLSchema,
  context: AuthContext,
  groups: PreparedBatchGraphqlOperationGroup[],
  resultBindings: BatchGraphqlResultBindings,
  results: BatchGraphqlOperationResult[],
  state: BatchGraphqlOperationExecutionState,
): Promise<void> => {
  for (let offset = 0; offset < groups.length; offset += BATCH_GRAPHQL_MAX_COORDINATED_GROUPS_PER_WAVE) {
    const waveGroups = groups.slice(offset, offset + BATCH_GRAPHQL_MAX_COORDINATED_GROUPS_PER_WAVE);
    await executeOperationGroupWaveWithEntityCreateCoordinator(
      schema,
      context,
      waveGroups,
      resultBindings,
      results,
      state,
    );
  }
};

const executeOperationGroups = async (
  schema: GraphQLSchema,
  context: AuthContext,
  operations: PreparedBatchGraphqlOperation[],
  groups: PreparedBatchGraphqlOperationGroup[],
  resultBindings: BatchGraphqlResultBindings,
  allowPartialFailures: boolean,
  executionId?: string,
): Promise<BatchGraphqlOperationExecution> => {
  const executionStartedAt = Date.now();
  const groupsByPhase = new Map<number, PreparedBatchGraphqlOperationGroup[]>();
  groups.forEach((group) => {
    const phaseGroups = groupsByPhase.get(group.executionPhase) ?? [];
    phaseGroups.push(group);
    groupsByPhase.set(group.executionPhase, phaseGroups);
  });
  const results = new Array<BatchGraphqlOperationResult>(operations.length);
  const state: BatchGraphqlOperationExecutionState = {
    allowPartialFailures,
    coalescedOperationResults: new Map(),
    failedGroupIds: new Set(),
    failedGroupRetryableById: new Map(),
    operationErrors: [],
  };
  const phases = Array.from(groupsByPhase.keys()).sort((left, right) => left - right);
  for (const phase of phases) {
    const phaseStartedAt = Date.now();
    const phaseGroups = groupsByPhase.get(phase) ?? [];
    const runnableGroups: PreparedBatchGraphqlOperationGroup[] = [];
    phaseGroups.forEach((group) => {
      const failedDependencyGroupIds = Array.from(group.dependencyGroupIds).filter((dependencyGroupId) => state.failedGroupIds.has(dependencyGroupId));
      if (failedDependencyGroupIds.length > 0) {
        const retryable = failedDependencyGroupIds.every((dependencyGroupId) => state.failedGroupRetryableById.get(dependencyGroupId) === true);
        state.failedGroupIds.add(group.groupId);
        state.failedGroupRetryableById.set(group.groupId, retryable);
        state.operationErrors.push(buildDependencyFailedOperationError(group, retryable));
        group.operations.forEach((operation) => {
          results[operation.operationIndex] = null;
        });
      } else {
        runnableGroups.push(group);
      }
    });
    const coordinatedGroups = runnableGroups.filter(containsCoordinatedAddOperation);
    const remainingGroups = runnableGroups.filter((group) => !containsCoordinatedAddOperation(group));
    // A single add group still needs coordinator scope because its resolver can
    // launch parallel nested creates that share participant ids.
    if (coordinatedGroups.length > 0) {
      await executeOperationGroupsWithEntityCreateCoordinator(schema, context, coordinatedGroups, resultBindings, results, state);
    }
    if (remainingGroups.length > 0) {
      await executeOperationGroupsWithConcurrency(schema, context, remainingGroups, resultBindings, results, state);
    }
    if (BATCH_GRAPHQL_PERFORMANCE_LOG) {
      logApp.info(BATCH_GRAPHQL_PHASE_LOG_MESSAGE, {
        event: 'completed',
        execution_id: executionId,
        phase,
        duration_ms: Date.now() - phaseStartedAt,
        phase_group_count: phaseGroups.length,
        runnable_group_count: runnableGroups.length,
        coordinated_group_count: coordinatedGroups.length,
        remaining_group_count: remainingGroups.length,
        operation_count: phaseGroups.reduce((total, group) => total + group.operations.length, 0),
        operation_error_count: state.operationErrors.length,
      });
    }
  }
  if (BATCH_GRAPHQL_PERFORMANCE_LOG) {
    logApp.info(BATCH_GRAPHQL_PHASE_LOG_MESSAGE, {
      event: 'completed',
      execution_id: executionId,
      phase: 'total',
      duration_ms: Date.now() - executionStartedAt,
      group_count: groups.length,
      operation_count: operations.length,
      operation_error_count: state.operationErrors.length,
    });
  }
  return {
    operationErrors: state.operationErrors.sort((left, right) => left.operationIndex - right.operationIndex),
    results,
  };
};

const buildBatchExecutionReceiptOperationManifest = (
  operations: PreparedBatchGraphqlOperation[],
): BatchExecutionReceiptOperationManifest[] => operations.map((operation) => ({
  query: print(operation.document),
  variables: operation.variables,
  operationName: operation.operationName ?? null,
  objectId: operation.objectId ?? null,
  executionGroup: operation.executionGroup ?? null,
  executionPhase: operation.executionPhase ?? null,
  files: operation.files?.map((file) => ({
    path: file.path,
    name: file.name,
    mimeType: file.mimeType,
    contentHash: hashSHA256(file.data),
    byteLength: Buffer.byteLength(file.data, 'utf8'),
  })) ?? null,
}));

const getBatchExecutionReceiptErrorCode = (error: unknown): string | undefined => {
  if (error === null || typeof error !== 'object') {
    return undefined;
  }
  const extensions = (error as { extensions?: { code?: unknown } }).extensions;
  return typeof extensions?.code === 'string' ? extensions.code : undefined;
};

const batchExecutionReceiptConflict = (message: string, receipt: BatchExecutionReceipt) => {
  return FunctionalError(message, {
    batch_error_code: BatchAdmissionErrorCode.ExecutionReceiptConflict,
    receipt_id: receipt.internal_id,
    delivery_id: receipt.delivery_id,
    receipt_state: receipt.state,
  });
};

const buildCachedBatchGraphqlExecutionResult = (
  metadata: BatchExecutionReceiptResultMetadata,
): BatchGraphqlExecutionResult => ({
  executionMode: metadata.executionMode,
  waitUntil: metadata.waitUntil,
  results: Array.from({ length: metadata.operationCount }, () => null),
  operationErrors: metadata.operationErrors,
  sideEffectKinds: metadata.sideEffectKinds as BatchGraphqlExecutionResult['sideEffectKinds'],
  materialized: true,
});

const readBatchExecutionReceiptReplay = (
  receipt: BatchExecutionReceipt,
): BatchGraphqlExecutionResult | null => {
  switch (receipt.state) {
    case BatchExecutionReceiptState.Prepared:
      return null;
    case BatchExecutionReceiptState.Completed: {
      const metadata = readBatchExecutionReceiptResultMetadata(receipt);
      if (!metadata) {
        throw batchExecutionReceiptConflict('Batch execution receipt is completed without replay-safe terminal metadata', receipt);
      }
      return buildCachedBatchGraphqlExecutionResult(metadata);
    }
    case BatchExecutionReceiptState.Started:
    case BatchExecutionReceiptState.RequiresReconciliation:
      throw buildBatchExecutionReceiptRequiresReconciliationError(receipt);
    case BatchExecutionReceiptState.FailedTerminal:
      throw buildBatchExecutionReceiptTerminalFailureError(receipt);
    default:
      throw batchExecutionReceiptConflict('Batch execution receipt has an invalid state', receipt);
  }
};

const withBatchExecutionReceiptLock = async <T>(
  deliveryId: string,
  executeWithLock: () => Promise<T>,
): Promise<T> => {
  const lock = await lockResources([buildBatchExecutionReceiptLockId(deliveryId)]);
  try {
    return await executeWithLock();
  } finally {
    await lock.unlock();
  }
};

const reservePreparedBatchExecutionReceipt = async (
  context: AuthContext,
  input: ReserveBatchExecutionReceiptInput,
): Promise<{ receipt: BatchExecutionReceipt; replay: BatchGraphqlExecutionResult | null }> => {
  return withBatchExecutionReceiptLock(input.deliveryId, async () => {
    const receipt = await reserveBatchExecutionReceipt(context, input);
    return {
      receipt,
      replay: readBatchExecutionReceiptReplay(receipt),
    };
  });
};

const startPreparedBatchExecutionReceipt = async (
  context: AuthContext,
  input: ReserveBatchExecutionReceiptInput,
): Promise<{ receipt: BatchExecutionReceipt; replay: BatchGraphqlExecutionResult | null }> => {
  return withBatchExecutionReceiptLock(input.deliveryId, async () => {
    const receipt = await reserveBatchExecutionReceipt(context, input);
    const replay = readBatchExecutionReceiptReplay(receipt);
    if (replay) {
      return { receipt, replay };
    }
    return {
      receipt: await recordBatchExecutionReceiptStarted(context, receipt),
      replay: null,
    };
  });
};

const recordPreparedBatchExecutionReceiptFailure = async (
  context: AuthContext,
  input: ReserveBatchExecutionReceiptInput,
  error: unknown,
): Promise<void> => {
  await withBatchExecutionReceiptLock(input.deliveryId, async () => {
    const receipt = await reserveBatchExecutionReceipt(context, input);
    if (receipt.state !== BatchExecutionReceiptState.Prepared) {
      return;
    }
    await recordBatchExecutionReceiptTerminalFailure(context, receipt, {
      stage: 'BUILD_EXECUTION_GROUPS',
      code: getBatchExecutionReceiptErrorCode(error),
      message: error instanceof Error ? error.message : String(error),
      proof: BatchExecutionReceiptFailureProof.PreStartValidation,
    });
  });
};

const recordStartedBatchExecutionReceiptResult = async (
  context: AuthContext,
  receipt: BatchExecutionReceipt,
  execution: BatchGraphqlExecutionResult,
): Promise<void> => {
  await withBatchExecutionReceiptLock(receipt.delivery_id, async () => {
    const currentReceipt = await loadBatchExecutionReceipt(context, receipt.delivery_id) ?? receipt;
    if (currentReceipt.state === BatchExecutionReceiptState.Completed) {
      readBatchExecutionReceiptReplay(currentReceipt);
      return;
    }
    if (execution.materialized) {
      await recordBatchExecutionReceiptCompletion(context, currentReceipt, {
        operationCount: execution.results.length,
        operationErrors: execution.operationErrors,
        executionMode: execution.executionMode,
        waitUntil: execution.waitUntil,
        sideEffectKinds: execution.sideEffectKinds,
        materialized: true,
      });
      return;
    }
    await recordBatchExecutionReceiptRequiresReconciliation(
      context,
      currentReceipt,
      new Error('Batch execution committed before durable materialization evidence existed'),
    );
  });
};

const recordStartedBatchExecutionReceiptError = async (
  context: AuthContext,
  receipt: BatchExecutionReceipt,
  error: unknown,
): Promise<void> => {
  await withBatchExecutionReceiptLock(receipt.delivery_id, async () => {
    const currentReceipt = await loadBatchExecutionReceipt(context, receipt.delivery_id) ?? receipt;
    if (
      currentReceipt.state === BatchExecutionReceiptState.Completed
      || currentReceipt.state === BatchExecutionReceiptState.FailedTerminal
      || currentReceipt.state === BatchExecutionReceiptState.RequiresReconciliation
    ) {
      return;
    }
    await recordBatchExecutionReceiptRequiresReconciliation(context, currentReceipt, error);
  });
};

export const executeBatchGraphqlOperations = async (
  schema: GraphQLSchema,
  context: AuthContext,
  operations: BatchGraphqlOperationInput[],
  options: BatchGraphqlExecutionOptions = {},
): Promise<BatchGraphqlExecutionResult> => {
  if (!Array.isArray(operations) || operations.length === 0) {
    throw FunctionalError('Batch GraphQL operations cannot be empty');
  }
  batchGraphqlExecutionSequence += 1;
  const executionId = `batch-graphql-${batchGraphqlExecutionSequence}`;
  const admissionStats = getBatchGraphqlExecutionAdmissionStats(operations);
  const admissionRequestedAt = Date.now();
  const admissionSnapshotBefore = BATCH_GRAPHQL_PERFORMANCE_LOG ? batchGraphqlExecutionAdmissionGate.snapshot() : undefined;
  const admissionMetadata = BATCH_GRAPHQL_PERFORMANCE_LOG ? {
    execution_id: executionId,
    sample_object_ids: operations.flatMap((operation) => (operation.objectId ? [operation.objectId] : [])).slice(0, 3),
    bundle_phase_count: options.bundlePlan?.executionPhases.length ?? 0,
  } : {};
  if (BATCH_GRAPHQL_PERFORMANCE_LOG) {
    logApp.info(BATCH_GRAPHQL_ADMISSION_LOG_MESSAGE, {
      event: 'requested',
      ...admissionMetadata,
      operation_count: admissionStats.operationCount,
      encoded_bytes: admissionStats.encodedBytes,
      admission_weight: admissionStats.weight,
      admission_snapshot_before: admissionSnapshotBefore,
    });
  }
  const releaseAdmission = await batchGraphqlExecutionAdmissionGate.acquire(admissionStats.weight);
  const admittedAt = Date.now();
  const materializationAdmissionWeight = Math.max(1, admissionStats.weight - BATCH_GRAPHQL_MATERIALIZATION_RELEASED_WEIGHT);
  if (BATCH_GRAPHQL_PERFORMANCE_LOG) {
    logApp.info(BATCH_GRAPHQL_ADMISSION_LOG_MESSAGE, {
      event: 'admitted',
      ...admissionMetadata,
      operation_count: admissionStats.operationCount,
      encoded_bytes: admissionStats.encodedBytes,
      admission_weight: admissionStats.weight,
      admission_wait_ms: admittedAt - admissionRequestedAt,
      admission_snapshot_before: admissionSnapshotBefore,
      admission_snapshot_after: batchGraphqlExecutionAdmissionGate.snapshot(),
    });
  }
  try {
    const preparedOperations = prepareBatchGraphqlOperations(schema, operations, options.pruneUnusedResultFields);
    const normalizedOptions = normalizeBatchExecutionOptions(options);
    let receiptReservationInput: ReserveBatchExecutionReceiptInput | undefined;
    if (options.directDeliveryContext) {
      const delivery = await assertBatchDirectDeliveryContext(context, options.directDeliveryContext);
      receiptReservationInput = {
        deliveryId: delivery.internal_id,
        submissionId: delivery.submission_id,
        deliveryPayloadFingerprint: delivery.payload_fingerprint,
        executionMode: normalizedOptions.executionMode,
        waitUntil: normalizedOptions.waitUntil,
        requestMetadata: buildBatchExecutionReceiptRequestMetadata({
          delivery,
          executionMode: normalizedOptions.executionMode,
          waitUntil: normalizedOptions.waitUntil,
          batchPlan: options.bundlePlan ?? null,
          operations: buildBatchExecutionReceiptOperationManifest(preparedOperations),
        }),
      };
      const reservation = await reservePreparedBatchExecutionReceipt(context, receiptReservationInput);
      if (reservation.replay) {
        return reservation.replay;
      }
    }
    let operationGroups: PreparedBatchGraphqlOperationGroup[];
    try {
      operationGroups = buildOperationGroups(preparedOperations, options.bundlePlan);
    } catch (error) {
      if (receiptReservationInput) {
        await recordPreparedBatchExecutionReceiptFailure(context, receiptReservationInput, error);
      }
      throw error;
    }
    let startedReceipt: BatchExecutionReceipt | undefined;
    if (receiptReservationInput) {
      const started = await startPreparedBatchExecutionReceipt(context, receiptReservationInput);
      if (started.replay) {
        return started.replay;
      }
      startedReceipt = started.receipt;
    }
    const resultBindings: BatchGraphqlResultBindings = new Map();
    try {
      const execution = await executeBatchMutations<BatchGraphqlOperationExecution>([{
        kind: BatchMutationKind.GraphqlOperation,
        executeWrite: () => executeOperationGroups(
          schema,
          context,
          preparedOperations,
          operationGroups,
          resultBindings,
          normalizedOptions.executionMode !== BatchExecutionMode.Atomic,
          executionId,
        ),
      }], {
        ...options,
        onMaterializationStarted: async () => {
          await options.onMaterializationStarted?.();
          if (materializationAdmissionWeight >= admissionStats.weight) {
            return;
          }
          releaseAdmission.downgrade(materializationAdmissionWeight);
          if (BATCH_GRAPHQL_PERFORMANCE_LOG) {
            logApp.info(BATCH_GRAPHQL_ADMISSION_LOG_MESSAGE, {
              event: 'downgraded',
              ...admissionMetadata,
              operation_count: admissionStats.operationCount,
              encoded_bytes: admissionStats.encodedBytes,
              admission_weight: admissionStats.weight,
              materialization_admission_weight: materializationAdmissionWeight,
              admission_wait_ms: admittedAt - admissionRequestedAt,
              execution_time_ms: Date.now() - admittedAt,
              admission_snapshot_after: batchGraphqlExecutionAdmissionGate.snapshot(),
            });
          }
        },
        performanceTraceId: executionId,
      });
      const result = {
        ...execution,
        operationErrors: execution.results[0].operationErrors,
        results: execution.results[0].results,
      };
      if (startedReceipt) {
        await recordStartedBatchExecutionReceiptResult(context, startedReceipt, result);
      }
      return result;
    } catch (error) {
      if (startedReceipt) {
        try {
          await recordStartedBatchExecutionReceiptError(context, startedReceipt, error);
        } catch (receiptError) {
          logApp.error('Failed to persist batch execution receipt reconciliation state', {
            cause: receiptError,
            delivery_id: startedReceipt.delivery_id,
            original_error: error,
          });
        }
      }
      throw error;
    }
  } finally {
    const completedAt = Date.now();
    releaseAdmission();
    if (BATCH_GRAPHQL_PERFORMANCE_LOG) {
      logApp.info(BATCH_GRAPHQL_ADMISSION_LOG_MESSAGE, {
        event: 'released',
        ...admissionMetadata,
        operation_count: admissionStats.operationCount,
        encoded_bytes: admissionStats.encodedBytes,
        admission_weight: admissionStats.weight,
        admission_wait_ms: admittedAt - admissionRequestedAt,
        execution_time_ms: completedAt - admittedAt,
        total_time_ms: completedAt - admissionRequestedAt,
        admission_snapshot_after: batchGraphqlExecutionAdmissionGate.snapshot(),
      });
    }
  }
};
