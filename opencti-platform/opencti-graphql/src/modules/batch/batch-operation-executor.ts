import { Readable } from 'node:stream';
import {
  execute,
  Kind,
  parse,
  type DocumentNode,
  type ExecutionResult,
  type FieldNode,
  type GraphQLSchema,
  type OperationDefinitionNode,
  type SelectionSetNode,
  validate,
} from 'graphql';
import Upload from 'graphql-upload/Upload.mjs';
import conf from '../../config/conf';
import { FunctionalError, MISSING_REF_ERROR } from '../../config/errors';
import type { AuthContext } from '../../types/user';
import { BatchEntityCreateCoordinator, runWithBatchEntityCreateCoordinator } from './batch-entity-create-coordinator';
import { BatchMutationKind, executeBatchMutations, type BatchExecutionOptions, type BatchExecutionResult } from './batch-executor';
import { BatchExecutionMode, type BatchGraphqlExecutionPlanInput, type BatchGraphqlFileInput, type BatchGraphqlOperationInput } from './batch-types';

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
type BatchGraphqlExecutionOptions = BatchExecutionOptions & {
  bundlePlan?: BatchGraphqlExecutionPlanInput;
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
  failedGroupIds: Set<number>;
  operationErrors: BatchGraphqlOperationError[];
};

const BATCH_RESULT_TOKEN_PREFIX = '__opencti_batch_result__';
const BATCH_RESULT_TOKEN_PATTERN = new RegExp(`^${BATCH_RESULT_TOKEN_PREFIX}:(\\d+):(.+)$`);
const BATCH_GRAPHQL_MAX_CONCURRENCY: number = conf.get('elasticsearch:max_concurrency') || 4;
const BATCH_DEPENDENCY_FAILED_CODE = 'BATCH_DEPENDENCY_FAILED';

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
): Promise<BatchGraphqlOperationResult> => {
  const resolvedVariables = hydrateOperationFiles(
    replaceResultTokens(operation.variables, resultBindings) as Record<string, unknown>,
    operation.files,
    operation.operationIndex,
  );
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
  registerResultBindings(result.data, operation.operationIndex, resultBindings);
  return result.data;
};

const buildRetryableOperationError = (
  operation: PreparedBatchGraphqlOperation,
  error: any,
): BatchGraphqlOperationError | undefined => {
  const operationErrors = error?.extensions?.data?.operation_errors;
  if (!Array.isArray(operationErrors) || operationErrors.length === 0) {
    return undefined;
  }
  const errorCodes = operationErrors.map((operationError) => operationError?.extensions?.code);
  if (errorCodes.some((code) => code !== MISSING_REF_ERROR)) {
    return undefined;
  }
  return {
    code: MISSING_REF_ERROR,
    message: error.message,
    objectId: operation.objectId,
    operationIndex: operation.operationIndex,
    retryable: true,
  };
};

const buildDependencyFailedOperationError = (
  group: PreparedBatchGraphqlOperationGroup,
): BatchGraphqlOperationError => ({
  code: BATCH_DEPENDENCY_FAILED_CODE,
  message: 'Batch GraphQL operation dependency failed',
  objectId: group.operations[0]?.objectId,
  operationIndex: group.operations[0]?.operationIndex ?? 0,
  retryable: true,
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

const collectBundleOperationReferencePhases = (
  value: unknown,
  objectId: string,
  phasesByObjectId: Map<string, number>,
  referencedPhases: Set<number> = new Set(),
): Set<number> => {
  if (typeof value === 'string') {
    if (value !== objectId) {
      const referencedPhase = phasesByObjectId.get(value);
      if (referencedPhase !== undefined) {
        referencedPhases.add(referencedPhase);
      }
    }
    return referencedPhases;
  }
  if (Array.isArray(value)) {
    value.forEach((item) => collectBundleOperationReferencePhases(item, objectId, phasesByObjectId, referencedPhases));
    return referencedPhases;
  }
  if (value !== null && typeof value === 'object') {
    Object.values(value).forEach((item) => collectBundleOperationReferencePhases(item, objectId, phasesByObjectId, referencedPhases));
  }
  return referencedPhases;
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

    const referencedPhases = collectBundleOperationReferencePhases(operation.variables, operation.objectId, phasesByObjectId);
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
      results[operation.operationIndex] = await executeOperation(schema, context, operation, resultBindings);
    } catch (error) {
      const operationError = state.allowPartialFailures ? buildRetryableOperationError(operation, error) : undefined;
      if (!operationError) {
        throw error;
      }
      state.failedGroupIds.add(group.groupId);
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

const isCoordinatedEntityCreateOperation = (operation: PreparedBatchGraphqlOperation): boolean => {
  const fieldName = getTopLevelMutationFieldName(operation);
  return operation.objectId !== undefined
    && fieldName !== undefined
    && fieldName.endsWith('Add')
    && !fieldName.includes('Relationship')
    && !fieldName.endsWith('RelationsAdd');
};

const canCoordinateEntityCreatePhase = (groups: PreparedBatchGraphqlOperationGroup[]): boolean => {
  return groups.length > 1 && groups.every((group) => group.operations.every(isCoordinatedEntityCreateOperation));
};

const executeOperationGroupsWithEntityCreateCoordinator = async (
  schema: GraphQLSchema,
  context: AuthContext,
  groups: PreparedBatchGraphqlOperationGroup[],
  resultBindings: BatchGraphqlResultBindings,
  results: BatchGraphqlOperationResult[],
  state: BatchGraphqlOperationExecutionState,
): Promise<void> => {
  const coordinator = new BatchEntityCreateCoordinator(context, groups.map((group) => group.groupId), BATCH_GRAPHQL_MAX_CONCURRENCY);
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

const executeOperationGroups = async (
  schema: GraphQLSchema,
  context: AuthContext,
  operations: PreparedBatchGraphqlOperation[],
  resultBindings: BatchGraphqlResultBindings,
  bundlePlan: BatchGraphqlExecutionPlanInput | undefined,
  allowPartialFailures: boolean,
): Promise<BatchGraphqlOperationExecution> => {
  const groups = buildOperationGroups(operations, bundlePlan);
  const groupsByPhase = new Map<number, PreparedBatchGraphqlOperationGroup[]>();
  groups.forEach((group) => {
    const phaseGroups = groupsByPhase.get(group.executionPhase) ?? [];
    phaseGroups.push(group);
    groupsByPhase.set(group.executionPhase, phaseGroups);
  });
  const results = new Array<BatchGraphqlOperationResult>(operations.length);
  const state: BatchGraphqlOperationExecutionState = {
    allowPartialFailures,
    failedGroupIds: new Set(),
    operationErrors: [],
  };
  const phases = Array.from(groupsByPhase.keys()).sort((left, right) => left - right);
  for (const phase of phases) {
    const phaseGroups = groupsByPhase.get(phase) ?? [];
    const runnableGroups: PreparedBatchGraphqlOperationGroup[] = [];
    phaseGroups.forEach((group) => {
      const hasFailedDependency = Array.from(group.dependencyGroupIds).some((dependencyGroupId) => state.failedGroupIds.has(dependencyGroupId));
      if (hasFailedDependency) {
        state.failedGroupIds.add(group.groupId);
        state.operationErrors.push(buildDependencyFailedOperationError(group));
        group.operations.forEach((operation) => {
          results[operation.operationIndex] = null;
        });
      } else {
        runnableGroups.push(group);
      }
    });
    if (canCoordinateEntityCreatePhase(runnableGroups)) {
      await executeOperationGroupsWithEntityCreateCoordinator(schema, context, runnableGroups, resultBindings, results, state);
    } else {
      await executeOperationGroupsWithConcurrency(schema, context, runnableGroups, resultBindings, results, state);
    }
  }
  return {
    operationErrors: state.operationErrors.sort((left, right) => left.operationIndex - right.operationIndex),
    results,
  };
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
  const preparedOperations = prepareBatchGraphqlOperations(schema, operations, options.pruneUnusedResultFields);
  const resultBindings: BatchGraphqlResultBindings = new Map();
  const execution = await executeBatchMutations<BatchGraphqlOperationExecution>([{
    kind: BatchMutationKind.GraphqlOperation,
    executeWrite: () => executeOperationGroups(
      schema,
      context,
      preparedOperations,
      resultBindings,
      options.bundlePlan,
      options.executionMode !== BatchExecutionMode.Atomic,
    ),
  }], options);
  return {
    ...execution,
    operationErrors: execution.results[0].operationErrors,
    results: execution.results[0].results,
  };
};
