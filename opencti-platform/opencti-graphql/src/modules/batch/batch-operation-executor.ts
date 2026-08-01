import { Readable } from 'node:stream';
import { Promise as BluePromise } from 'bluebird';
import { execute, Kind, parse, type DocumentNode, type ExecutionResult, type GraphQLSchema, validate } from 'graphql';
import Upload from 'graphql-upload/Upload.mjs';
import conf from '../../config/conf';
import { FunctionalError } from '../../config/errors';
import type { AuthContext } from '../../types/user';
import { BatchMutationKind, executeBatchMutations, type BatchExecutionOptions, type BatchExecutionResult } from './batch-executor';
import type { BatchGraphqlExecutionPlanInput, BatchGraphqlFileInput, BatchGraphqlOperationInput } from './batch-types';

type BatchGraphqlOperationResult = Record<string, unknown> | null | undefined;
type BatchGraphqlResultBindings = Map<string, unknown>;
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
};

const BATCH_RESULT_TOKEN_PREFIX = '__opencti_batch_result__';
const BATCH_RESULT_TOKEN_PATTERN = new RegExp(`^${BATCH_RESULT_TOKEN_PREFIX}:(\\d+):(.+)$`);
const BATCH_GRAPHQL_MAX_CONCURRENCY: number = conf.get('elasticsearch:max_concurrency') || 4;

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
    return dependencies;
  }
  if (Array.isArray(value)) {
    value.forEach((item) => collectOperationResultTokenDependencies(item, operationIndex, dependencies));
    return dependencies;
  }
  if (value !== null && typeof value === 'object') {
    Object.values(value).forEach((item) => collectOperationResultTokenDependencies(item, operationIndex, dependencies));
  }
  return dependencies;
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
    });
  }
  registerResultBindings(result.data, operation.operationIndex, resultBindings);
  return result.data;
};

const prepareBatchGraphqlOperations = (
  schema: GraphQLSchema,
  operations: BatchGraphqlOperationInput[],
): PreparedBatchGraphqlOperation[] => {
  return operations.map((operation, operationIndex) => {
    const prepared = validateOperationDocument(schema, operation, operationIndex);
    validateOperationFiles(prepared.variables, operation.files, operationIndex);
    const dependencyOperationIndexes = collectOperationResultTokenDependencies(prepared.variables, operationIndex);
    return {
      ...prepared,
      dependencyOperationIndexes,
      executionGroup: parseExecutionCoordinate(operation.executionGroup, 'execution_group', operationIndex),
      executionPhase: parseExecutionCoordinate(operation.executionPhase, 'execution_phase', operationIndex),
      files: operation.files,
      objectId: parseObjectId(operation.objectId, operationIndex),
      operationIndex,
      operationName: operation.operationName,
    };
  });
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

const buildBundlePlannedOperationGroups = (
  operations: PreparedBatchGraphqlOperation[],
  phasesByObjectId: Map<string, number>,
): PreparedBatchGraphqlOperationGroup[] => {
  const groupIdsByObjectId = new Map<string, number>();
  let nextGroupId = 0;
  const operationsWithBackendPlan = operations.map((operation) => {
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
    let executionGroup = groupIdsByObjectId.get(operation.objectId);
    if (executionGroup === undefined) {
      executionGroup = nextGroupId;
      groupIdsByObjectId.set(operation.objectId, executionGroup);
      nextGroupId += 1;
    }
    return {
      ...operation,
      executionGroup,
      executionPhase,
    };
  });
  return buildDeclaredOperationGroups(operationsWithBackendPlan);
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
): Promise<void> => {
  for (const operation of group.operations) {
    results[operation.operationIndex] = await executeOperation(schema, context, operation, resultBindings);
  }
};

const executeOperationGroups = async (
  schema: GraphQLSchema,
  context: AuthContext,
  operations: PreparedBatchGraphqlOperation[],
  resultBindings: BatchGraphqlResultBindings,
  bundlePlan: BatchGraphqlExecutionPlanInput | undefined,
): Promise<BatchGraphqlOperationResult[]> => {
  const groups = buildOperationGroups(operations, bundlePlan);
  const groupsByPhase = new Map<number, PreparedBatchGraphqlOperationGroup[]>();
  groups.forEach((group) => {
    const phaseGroups = groupsByPhase.get(group.executionPhase) ?? [];
    phaseGroups.push(group);
    groupsByPhase.set(group.executionPhase, phaseGroups);
  });
  const results = new Array<BatchGraphqlOperationResult>(operations.length);
  const phases = Array.from(groupsByPhase.keys()).sort((left, right) => left - right);
  for (const phase of phases) {
    await BluePromise.map(
      groupsByPhase.get(phase) ?? [],
      (group) => executeOperationGroup(schema, context, group, resultBindings, results),
      { concurrency: BATCH_GRAPHQL_MAX_CONCURRENCY },
    );
  }
  return results;
};

export const executeBatchGraphqlOperations = async (
  schema: GraphQLSchema,
  context: AuthContext,
  operations: BatchGraphqlOperationInput[],
  options: BatchGraphqlExecutionOptions = {},
): Promise<BatchExecutionResult<BatchGraphqlOperationResult>> => {
  if (!Array.isArray(operations) || operations.length === 0) {
    throw FunctionalError('Batch GraphQL operations cannot be empty');
  }
  const preparedOperations = prepareBatchGraphqlOperations(schema, operations);
  const resultBindings: BatchGraphqlResultBindings = new Map();
  const execution = await executeBatchMutations<BatchGraphqlOperationResult[]>([{
    kind: BatchMutationKind.GraphqlOperation,
    executeWrite: () => executeOperationGroups(schema, context, preparedOperations, resultBindings, options.bundlePlan),
  }], options);
  return {
    ...execution,
    results: execution.results[0],
  };
};
