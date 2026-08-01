import { Readable } from 'node:stream';
import { execute, Kind, parse, type DocumentNode, type ExecutionResult, type GraphQLSchema, validate } from 'graphql';
import Upload from 'graphql-upload/Upload.mjs';
import { FunctionalError } from '../../config/errors';
import type { AuthContext } from '../../types/user';
import { BatchMutationKind, executeBatchMutations, type BatchExecutionOptions, type BatchExecutionResult } from './batch-executor';
import type { BatchGraphqlFileInput, BatchGraphqlOperationInput } from './batch-types';

type BatchGraphqlOperationResult = Record<string, unknown> | null | undefined;
type BatchGraphqlResultBindings = Map<string, unknown>;
type PreparedBatchGraphqlOperation = {
  document: DocumentNode;
  files?: BatchGraphqlFileInput[] | null;
  operationIndex: number;
  operationName?: string | null;
  variables: Record<string, unknown>;
};

const BATCH_RESULT_TOKEN_PREFIX = '__opencti_batch_result__';

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
  return operations.map((operation, operationIndex) => ({
    ...validateOperationDocument(schema, operation, operationIndex),
    files: operation.files,
    operationIndex,
    operationName: operation.operationName,
  }));
};

export const executeBatchGraphqlOperations = async (
  schema: GraphQLSchema,
  context: AuthContext,
  operations: BatchGraphqlOperationInput[],
  options: BatchExecutionOptions = {},
): Promise<BatchExecutionResult<BatchGraphqlOperationResult>> => {
  if (!Array.isArray(operations) || operations.length === 0) {
    throw FunctionalError('Batch GraphQL operations cannot be empty');
  }
  const preparedOperations = prepareBatchGraphqlOperations(schema, operations);
  const resultBindings: BatchGraphqlResultBindings = new Map();
  const mutations = preparedOperations.map((operation) => ({
    kind: BatchMutationKind.GraphqlOperation,
    executeWrite: () => executeOperation(schema, context, operation, resultBindings),
  }));
  return executeBatchMutations(mutations, options);
};
