import { execute, Kind, parse, type DocumentNode, type ExecutionResult, type GraphQLSchema, validate } from 'graphql';
import { FunctionalError } from '../../config/errors';
import type { AuthContext } from '../../types/user';
import { BatchMutationKind, executeBatchMutations, type BatchExecutionOptions, type BatchExecutionResult } from './batch-executor';
import type { BatchGraphqlOperationInput } from './batch-types';

type BatchGraphqlOperationResult = Record<string, unknown> | null | undefined;

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
  operation: BatchGraphqlOperationInput,
  operationIndex: number,
): Promise<BatchGraphqlOperationResult> => {
  const { document, variables } = validateOperationDocument(schema, operation, operationIndex);
  const result = await execute({
    schema,
    document,
    contextValue: context,
    operationName: operation.operationName ?? undefined,
    variableValues: variables,
  }) as ExecutionResult<BatchGraphqlOperationResult>;
  if (result.errors && result.errors.length > 0) {
    throw FunctionalError('Batch GraphQL operation failed', {
      operation_index: operationIndex,
      errors: result.errors.map((error) => error.message),
    });
  }
  return result.data;
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
  const mutations = operations.map((operation, operationIndex) => ({
    kind: BatchMutationKind.GraphqlOperation,
    executeWrite: () => executeOperation(schema, context, operation, operationIndex),
  }));
  return executeBatchMutations(mutations, options);
};
