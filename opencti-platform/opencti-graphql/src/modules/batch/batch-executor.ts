import { BatchExecutionMode, BatchWaitUntil } from './batch-types';

export enum BatchMutationKind {
  CreateEntity = 'CREATE_ENTITY',
  CreateRelation = 'CREATE_RELATION',
  UpdateAttribute = 'UPDATE_ATTRIBUTE',
}

export interface BatchMutation<T> {
  kind: BatchMutationKind;
  execute: () => Promise<T>;
}

export interface BatchExecutionOptions {
  executionMode?: BatchExecutionMode;
  waitUntil?: BatchWaitUntil;
}

export interface BatchExecutionResult<T> {
  executionMode: BatchExecutionMode;
  waitUntil: BatchWaitUntil;
  results: T[];
}

const normalizeBatchExecutionOptions = (options: BatchExecutionOptions = {}): Required<BatchExecutionOptions> => ({
  executionMode: options.executionMode ?? BatchExecutionMode.Compatibility,
  waitUntil: options.waitUntil ?? BatchWaitUntil.Materialized,
});

export const executeBatchMutations = async <T>(
  mutations: BatchMutation<T>[],
  options: BatchExecutionOptions = {},
): Promise<BatchExecutionResult<T>> => {
  const normalizedOptions = normalizeBatchExecutionOptions(options);
  const results: T[] = [];

  for (const mutation of mutations) {
    results.push(await mutation.execute());
  }

  return {
    ...normalizedOptions,
    results,
  };
};

export const executeSingleBatchMutation = async <T>(
  mutation: BatchMutation<T>,
  options: BatchExecutionOptions = {},
): Promise<T> => {
  const execution = await executeBatchMutations([mutation], options);
  return execution.results[0];
};
