import { AsyncLocalStorage } from 'node:async_hooks';
import { BatchExecutionMode, BatchWaitUntil } from './batch-types';

export enum BatchMutationKind {
  CreateEntity = 'CREATE_ENTITY',
  CreateRelation = 'CREATE_RELATION',
  UpdateAttribute = 'UPDATE_ATTRIBUTE',
}

export enum BatchSideEffectKind {
  AutoEnrichment = 'AUTO_ENRICHMENT',
  CompatibilityProjection = 'COMPATIBILITY_PROJECTION',
}

export interface BatchSideEffect {
  kind: BatchSideEffectKind;
  execute: () => Promise<void>;
}

export interface BatchMutation<T> {
  kind: BatchMutationKind;
  executeWrite: () => Promise<T>;
  sideEffects?: (result: T) => BatchSideEffect[];
}

export interface BatchExecutionOptions {
  executionMode?: BatchExecutionMode;
  waitUntil?: BatchWaitUntil;
}

export interface BatchExecutionResult<T> {
  executionMode: BatchExecutionMode;
  waitUntil: BatchWaitUntil;
  results: T[];
  sideEffectKinds: BatchSideEffectKind[];
}

interface BatchExecutionState {
  sideEffects: BatchSideEffect[];
}

const batchExecutionStorage = new AsyncLocalStorage<BatchExecutionState>();

const normalizeBatchExecutionOptions = (options: BatchExecutionOptions = {}): Required<BatchExecutionOptions> => ({
  executionMode: options.executionMode ?? BatchExecutionMode.Compatibility,
  waitUntil: options.waitUntil ?? BatchWaitUntil.Materialized,
});

const executeWrites = async <T>(
  mutations: BatchMutation<T>[],
  state: BatchExecutionState,
): Promise<T[]> => {
  const results: T[] = [];

  for (const mutation of mutations) {
    const result = await mutation.executeWrite();
    results.push(result);
    if (mutation.sideEffects) {
      state.sideEffects.push(...mutation.sideEffects(result));
    }
  }

  return results;
};

const materializeSideEffects = async (state: BatchExecutionState) => {
  for (let index = 0; index < state.sideEffects.length; index += 1) {
    await state.sideEffects[index].execute();
  }
};

export const registerBatchSideEffect = async (sideEffect: BatchSideEffect): Promise<void> => {
  const state = batchExecutionStorage.getStore();
  if (state) {
    state.sideEffects.push(sideEffect);
    return;
  }
  await sideEffect.execute();
};

export const executeBatchMutations = async <T>(
  mutations: BatchMutation<T>[],
  options: BatchExecutionOptions = {},
): Promise<BatchExecutionResult<T>> => {
  const normalizedOptions = normalizeBatchExecutionOptions(options);
  const existingState = batchExecutionStorage.getStore();
  if (existingState) {
    const results = await executeWrites(mutations, existingState);
    return {
      ...normalizedOptions,
      results,
      sideEffectKinds: existingState.sideEffects.map((sideEffect) => sideEffect.kind),
    };
  }

  const state: BatchExecutionState = { sideEffects: [] };
  return batchExecutionStorage.run(state, async () => {
    const results = await executeWrites(mutations, state);
    await materializeSideEffects(state);
    return {
      ...normalizedOptions,
      results,
      sideEffectKinds: state.sideEffects.map((sideEffect) => sideEffect.kind),
    };
  });
};

export const executeSingleBatchMutation = async <T>(
  mutation: BatchMutation<T>,
  options: BatchExecutionOptions = {},
): Promise<T> => {
  const execution = await executeBatchMutations([mutation], options);
  return execution.results[0];
};
