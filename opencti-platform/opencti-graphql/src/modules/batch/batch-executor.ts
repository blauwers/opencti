import { AsyncLocalStorage } from 'node:async_hooks';
import { booleanConf, logApp } from '../../config/conf';
import { BatchExecutionMode, BatchWaitUntil } from './batch-types';

export enum BatchMutationKind {
  CreateEntity = 'CREATE_ENTITY',
  CreateRelation = 'CREATE_RELATION',
  UpdateAttribute = 'UPDATE_ATTRIBUTE',
  DeleteElement = 'DELETE_ELEMENT',
  MergeEntities = 'MERGE_ENTITIES',
  GraphqlOperation = 'GRAPHQL_OPERATION',
}

export enum BatchSideEffectKind {
  AutoEnrichment = 'AUTO_ENRICHMENT',
  CompatibilityProjection = 'COMPATIBILITY_PROJECTION',
  FileLifecycle = 'FILE_LIFECYCLE',
  WorkLifecycle = 'WORK_LIFECYCLE',
  ConnectorDispatch = 'CONNECTOR_DISPATCH',
  StreamPublication = 'STREAM_PUBLICATION',
}

export interface BatchSideEffect {
  kind: BatchSideEffectKind;
  execute: () => Promise<void>;
}

export interface BatchCommitter {
  key: string;
  execute: () => Promise<void>;
}

export interface BatchFinalizer {
  key: string;
  execute: () => Promise<void>;
}

export interface BatchMutation<T> {
  kind: BatchMutationKind;
  executeWrite: () => Promise<T>;
  sideEffects?: (result: T) => BatchSideEffect[];
}

export interface BatchExecutionOptions {
  executionMode?: BatchExecutionMode;
  performanceTraceId?: string;
  waitUntil?: BatchWaitUntil | string;
}

type NormalizedBatchExecutionOptions = {
  executionMode: BatchExecutionMode;
  waitUntil: BatchWaitUntil;
};

export interface BatchExecutionResult<T> {
  executionMode: BatchExecutionMode;
  waitUntil: BatchWaitUntil;
  results: T[];
  sideEffectKinds: BatchSideEffectKind[];
  materialized: boolean;
}

interface BatchExecutionState {
  committers: Map<string, BatchCommitter>;
  finalizers: Map<string, BatchFinalizer>;
  finalizersRun: boolean;
  metadata: Map<string, unknown>;
  sideEffects: BatchSideEffect[];
  writeBoundaryOpen: boolean;
}

const batchExecutionStorage = new AsyncLocalStorage<BatchExecutionState>();
const pendingMaterializations = new Set<Promise<void>>();
const BATCH_EXECUTION_PERFORMANCE_LOG = booleanConf('app:performance_logger', false);
const BATCH_EXECUTION_LOG_MESSAGE = '[BATCH] Execution phase';
const BATCH_EXECUTION_PERFORMANCE_TRACE_METADATA_KEY = 'batch.performance-trace-id';

const normalizeBatchExecutionOptions = (options: BatchExecutionOptions = {}): NormalizedBatchExecutionOptions => ({
  executionMode: options.executionMode ?? BatchExecutionMode.Bulk,
  waitUntil: options.waitUntil === BatchWaitUntil.Committed ? BatchWaitUntil.Committed : BatchWaitUntil.Materialized,
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

const commitWrites = async (state: BatchExecutionState) => {
  for (const committer of state.committers.values()) {
    await committer.execute();
  }
};

const runFinalizers = async (state: BatchExecutionState) => {
  if (state.finalizersRun) {
    return;
  }
  state.finalizersRun = true;
  let firstError: unknown;
  for (const finalizer of state.finalizers.values()) {
    try {
      await finalizer.execute();
    } catch (cause) {
      firstError ??= cause;
    }
  }
  if (firstError) {
    throw firstError;
  }
};

export const hasActiveBatchExecution = (): boolean => {
  return batchExecutionStorage.getStore() !== undefined;
};

export const getBatchExecutionScope = (): object | undefined => {
  return batchExecutionStorage.getStore();
};

export const isBatchWriteBoundaryOpen = (): boolean => {
  return batchExecutionStorage.getStore()?.writeBoundaryOpen === true;
};

export const getBatchExecutionMetadata = <T>(key: string): T | undefined => {
  return batchExecutionStorage.getStore()?.metadata.get(key) as T | undefined;
};

export const setBatchExecutionMetadata = <T>(key: string, value: T): void => {
  const state = batchExecutionStorage.getStore();
  if (state) {
    state.metadata.set(key, value);
  }
};

export const getBatchExecutionPerformanceTraceId = (): string | undefined => {
  return getBatchExecutionMetadata<string>(BATCH_EXECUTION_PERFORMANCE_TRACE_METADATA_KEY);
};

const logBatchExecutionPhase = (
  state: BatchExecutionState,
  phase: string,
  durationMs: number,
  extra: Record<string, unknown> = {},
) => {
  if (!BATCH_EXECUTION_PERFORMANCE_LOG) {
    return;
  }
  logApp.info(BATCH_EXECUTION_LOG_MESSAGE, {
    event: 'completed',
    execution_id: state.metadata.get(BATCH_EXECUTION_PERFORMANCE_TRACE_METADATA_KEY),
    phase,
    duration_ms: durationMs,
    committer_count: state.committers.size,
    finalizer_count: state.finalizers.size,
    side_effect_count: state.sideEffects.length,
    ...extra,
  });
};

export const registerBatchCommitter = (committer: BatchCommitter): boolean => {
  const state = batchExecutionStorage.getStore();
  if (!state) {
    return false;
  }
  state.committers.set(committer.key, committer);
  return true;
};

export const registerBatchFinalizer = (finalizer: BatchFinalizer): boolean => {
  const state = batchExecutionStorage.getStore();
  if (!state) {
    return false;
  }
  state.finalizers.set(finalizer.key, finalizer);
  return true;
};

export const registerBatchSideEffect = async (sideEffect: BatchSideEffect): Promise<void> => {
  const state = batchExecutionStorage.getStore();
  if (state) {
    state.sideEffects.push(sideEffect);
    return;
  }
  await sideEffect.execute();
};

const trackPendingMaterialization = (materialization: Promise<void>) => {
  pendingMaterializations.add(materialization);
  materialization
    .catch((cause) => logApp.error('Batch materialization failed after committed response', { cause }))
    .finally(() => pendingMaterializations.delete(materialization));
};

export const waitForPendingBatchMaterializations = async (): Promise<void> => {
  await Promise.allSettled(Array.from(pendingMaterializations));
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
      materialized: false,
    };
  }

  const state: BatchExecutionState = {
    committers: new Map(),
    finalizers: new Map(),
    finalizersRun: false,
    metadata: new Map(),
    sideEffects: [],
    writeBoundaryOpen: true,
  };
  if (options.performanceTraceId) {
    state.metadata.set(BATCH_EXECUTION_PERFORMANCE_TRACE_METADATA_KEY, options.performanceTraceId);
  }
  return batchExecutionStorage.run(state, async () => {
    const executionStartedAt = Date.now();
    try {
      const writesStartedAt = Date.now();
      const results = await executeWrites(mutations, state);
      logBatchExecutionPhase(state, 'execute_writes', Date.now() - writesStartedAt, {
        mutation_count: mutations.length,
      });
      state.writeBoundaryOpen = false;
      const commitStartedAt = Date.now();
      await commitWrites(state);
      logBatchExecutionPhase(state, 'commit_writes', Date.now() - commitStartedAt);
      const finalizersStartedAt = Date.now();
      await runFinalizers(state);
      logBatchExecutionPhase(state, 'run_finalizers', Date.now() - finalizersStartedAt);
      const materializationStartedAt = Date.now();
      const materialization = materializeSideEffects(state)
        .finally(() => logBatchExecutionPhase(state, 'materialize_side_effects', Date.now() - materializationStartedAt));
      const hasSideEffects = state.sideEffects.length > 0;
      if (normalizedOptions.waitUntil === BatchWaitUntil.Materialized || !hasSideEffects) {
        await materialization;
      } else {
        trackPendingMaterialization(materialization);
      }
      return {
        ...normalizedOptions,
        results,
        sideEffectKinds: state.sideEffects.map((sideEffect) => sideEffect.kind),
        materialized: normalizedOptions.waitUntil === BatchWaitUntil.Materialized || !hasSideEffects,
      };
    } finally {
      state.writeBoundaryOpen = false;
      const finalizersStartedAt = Date.now();
      await runFinalizers(state);
      logBatchExecutionPhase(state, 'finalize_cleanup', Date.now() - finalizersStartedAt);
      logBatchExecutionPhase(state, 'total', Date.now() - executionStartedAt, {
        mutation_count: mutations.length,
      });
    }
  });
};

export const executeSingleBatchMutation = async <T>(
  mutation: BatchMutation<T>,
  options: BatchExecutionOptions = {},
): Promise<T> => {
  const execution = await executeBatchMutations([mutation], options);
  return execution.results[0];
};
