import { AsyncLocalStorage } from 'node:async_hooks';
import conf, { booleanConf, logApp } from '../../config/conf';
import { promiseMap } from '../../utils/promiseUtils';
import { createBatchExecutionAdmissionGate } from './batch-execution-admission';
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
  materializationState?: BatchSideEffectMaterializationState;
  metadata: Map<string, unknown>;
  sideEffects: BatchSideEffect[];
  sideEffectKinds: BatchSideEffectKind[];
  writeBoundaryOpen: boolean;
}

interface BatchExecutionScope {
  active: boolean;
  state: BatchExecutionState;
}

interface BatchSideEffectMaterializationState {
  active: boolean;
  activeExecutionsByKind: Map<BatchSideEffectKind, Set<BatchSideEffectActiveExecution>>;
  autoEnrichmentQueuedCount: number;
  completedCount: number;
  completedCountByKind: Map<BatchSideEffectKind, number>;
  inclusiveDurationMsByKind: Map<BatchSideEffectKind, number>;
  orderedQueuedCount: number;
  progressTimer?: NodeJS.Timeout;
  queuedCount: number;
  stage: BatchSideEffectMaterializationStage;
  startedAt: number;
}

interface BatchSideEffectActiveExecution {
  startedAt: number;
}

type BatchSideEffectMaterializationStage = 'ordered' | 'auto_enrichment' | 'complete';

const batchExecutionStorage = new AsyncLocalStorage<BatchExecutionScope>();
const pendingMaterializations = new Set<Promise<void>>();
const BATCH_EXECUTION_PERFORMANCE_LOG = booleanConf('app:performance_logger', false);
const BATCH_EXECUTION_LOG_MESSAGE = '[BATCH] Execution phase';
const BATCH_EXECUTION_PERFORMANCE_TRACE_METADATA_KEY = 'batch.performance-trace-id';
const BATCH_SIDE_EFFECT_PROGRESS_INTERVAL_MS = 5000;
const BATCH_SIDE_EFFECT_DEFAULT_MAX_ACTIVE_AUTO_ENRICHMENTS = 4;
const configuredBatchMaxActiveAutoEnrichments = Number(conf.get('app:concurrency:batch_max_active_auto_enrichments'));
const BATCH_SIDE_EFFECT_MAX_ACTIVE_AUTO_ENRICHMENTS = Number.isInteger(configuredBatchMaxActiveAutoEnrichments)
  && configuredBatchMaxActiveAutoEnrichments > 0
  ? configuredBatchMaxActiveAutoEnrichments
  : BATCH_SIDE_EFFECT_DEFAULT_MAX_ACTIVE_AUTO_ENRICHMENTS;
const batchAutoEnrichmentMaterializationGate = createBatchExecutionAdmissionGate(BATCH_SIDE_EFFECT_MAX_ACTIVE_AUTO_ENRICHMENTS);

const getActiveBatchExecutionState = (): BatchExecutionState | undefined => {
  const scope = batchExecutionStorage.getStore();
  return scope?.active ? scope.state : undefined;
};

const runWithBatchExecutionState = async <T>(
  state: BatchExecutionState,
  execute: () => Promise<T>,
): Promise<T> => {
  const scope: BatchExecutionScope = {
    active: true,
    state,
  };
  return batchExecutionStorage.run(scope, async () => {
    try {
      return await execute();
    } finally {
      scope.active = false;
    }
  });
};

const normalizeBatchExecutionOptions = (options: BatchExecutionOptions = {}): NormalizedBatchExecutionOptions => ({
  executionMode: options.executionMode ?? BatchExecutionMode.Bulk,
  waitUntil: options.waitUntil === BatchWaitUntil.Committed ? BatchWaitUntil.Committed : BatchWaitUntil.Materialized,
});

const countSideEffectKinds = (sideEffectKinds: BatchSideEffectKind[]) => {
  const counts = new Map<BatchSideEffectKind, number>();
  sideEffectKinds.forEach((kind) => {
    counts.set(kind, (counts.get(kind) ?? 0) + 1);
  });
  return Object.fromEntries(counts);
};

const mapSideEffectKindNumbers = (values: Map<BatchSideEffectKind, number>) => {
  return Object.fromEntries(values);
};

const mapActiveSideEffectCounts = (values: Map<BatchSideEffectKind, Set<BatchSideEffectActiveExecution>>) => {
  return Object.fromEntries(Array.from(values.entries()).map(([kind, executions]) => [kind, executions.size]));
};

const mapOldestActiveSideEffectDurationMs = (
  values: Map<BatchSideEffectKind, Set<BatchSideEffectActiveExecution>>,
) => {
  const now = Date.now();
  return Object.fromEntries(Array.from(values.entries()).map(([kind, executions]) => {
    const oldestStartedAt = Math.min(...Array.from(executions, (execution) => execution.startedAt));
    return [kind, now - oldestStartedAt];
  }));
};

const logBatchSideEffectProgress = (state: BatchExecutionState) => {
  const materializationState = state.materializationState;
  if (!BATCH_EXECUTION_PERFORMANCE_LOG || !materializationState?.active) {
    return;
  }
  logApp.info(BATCH_EXECUTION_LOG_MESSAGE, {
    event: 'progress',
    execution_id: state.metadata.get(BATCH_EXECUTION_PERFORMANCE_TRACE_METADATA_KEY),
    phase: 'materialize_side_effects',
    duration_ms: Date.now() - materializationState.startedAt,
    materialization_stage: materializationState.stage,
    active_side_effect_count_by_kind: mapActiveSideEffectCounts(materializationState.activeExecutionsByKind),
    oldest_active_side_effect_duration_ms_by_kind: mapOldestActiveSideEffectDurationMs(materializationState.activeExecutionsByKind),
    completed_side_effect_count: materializationState.completedCount,
    auto_enrichment_materialization_admission: batchAutoEnrichmentMaterializationGate.snapshot(),
    auto_enrichment_queued_side_effect_count: materializationState.autoEnrichmentQueuedCount,
    ordered_queued_side_effect_count: materializationState.orderedQueuedCount,
    queued_side_effect_count: materializationState.queuedCount,
    registered_side_effect_count: state.sideEffectKinds.length,
    side_effect_kind_counts: countSideEffectKinds(state.sideEffectKinds),
    materialized_side_effect_kind_counts: mapSideEffectKindNumbers(materializationState.completedCountByKind),
    materialized_side_effect_inclusive_duration_ms_by_kind: mapSideEffectKindNumbers(materializationState.inclusiveDurationMsByKind),
  });
};

const executeBatchSideEffect = async (state: BatchExecutionState, sideEffect: BatchSideEffect) => {
  const materializationState = state.materializationState;
  if (!materializationState) {
    await sideEffect.execute();
    return;
  }
  const startedAt = Date.now();
  const activeExecution = { startedAt };
  const activeExecutions = materializationState.activeExecutionsByKind.get(sideEffect.kind) ?? new Set();
  activeExecutions.add(activeExecution);
  materializationState.activeExecutionsByKind.set(sideEffect.kind, activeExecutions);
  try {
    await sideEffect.execute();
  } finally {
    materializationState.completedCount += 1;
    materializationState.completedCountByKind.set(
      sideEffect.kind,
      (materializationState.completedCountByKind.get(sideEffect.kind) ?? 0) + 1,
    );
    materializationState.inclusiveDurationMsByKind.set(
      sideEffect.kind,
      (materializationState.inclusiveDurationMsByKind.get(sideEffect.kind) ?? 0) + (Date.now() - startedAt),
    );
    activeExecutions.delete(activeExecution);
    if (activeExecutions.size === 0) {
      materializationState.activeExecutionsByKind.delete(sideEffect.kind);
    }
  }
};

const scheduleBatchSideEffect = async (state: BatchExecutionState, sideEffect: BatchSideEffect): Promise<void> => {
  state.sideEffectKinds.push(sideEffect.kind);
  if (state.materializationState?.active) {
    await executeBatchSideEffect(state, sideEffect);
    return;
  }
  state.sideEffects.push(sideEffect);
};

const executeWrites = async <T>(
  mutations: BatchMutation<T>[],
  state: BatchExecutionState,
): Promise<T[]> => {
  const results: T[] = [];

  for (const mutation of mutations) {
    const result = await mutation.executeWrite();
    results.push(result);
    if (mutation.sideEffects) {
      for (const sideEffect of mutation.sideEffects(result)) {
        await scheduleBatchSideEffect(state, sideEffect);
      }
    }
  }

  return results;
};

const executeAutoEnrichmentSideEffect = async (state: BatchExecutionState, sideEffect: BatchSideEffect) => {
  const releaseAdmission = await batchAutoEnrichmentMaterializationGate.acquire();
  try {
    await executeBatchSideEffect(state, sideEffect);
  } finally {
    releaseAdmission();
  }
};

const materializeSideEffects = async (state: BatchExecutionState) => {
  const queuedSideEffects = state.sideEffects.slice();
  const orderedSideEffects = queuedSideEffects.filter((sideEffect) => sideEffect.kind !== BatchSideEffectKind.AutoEnrichment);
  const autoEnrichmentSideEffects = queuedSideEffects.filter((sideEffect) => sideEffect.kind === BatchSideEffectKind.AutoEnrichment);
  const materializationState: BatchSideEffectMaterializationState = {
    active: true,
    activeExecutionsByKind: new Map(),
    autoEnrichmentQueuedCount: autoEnrichmentSideEffects.length,
    completedCount: 0,
    completedCountByKind: new Map(),
    inclusiveDurationMsByKind: new Map(),
    orderedQueuedCount: orderedSideEffects.length,
    queuedCount: queuedSideEffects.length,
    stage: 'ordered',
    startedAt: Date.now(),
  };
  state.materializationState = materializationState;
  if (BATCH_EXECUTION_PERFORMANCE_LOG) {
    logApp.info(BATCH_EXECUTION_LOG_MESSAGE, {
      event: 'started',
      execution_id: state.metadata.get(BATCH_EXECUTION_PERFORMANCE_TRACE_METADATA_KEY),
      phase: 'materialize_side_effects',
      auto_enrichment_concurrency: BATCH_SIDE_EFFECT_MAX_ACTIVE_AUTO_ENRICHMENTS,
      auto_enrichment_queued_side_effect_count: materializationState.autoEnrichmentQueuedCount,
      ordered_queued_side_effect_count: materializationState.orderedQueuedCount,
      queued_side_effect_count: materializationState.queuedCount,
      registered_side_effect_count: state.sideEffectKinds.length,
      side_effect_kind_counts: countSideEffectKinds(state.sideEffectKinds),
    });
    materializationState.progressTimer = setInterval(() => logBatchSideEffectProgress(state), BATCH_SIDE_EFFECT_PROGRESS_INTERVAL_MS);
    materializationState.progressTimer.unref();
  }
  try {
    for (let index = 0; index < orderedSideEffects.length; index += 1) {
      await executeBatchSideEffect(state, orderedSideEffects[index]);
    }
    materializationState.stage = 'auto_enrichment';
    await promiseMap(
      autoEnrichmentSideEffects,
      (sideEffect) => executeAutoEnrichmentSideEffect(state, sideEffect),
      BATCH_SIDE_EFFECT_MAX_ACTIVE_AUTO_ENRICHMENTS,
    );
  } finally {
    materializationState.stage = 'complete';
    materializationState.active = false;
    if (materializationState.progressTimer) {
      clearInterval(materializationState.progressTimer);
    }
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
  return getActiveBatchExecutionState() !== undefined;
};

export const getBatchExecutionScope = (): object | undefined => {
  return getActiveBatchExecutionState();
};

export const isBatchWriteBoundaryOpen = (): boolean => {
  return getActiveBatchExecutionState()?.writeBoundaryOpen === true;
};

export const getBatchExecutionMetadata = <T>(key: string): T | undefined => {
  return getActiveBatchExecutionState()?.metadata.get(key) as T | undefined;
};

export const setBatchExecutionMetadata = <T>(key: string, value: T): void => {
  const state = getActiveBatchExecutionState();
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
    queued_side_effect_count: state.sideEffects.length,
    registered_side_effect_count: state.sideEffectKinds.length,
    side_effect_count: state.sideEffectKinds.length,
    side_effect_kind_counts: countSideEffectKinds(state.sideEffectKinds),
    auto_enrichment_materialization_admission: batchAutoEnrichmentMaterializationGate.snapshot(),
    auto_enrichment_queued_side_effect_count: state.materializationState?.autoEnrichmentQueuedCount,
    ordered_queued_side_effect_count: state.materializationState?.orderedQueuedCount,
    materialized_side_effect_count: state.materializationState?.completedCount,
    materialized_side_effect_kind_counts: state.materializationState
      ? mapSideEffectKindNumbers(state.materializationState.completedCountByKind)
      : undefined,
    materialized_side_effect_inclusive_duration_ms_by_kind: state.materializationState
      ? mapSideEffectKindNumbers(state.materializationState.inclusiveDurationMsByKind)
      : undefined,
    ...extra,
  });
};

export const registerBatchCommitter = (committer: BatchCommitter): boolean => {
  const state = getActiveBatchExecutionState();
  if (!state) {
    return false;
  }
  state.committers.set(committer.key, committer);
  return true;
};

export const registerBatchFinalizer = (finalizer: BatchFinalizer): boolean => {
  const state = getActiveBatchExecutionState();
  if (!state) {
    return false;
  }
  state.finalizers.set(finalizer.key, finalizer);
  return true;
};

export const registerBatchSideEffect = async (sideEffect: BatchSideEffect): Promise<void> => {
  const state = getActiveBatchExecutionState();
  if (state) {
    await scheduleBatchSideEffect(state, sideEffect);
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
  const existingState = getActiveBatchExecutionState();
  if (existingState) {
    const results = await executeWrites(mutations, existingState);
    return {
      ...normalizedOptions,
      results,
      sideEffectKinds: existingState.sideEffectKinds,
      materialized: false,
    };
  }

  const state: BatchExecutionState = {
    committers: new Map(),
    finalizers: new Map(),
    finalizersRun: false,
    metadata: new Map(),
    sideEffects: [],
    sideEffectKinds: [],
    writeBoundaryOpen: true,
  };
  if (options.performanceTraceId) {
    state.metadata.set(BATCH_EXECUTION_PERFORMANCE_TRACE_METADATA_KEY, options.performanceTraceId);
  }
  return runWithBatchExecutionState(state, async () => {
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
      const materialization = runWithBatchExecutionState(state, () => materializeSideEffects(state))
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
        sideEffectKinds: state.sideEffectKinds,
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
