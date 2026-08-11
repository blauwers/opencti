import conf from '../../config/conf';
import { resolveBatchRequestTimeout } from '../../config/conf-utils';
import { getBatchExecutionMetadata, isBatchWriteBoundaryOpen, registerBatchFinalizer, setBatchExecutionMetadata } from './batch-executor';

type BatchRetainedLock = {
  unlock: () => Promise<void>;
};

type BatchRetainedLockState = {
  locks: Set<BatchRetainedLock>;
  participantIdsByDraft: Map<string, Set<string>>;
};

export type BatchLockOptions = {
  draftId?: string;
  retryCount?: number;
  extensionRetryCount?: number;
  releaseRetryCount?: number;
};

const BATCH_RETAINED_LOCKS_METADATA_KEY = 'batch.retained-locks';
const BATCH_REQUEST_TIMEOUT_CONFIG_KEY = 'app:batch_request_timeout';
const BATCH_LOCK_RETRY_COUNT_CONFIG_KEY = 'app:concurrency:batch_retry_count';
const LOCK_RETRY_DELAY_CONFIG_KEY = 'app:concurrency:retry_delay';
const REQUEST_TIMEOUT_CONFIG_KEY = 'app:request_timeout';
const STANDARD_LOCK_RETRY_COUNT_CONFIG_KEY = 'app:concurrency:retry_count';

const getDraftKey = (draftId?: string) => draftId ?? '';

const asNonNegativeInteger = (value: unknown): number | undefined => {
  const parsed = Number(value);
  return Number.isInteger(parsed) && parsed >= 0 ? parsed : undefined;
};

const asPositiveNumber = (value: unknown): number | undefined => {
  const parsed = Number(value);
  return Number.isFinite(parsed) && parsed > 0 ? parsed : undefined;
};

const getBatchRetainedLockState = (): BatchRetainedLockState | undefined => {
  return getBatchExecutionMetadata<BatchRetainedLockState>(BATCH_RETAINED_LOCKS_METADATA_KEY);
};

const releaseBatchRetainedLocks = async (state: BatchRetainedLockState) => {
  await Promise.allSettled(Array.from(state.locks).reverse().map((lock) => lock.unlock()));
};

export const retainBatchLockUntilCommit = (
  lock: BatchRetainedLock,
  participantIds: string[],
  draftId?: string,
): boolean => {
  if (!isBatchWriteBoundaryOpen()) {
    return false;
  }
  let state = getBatchRetainedLockState();
  if (!state) {
    state = {
      locks: new Set(),
      participantIdsByDraft: new Map(),
    };
    setBatchExecutionMetadata(BATCH_RETAINED_LOCKS_METADATA_KEY, state);
    registerBatchFinalizer({
      key: BATCH_RETAINED_LOCKS_METADATA_KEY,
      execute: () => releaseBatchRetainedLocks(state as BatchRetainedLockState),
    });
  }
  state.locks.add(lock);
  const draftKey = getDraftKey(draftId);
  const heldIds = state.participantIdsByDraft.get(draftKey) ?? new Set<string>();
  participantIds.forEach((participantId) => heldIds.add(participantId));
  state.participantIdsByDraft.set(draftKey, heldIds);
  return true;
};

export const getBatchRetainedLockIds = (draftId?: string): string[] => {
  return Array.from(getBatchRetainedLockState()?.participantIdsByDraft.get(getDraftKey(draftId)) ?? []);
};

export const getBatchLongWaitLockOptions = (
  draftId?: string,
): BatchLockOptions => {
  const lockOptions = { draftId };
  const batchRetryCount = asNonNegativeInteger(conf.get(BATCH_LOCK_RETRY_COUNT_CONFIG_KEY));
  if (batchRetryCount === undefined) {
    return lockOptions;
  }
  const standardRetryCount = asNonNegativeInteger(conf.get(STANDARD_LOCK_RETRY_COUNT_CONFIG_KEY));
  return {
    ...lockOptions,
    retryCount: batchRetryCount,
    extensionRetryCount: standardRetryCount,
    releaseRetryCount: 0,
  };
};

export const resolveBatchDirectDeliveryExecutionRetryCount = ({
  batchRequestTimeoutMs,
  batchRetryCount,
  retryDelayMs,
}: {
  batchRequestTimeoutMs: unknown;
  batchRetryCount: unknown;
  retryDelayMs: unknown;
}): number | undefined => {
  const normalizedBatchRetryCount = asNonNegativeInteger(batchRetryCount);
  const normalizedBatchRequestTimeoutMs = asPositiveNumber(batchRequestTimeoutMs);
  const normalizedRetryDelayMs = asPositiveNumber(retryDelayMs);
  if (
    normalizedBatchRetryCount === undefined
    || normalizedBatchRequestTimeoutMs === undefined
    || normalizedRetryDelayMs === undefined
  ) {
    return normalizedBatchRetryCount;
  }
  // Direct-delivery descendants wait before receipt reservation, so they do not
  // have a durable retry boundary yet. Let this serialization gate wait for the
  // full batch request budget instead of failing at the ordinary retained-lock
  // window while a live sibling is still running.
  const requestRetryBudget = Math.floor(normalizedBatchRequestTimeoutMs / normalizedRetryDelayMs);
  return Math.max(normalizedBatchRetryCount, requestRetryBudget);
};

export const getBatchDirectDeliveryExecutionLockOptions = (
  draftId?: string,
): BatchLockOptions => {
  const longWaitLockOptions = getBatchLongWaitLockOptions(draftId);
  const retryCount = resolveBatchDirectDeliveryExecutionRetryCount({
    batchRequestTimeoutMs: resolveBatchRequestTimeout(
      conf.get(REQUEST_TIMEOUT_CONFIG_KEY),
      conf.get(BATCH_REQUEST_TIMEOUT_CONFIG_KEY),
    ),
    batchRetryCount: longWaitLockOptions.retryCount,
    retryDelayMs: conf.get(LOCK_RETRY_DELAY_CONFIG_KEY),
  });
  return retryCount === undefined ? longWaitLockOptions : {
    ...longWaitLockOptions,
    retryCount,
  };
};

export const getBatchAwareLockOptions = (
  draftId?: string,
): BatchLockOptions => {
  if (!isBatchWriteBoundaryOpen()) {
    return { draftId };
  }
  return getBatchLongWaitLockOptions(draftId);
};
