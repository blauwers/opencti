import conf from '../../config/conf';
import { getBatchExecutionMetadata, isBatchWriteBoundaryOpen, registerBatchFinalizer, setBatchExecutionMetadata } from './batch-executor';

type BatchRetainedLock = {
  unlock: () => Promise<void>;
};

type BatchRetainedLockState = {
  locks: Set<BatchRetainedLock>;
  participantIdsByDraft: Map<string, Set<string>>;
};

const BATCH_RETAINED_LOCKS_METADATA_KEY = 'batch.retained-locks';
const BATCH_LOCK_RETRY_COUNT_CONFIG_KEY = 'app:concurrency:batch_retry_count';
const STANDARD_LOCK_RETRY_COUNT_CONFIG_KEY = 'app:concurrency:retry_count';

const getDraftKey = (draftId?: string) => draftId ?? '';

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

export const getBatchAwareLockOptions = (
  draftId?: string,
): { draftId?: string; retryCount?: number; extensionRetryCount?: number; releaseRetryCount?: number } => {
  const lockOptions = { draftId };
  if (!isBatchWriteBoundaryOpen()) {
    return lockOptions;
  }
  const batchRetryCount = Number(conf.get(BATCH_LOCK_RETRY_COUNT_CONFIG_KEY));
  if (!Number.isInteger(batchRetryCount) || batchRetryCount < 0) {
    return lockOptions;
  }
  const standardRetryCount = Number(conf.get(STANDARD_LOCK_RETRY_COUNT_CONFIG_KEY));
  return {
    ...lockOptions,
    retryCount: batchRetryCount,
    extensionRetryCount: Number.isInteger(standardRetryCount) && standardRetryCount >= 0 ? standardRetryCount : undefined,
    releaseRetryCount: 0,
  };
};
