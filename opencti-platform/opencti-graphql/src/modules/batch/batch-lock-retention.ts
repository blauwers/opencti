import { getBatchExecutionMetadata, isBatchWriteBoundaryOpen, registerBatchFinalizer, setBatchExecutionMetadata } from './batch-executor';

type BatchRetainedLock = {
  unlock: () => Promise<void>;
};

type BatchRetainedLockState = {
  locks: Set<BatchRetainedLock>;
  participantIdsByDraft: Map<string, Set<string>>;
};

const BATCH_RETAINED_LOCKS_METADATA_KEY = 'batch.retained-locks';

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
