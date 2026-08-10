import { lockResources } from '../../lock/master-lock';
import { getBatchLongWaitLockOptions } from './batch-lock-retention';
import { type BatchDelivery, BatchDeliveryBranchKind } from './batch-types';

type BatchDirectDeliveryExecutionLock = {
  unlock: () => Promise<void>;
};

const BATCH_DIRECT_DELIVERY_EXECUTION_LOCK_PREFIX = 'batch-direct-delivery-execution:';
const BATCH_DIRECT_DELIVERY_SERIALIZED_BRANCH_KINDS = new Set([
  BatchDeliveryBranchKind.OversizedChunk,
]);

export const buildBatchDirectDeliveryExecutionLockId = (submissionId: string): string => {
  return `${BATCH_DIRECT_DELIVERY_EXECUTION_LOCK_PREFIX}${submissionId}`;
};

export const acquireBatchDirectDeliveryExecutionLock = async (
  delivery: BatchDelivery,
): Promise<BatchDirectDeliveryExecutionLock | undefined> => {
  if (!BATCH_DIRECT_DELIVERY_SERIALIZED_BRANCH_KINDS.has(delivery.branch_kind)) {
    return undefined;
  }
  // Oversized chunks are descendants of a sequential client-side plan. Keep
  // descendants from one submission from retaining overlapping entity locks
  // concurrently while still allowing unrelated submissions to run in parallel.
  const lock = await lockResources(
    [buildBatchDirectDeliveryExecutionLockId(delivery.submission_id)],
    getBatchLongWaitLockOptions(),
  );
  return lock as BatchDirectDeliveryExecutionLock;
};
