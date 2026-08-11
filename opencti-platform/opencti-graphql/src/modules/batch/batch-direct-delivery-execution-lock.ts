import { lockResources } from '../../lock/master-lock';
import { getBatchDirectDeliveryExecutionLockOptions } from './batch-lock-retention';
import { type BatchDelivery, BatchDeliveryBranchKind } from './batch-types';

type BatchDirectDeliveryExecutionLock = {
  unlock: () => Promise<void>;
};

const BATCH_DIRECT_DELIVERY_EXECUTION_LOCK_PREFIX = 'batch-direct-delivery-execution:';
const BATCH_DIRECT_DELIVERY_SERIALIZED_BRANCH_KINDS = new Set([
  BatchDeliveryBranchKind.LegacySplit,
  BatchDeliveryBranchKind.OversizedChunk,
  BatchDeliveryBranchKind.IntactReplay,
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
  // Child descendants from one submission can overlap on retained entity locks,
  // especially after split or replay handoffs. Keep one child execution active
  // per submission while still allowing unrelated submissions to run in parallel.
  const lock = await lockResources(
    [buildBatchDirectDeliveryExecutionLockId(delivery.submission_id)],
    getBatchDirectDeliveryExecutionLockOptions(),
  );
  return lock as BatchDirectDeliveryExecutionLock;
};
