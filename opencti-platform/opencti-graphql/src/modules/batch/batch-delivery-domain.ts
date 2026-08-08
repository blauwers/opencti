import jsonCanonicalize from 'canonicalize';
import { FunctionalError } from '../../config/errors';
import { elIndex, elLoadById, elUpdate } from '../../database/engine';
import { INDEX_INTERNAL_OBJECTS, READ_INDEX_INTERNAL_OBJECTS } from '../../database/utils';
import { BASE_TYPE_ENTITY } from '../../schema/general';
import { getParentTypes } from '../../schema/schemaUtils';
import type { AuthContext } from '../../types/user';
import { SYSTEM_USER } from '../../utils/access';
import { now } from '../../utils/format';
import { hashSHA256 } from '../../utils/hash';
import {
  BatchAdmissionErrorCode,
  type BatchDelivery,
  BatchDeliveryBranchKind,
  type BatchDeliveryEnvelope,
  BatchDeliveryKind,
  BatchDeliveryProtocol,
  BatchDeliveryState,
  type BatchQueueMessage,
  ENTITY_TYPE_BATCH_DELIVERY,
} from './batch-types';

const BATCH_DELIVERY_PREFIX = 'batch-delivery--';
const BATCH_DELIVERY_QUEUE_PAYLOAD_VERSION = 1;
const BATCH_DELIVERY_STATE_ORDER = {
  [BatchDeliveryState.Ready]: 0,
  [BatchDeliveryState.Published]: 1,
};

export interface ReserveBatchDeliveryInput {
  deliveryId: string;
  submissionId: string;
  parentDeliveryId: string | null;
  deliveryKind: BatchDeliveryKind;
  branchKind: BatchDeliveryBranchKind;
  branchSequence: number;
  branchOrdinal: number;
  payloadFingerprint: string;
  queueMessage: BatchQueueMessage;
  requiredWorkerProtocol: BatchDeliveryProtocol;
}

type BatchDeliveryStatePatch = Partial<Pick<BatchDelivery, 'published_at' | 'last_error'>>;

const batchDeliveryConflict = (message: string, data: Record<string, unknown> = {}) => {
  return FunctionalError(message, {
    batch_error_code: BatchAdmissionErrorCode.DeliveryIdentityConflict,
    ...data,
  });
};

const assertNonNegativeInteger = (value: number, field: string) => {
  if (!Number.isInteger(value) || value < 0) {
    throw batchDeliveryConflict('Invalid batch delivery lineage', { [field]: value });
  }
};

const assertDeliveryKindAndBranch = (
  deliveryKind: BatchDeliveryKind,
  parentDeliveryId: string | null,
  branchKind: BatchDeliveryBranchKind,
  branchSequence: number,
  branchOrdinal: number,
) => {
  assertNonNegativeInteger(branchSequence, 'branch_sequence');
  assertNonNegativeInteger(branchOrdinal, 'branch_ordinal');
  if (deliveryKind === BatchDeliveryKind.Root) {
    if (parentDeliveryId !== null || branchKind !== BatchDeliveryBranchKind.Root || branchSequence !== 0 || branchOrdinal !== 0) {
      throw batchDeliveryConflict('Invalid root batch delivery lineage', {
        delivery_kind: deliveryKind,
        parent_delivery_id: parentDeliveryId,
        branch_kind: branchKind,
        branch_sequence: branchSequence,
        branch_ordinal: branchOrdinal,
      });
    }
    return;
  }
  if (deliveryKind !== BatchDeliveryKind.Child || typeof parentDeliveryId !== 'string' || parentDeliveryId.length === 0 || branchKind === BatchDeliveryBranchKind.Root) {
    throw batchDeliveryConflict('Invalid child batch delivery lineage', {
      delivery_kind: deliveryKind,
      parent_delivery_id: parentDeliveryId,
      branch_kind: branchKind,
    });
  }
};

const getBatchDeliveryStateOrder = (state: BatchDeliveryState): number => {
  const order = BATCH_DELIVERY_STATE_ORDER[state];
  if (order === undefined) {
    throw FunctionalError('Invalid batch delivery state', { state });
  }
  return order;
};

const assertDeliveryIdMatchesLineage = (input: ReserveBatchDeliveryInput) => {
  const expectedDeliveryId = input.deliveryKind === BatchDeliveryKind.Root
    ? buildRootBatchDeliveryId(input.submissionId)
    : buildChildBatchDeliveryId(
        input.parentDeliveryId as string,
        input.branchKind as Exclude<BatchDeliveryBranchKind, BatchDeliveryBranchKind.Root>,
        input.branchSequence,
        input.branchOrdinal,
      );
  if (input.deliveryId !== expectedDeliveryId) {
    throw batchDeliveryConflict('Batch delivery id does not match its lineage', {
      delivery_id: input.deliveryId,
      expected_delivery_id: expectedDeliveryId,
    });
  }
};

export const buildBatchDeliveryPayloadFingerprint = (payload: unknown): string => {
  const canonicalPayload = jsonCanonicalize(payload);
  if (typeof canonicalPayload !== 'string') {
    throw FunctionalError('Invalid batch delivery payload');
  }
  return hashSHA256(canonicalPayload);
};

export const buildRootBatchDeliveryId = (submissionId: string): string => {
  return `${BATCH_DELIVERY_PREFIX}${hashSHA256(JSON.stringify([
    submissionId,
    BatchDeliveryBranchKind.Root,
    0,
    0,
  ]))}`;
};

export const buildChildBatchDeliveryId = (
  parentDeliveryId: string,
  branchKind: Exclude<BatchDeliveryBranchKind, BatchDeliveryBranchKind.Root>,
  branchSequence: number,
  branchOrdinal: number,
): string => {
  assertDeliveryKindAndBranch(
    BatchDeliveryKind.Child,
    parentDeliveryId,
    branchKind,
    branchSequence,
    branchOrdinal,
  );
  return `${BATCH_DELIVERY_PREFIX}${hashSHA256(JSON.stringify([
    parentDeliveryId,
    branchKind,
    branchSequence,
    branchOrdinal,
  ]))}`;
};

export const buildRootBatchDeliveryEnvelope = (submissionId: string): BatchDeliveryEnvelope => {
  return {
    delivery_id: buildRootBatchDeliveryId(submissionId),
    parent_delivery_id: null,
    delivery_kind: BatchDeliveryKind.Root,
    delivery_protocol_version: BatchDeliveryProtocol.V2,
    delivery_branch_kind: BatchDeliveryBranchKind.Root,
    delivery_branch_sequence: 0,
    delivery_branch_ordinal: 0,
  };
};

export const buildChildBatchDeliveryEnvelope = (
  parentDeliveryId: string,
  branchKind: Exclude<BatchDeliveryBranchKind, BatchDeliveryBranchKind.Root>,
  branchSequence: number,
  branchOrdinal: number,
): BatchDeliveryEnvelope => {
  return {
    delivery_id: buildChildBatchDeliveryId(parentDeliveryId, branchKind, branchSequence, branchOrdinal),
    parent_delivery_id: parentDeliveryId,
    delivery_kind: BatchDeliveryKind.Child,
    delivery_protocol_version: BatchDeliveryProtocol.V2,
    delivery_branch_kind: branchKind,
    delivery_branch_sequence: branchSequence,
    delivery_branch_ordinal: branchOrdinal,
  };
};

export const loadBatchDelivery = async (context: AuthContext, deliveryId: string): Promise<BatchDelivery | null> => {
  const delivery = await elLoadById(context, SYSTEM_USER, deliveryId, {
    type: ENTITY_TYPE_BATCH_DELIVERY,
    indices: READ_INDEX_INTERNAL_OBJECTS,
  });
  return delivery ? delivery as unknown as BatchDelivery : null;
};

export const assertBatchDeliveryReservation = (
  delivery: BatchDelivery,
  input: ReserveBatchDeliveryInput,
): void => {
  const conflicts = {
    submission_id: delivery.submission_id !== input.submissionId,
    parent_delivery_id: delivery.parent_delivery_id !== input.parentDeliveryId,
    delivery_kind: delivery.delivery_kind !== input.deliveryKind,
    branch_kind: delivery.branch_kind !== input.branchKind,
    branch_sequence: delivery.branch_sequence !== input.branchSequence,
    branch_ordinal: delivery.branch_ordinal !== input.branchOrdinal,
    payload_fingerprint: delivery.payload_fingerprint !== input.payloadFingerprint,
    required_worker_protocol: delivery.required_worker_protocol !== input.requiredWorkerProtocol,
    queue_payload: delivery.queue_payload !== JSON.stringify(input.queueMessage),
  };
  const conflictingFields = Object.entries(conflicts)
    .filter(([, conflict]) => conflict)
    .map(([field]) => field);
  if (conflictingFields.length > 0) {
    throw batchDeliveryConflict('Batch delivery slot is already associated with different immutable data', {
      delivery_id: delivery.internal_id,
      conflicting_fields: conflictingFields,
    });
  }
};

export const reserveBatchDelivery = async (
  context: AuthContext,
  input: ReserveBatchDeliveryInput,
): Promise<BatchDelivery> => {
  assertDeliveryKindAndBranch(
    input.deliveryKind,
    input.parentDeliveryId,
    input.branchKind,
    input.branchSequence,
    input.branchOrdinal,
  );
  assertDeliveryIdMatchesLineage(input);
  const existingDelivery = await loadBatchDelivery(context, input.deliveryId);
  if (existingDelivery) {
    assertBatchDeliveryReservation(existingDelivery, input);
    return existingDelivery;
  }
  const createdAt = now();
  const delivery: BatchDelivery = {
    id: input.deliveryId,
    internal_id: input.deliveryId,
    standard_id: input.deliveryId,
    entity_type: ENTITY_TYPE_BATCH_DELIVERY,
    base_type: BASE_TYPE_ENTITY,
    parent_types: getParentTypes(ENTITY_TYPE_BATCH_DELIVERY),
    submission_id: input.submissionId,
    parent_delivery_id: input.parentDeliveryId,
    delivery_kind: input.deliveryKind,
    branch_kind: input.branchKind,
    branch_sequence: input.branchSequence,
    branch_ordinal: input.branchOrdinal,
    payload_fingerprint: input.payloadFingerprint,
    queue_payload_version: BATCH_DELIVERY_QUEUE_PAYLOAD_VERSION,
    queue_payload: JSON.stringify(input.queueMessage),
    required_worker_protocol: input.requiredWorkerProtocol,
    state: BatchDeliveryState.Ready,
    created_at: createdAt,
    updated_at: createdAt,
    published_at: null,
    last_error: null,
  };
  await elIndex(INDEX_INTERNAL_OBJECTS, delivery, { context });
  return delivery;
};

export const isBatchDeliveryStateAtLeast = (delivery: BatchDelivery, state: BatchDeliveryState): boolean => {
  return getBatchDeliveryStateOrder(delivery.state) >= getBatchDeliveryStateOrder(state);
};

export const advanceBatchDeliveryState = async (
  context: AuthContext,
  delivery: BatchDelivery,
  state: BatchDeliveryState,
  patch: BatchDeliveryStatePatch = {},
): Promise<BatchDelivery> => {
  if (isBatchDeliveryStateAtLeast(delivery, state)) {
    return delivery;
  }
  const updatedDelivery = {
    ...delivery,
    ...patch,
    state,
    updated_at: now(),
    last_error: null,
  };
  await elUpdate(context, delivery._index ?? INDEX_INTERNAL_OBJECTS, delivery.internal_id, {
    doc: {
      ...patch,
      state,
      updated_at: updatedDelivery.updated_at,
      last_error: null,
    },
  });
  return updatedDelivery;
};

export const recordBatchDeliveryError = async (
  context: AuthContext,
  delivery: BatchDelivery,
  error: unknown,
): Promise<BatchDelivery> => {
  const updatedDelivery = {
    ...delivery,
    updated_at: now(),
    last_error: error instanceof Error ? error.message : String(error),
  };
  await elUpdate(context, delivery._index ?? INDEX_INTERNAL_OBJECTS, delivery.internal_id, {
    doc: {
      updated_at: updatedDelivery.updated_at,
      last_error: updatedDelivery.last_error,
    },
  });
  return updatedDelivery;
};

export const readBatchDeliveryQueueMessage = (delivery: BatchDelivery): BatchQueueMessage => {
  try {
    const queueMessage = JSON.parse(delivery.queue_payload) as BatchQueueMessage;
    if (queueMessage.submission_id !== delivery.submission_id) {
      throw FunctionalError('Invalid batch delivery queue payload', {
        delivery_id: delivery.internal_id,
        submission_id: delivery.submission_id,
        queue_submission_id: queueMessage.submission_id,
      });
    }
    if (delivery.required_worker_protocol === BatchDeliveryProtocol.V2) {
      if (
        queueMessage.delivery_protocol_version !== BatchDeliveryProtocol.V2
        || queueMessage.delivery_id !== delivery.internal_id
        || queueMessage.parent_delivery_id !== delivery.parent_delivery_id
        || queueMessage.delivery_kind !== delivery.delivery_kind
        || queueMessage.delivery_branch_kind !== delivery.branch_kind
        || queueMessage.delivery_branch_sequence !== delivery.branch_sequence
        || queueMessage.delivery_branch_ordinal !== delivery.branch_ordinal
      ) {
        throw FunctionalError('Invalid batch delivery queue payload', {
          delivery_id: delivery.internal_id,
        });
      }
    } else if (
      queueMessage.delivery_id !== undefined
      || queueMessage.delivery_protocol_version !== undefined
      || queueMessage.parent_delivery_id !== undefined
      || queueMessage.delivery_kind !== undefined
      || queueMessage.delivery_branch_kind !== undefined
      || queueMessage.delivery_branch_sequence !== undefined
      || queueMessage.delivery_branch_ordinal !== undefined
    ) {
      throw FunctionalError('Invalid batch delivery queue payload', {
        delivery_id: delivery.internal_id,
      });
    }
    return queueMessage;
  } catch (cause) {
    throw FunctionalError('Invalid batch delivery queue payload', {
      cause,
      delivery_id: delivery.internal_id,
    });
  }
};
