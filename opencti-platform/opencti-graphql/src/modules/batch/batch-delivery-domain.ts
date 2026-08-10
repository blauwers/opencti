import jsonCanonicalize from 'canonicalize';
import { v4 as uuidv4 } from 'uuid';
import { FunctionalError } from '../../config/errors';
import { elFindByIds, elIndex, elLoadById, elUpdate } from '../../database/engine';
import { INDEX_INTERNAL_OBJECTS, READ_INDEX_INTERNAL_OBJECTS } from '../../database/utils';
import { updateBatchExpectation } from '../../domain/work';
import { lockResources } from '../../lock/master-lock';
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
  type BatchDeliveryChildReservationInput,
  type BatchDeliveryEnvelope,
  type BatchDeliveryHandoff,
  BatchDeliveryHandoffEvidence,
  BatchDeliveryKind,
  BatchDeliveryProtocol,
  BatchDeliveryState,
  type BatchQueueMessage,
  ENTITY_TYPE_BATCH_DELIVERY,
} from './batch-types';

const BATCH_DELIVERY_PREFIX = 'batch-delivery--';
const BATCH_DELIVERY_CANDIDATE_PREFIX = 'batch-delivery-candidate--';
const BATCH_DELIVERY_QUEUE_PAYLOAD_VERSION = 1;
const BATCH_DELIVERY_HANDOFF_LOCK_PREFIX = 'batch-delivery-handoff:';
const BATCH_DELIVERY_STATE_ORDER = {
  [BatchDeliveryState.Ready]: 0,
  [BatchDeliveryState.Published]: 1,
};
const BATCH_DELIVERY_HANDOFF_EVIDENCE_ORDER = {
  [BatchDeliveryHandoffEvidence.None]: 0,
  [BatchDeliveryHandoffEvidence.Planned]: 1,
  [BatchDeliveryHandoffEvidence.ChildrenReserved]: 2,
  [BatchDeliveryHandoffEvidence.ChildrenPublished]: 3,
};
const BATCH_DELIVERY_EXPECTATION_REPLACEMENT_BRANCH_KINDS = new Set([
  BatchDeliveryBranchKind.LegacySplit,
  BatchDeliveryBranchKind.OversizedChunk,
]);

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

const isBatchDeliveryCandidateId = (candidateId: unknown): candidateId is string => {
  return typeof candidateId === 'string' && candidateId.startsWith(BATCH_DELIVERY_CANDIDATE_PREFIX) && candidateId.length > BATCH_DELIVERY_CANDIDATE_PREFIX.length;
};

type BatchDeliveryStatePatch = Partial<Pick<BatchDelivery, 'published_at' | 'last_error'>>;
type BatchDeliveryHandoffPatch = Partial<Pick<
  BatchDelivery,
  'child_set_fingerprint' | 'child_count' | 'child_delivery_ids' | 'children_reserved_at' | 'children_published_at' | 'last_error'
>>;

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

const getBatchDeliveryHandoffEvidenceOrder = (evidence: BatchDeliveryHandoffEvidence): number => {
  const order = BATCH_DELIVERY_HANDOFF_EVIDENCE_ORDER[evidence];
  if (order === undefined) {
    throw FunctionalError('Invalid batch delivery handoff evidence', { evidence });
  }
  return order;
};

export const getBatchDeliveryHandoffEvidence = (delivery: BatchDelivery): BatchDeliveryHandoffEvidence => {
  return delivery.handoff_evidence ?? BatchDeliveryHandoffEvidence.None;
};

const withBatchDeliveryHandoffDefaults = (delivery: BatchDelivery): BatchDelivery => {
  if (
    delivery.handoff_evidence !== undefined
    && delivery.child_set_fingerprint !== undefined
    && delivery.child_count !== undefined
    && delivery.child_delivery_ids !== undefined
    && delivery.children_reserved_at !== undefined
    && delivery.children_published_at !== undefined
  ) {
    return delivery;
  }
  return {
    ...delivery,
    handoff_evidence: getBatchDeliveryHandoffEvidence(delivery),
    child_set_fingerprint: delivery.child_set_fingerprint ?? null,
    child_count: delivery.child_count ?? 0,
    child_delivery_ids: Array.isArray(delivery.child_delivery_ids) ? delivery.child_delivery_ids : [],
    children_reserved_at: delivery.children_reserved_at ?? null,
    children_published_at: delivery.children_published_at ?? null,
  };
};

const parseBatchDeliveryQueuePayload = (queuePayload: string, deliveryId?: string): BatchQueueMessage => {
  try {
    const parsedPayload = JSON.parse(queuePayload);
    if (!parsedPayload || typeof parsedPayload !== 'object' || Array.isArray(parsedPayload)) {
      throw new Error('Queue payload must be an object');
    }
    return parsedPayload as BatchQueueMessage;
  } catch (cause) {
    throw FunctionalError('Invalid batch delivery queue payload', {
      cause,
      ...(deliveryId ? { delivery_id: deliveryId } : {}),
    });
  }
};

const buildBatchDeliveryContentFingerprint = (queueMessage: BatchQueueMessage): string => {
  try {
    if (typeof queueMessage.content !== 'string' || queueMessage.content.length === 0) {
      throw new Error('Queue payload content must be a non-empty string');
    }
    const content = JSON.parse(Buffer.from(queueMessage.content, 'base64').toString('utf-8'));
    return buildBatchDeliveryPayloadFingerprint(content);
  } catch (cause) {
    throw FunctionalError('Invalid batch delivery queue payload content', { cause });
  }
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

const assertBatchDeliveryQueueMessageMatchesReservation = (input: ReserveBatchDeliveryInput) => {
  const queueMessage = input.queueMessage;
  if (queueMessage.submission_id !== input.submissionId) {
    throw batchDeliveryConflict('Batch delivery queue payload does not match its submission', {
      delivery_id: input.deliveryId,
      submission_id: input.submissionId,
      queue_submission_id: queueMessage.submission_id,
    });
  }
  if (input.requiredWorkerProtocol === BatchDeliveryProtocol.V2) {
    if (
      queueMessage.delivery_protocol_version !== BatchDeliveryProtocol.V2
      || queueMessage.delivery_id !== input.deliveryId
      || queueMessage.parent_delivery_id !== input.parentDeliveryId
      || queueMessage.delivery_kind !== input.deliveryKind
      || queueMessage.delivery_branch_kind !== input.branchKind
      || queueMessage.delivery_branch_sequence !== input.branchSequence
      || queueMessage.delivery_branch_ordinal !== input.branchOrdinal
    ) {
      throw batchDeliveryConflict('Batch delivery queue payload does not match its lineage', {
        delivery_id: input.deliveryId,
      });
    }
    return;
  }
  if (
    queueMessage.delivery_id !== undefined
    || queueMessage.delivery_protocol_version !== undefined
    || queueMessage.parent_delivery_id !== undefined
    || queueMessage.delivery_kind !== undefined
    || queueMessage.delivery_branch_kind !== undefined
    || queueMessage.delivery_branch_sequence !== undefined
    || queueMessage.delivery_branch_ordinal !== undefined
  ) {
    throw batchDeliveryConflict('Batch delivery queue payload does not match its protocol', {
      delivery_id: input.deliveryId,
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

export const buildBatchDeliveryCandidateId = (): string => {
  return `${BATCH_DELIVERY_CANDIDATE_PREFIX}${uuidv4()}`;
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

export const promoteBatchDeliveryCandidateRoot = async (
  context: AuthContext,
  candidateId: string,
  queueMessage: BatchQueueMessage,
): Promise<BatchDelivery> => {
  if (!isBatchDeliveryCandidateId(candidateId) || queueMessage.batch_delivery_candidate_id !== candidateId) {
    throw batchDeliveryConflict('Invalid batch delivery promotion candidate', {
      candidate_id: candidateId,
      queue_candidate_id: queueMessage.batch_delivery_candidate_id,
    });
  }
  if (
    queueMessage.submission_id !== undefined
    || queueMessage.delivery_id !== undefined
    || queueMessage.delivery_protocol_version !== undefined
    || queueMessage.parent_delivery_id !== undefined
    || queueMessage.delivery_kind !== undefined
    || queueMessage.delivery_branch_kind !== undefined
    || queueMessage.delivery_branch_sequence !== undefined
    || queueMessage.delivery_branch_ordinal !== undefined
  ) {
    throw batchDeliveryConflict('Batch delivery promotion payload already has delivery identity', {
      candidate_id: candidateId,
    });
  }
  const rootEnvelope = buildRootBatchDeliveryEnvelope(candidateId);
  const promotedQueueMessage = {
    ...queueMessage,
    submission_id: candidateId,
    ...rootEnvelope,
  };
  return reserveBatchDelivery(context, {
    deliveryId: rootEnvelope.delivery_id,
    submissionId: candidateId,
    parentDeliveryId: null,
    deliveryKind: BatchDeliveryKind.Root,
    branchKind: BatchDeliveryBranchKind.Root,
    branchSequence: 0,
    branchOrdinal: 0,
    payloadFingerprint: buildBatchDeliveryContentFingerprint(promotedQueueMessage),
    queueMessage: promotedQueueMessage,
    requiredWorkerProtocol: BatchDeliveryProtocol.V2,
  });
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
  return delivery ? withBatchDeliveryHandoffDefaults(delivery as unknown as BatchDelivery) : null;
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
    queue_payload: buildBatchDeliveryPayloadFingerprint(parseBatchDeliveryQueuePayload(delivery.queue_payload, delivery.internal_id))
      !== buildBatchDeliveryPayloadFingerprint(input.queueMessage),
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
  assertBatchDeliveryQueueMessageMatchesReservation(input);
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
    handoff_evidence: BatchDeliveryHandoffEvidence.None,
    child_set_fingerprint: null,
    child_count: 0,
    child_delivery_ids: [],
    created_at: createdAt,
    updated_at: createdAt,
    published_at: null,
    children_reserved_at: null,
    children_published_at: null,
    last_error: null,
  };
  await elIndex(INDEX_INTERNAL_OBJECTS, delivery, { context });
  return delivery;
};

export const isBatchDeliveryStateAtLeast = (delivery: BatchDelivery, state: BatchDeliveryState): boolean => {
  return getBatchDeliveryStateOrder(delivery.state) >= getBatchDeliveryStateOrder(state);
};

export const isBatchDeliveryHandoffEvidenceAtLeast = (
  delivery: BatchDelivery,
  evidence: BatchDeliveryHandoffEvidence,
): boolean => {
  return getBatchDeliveryHandoffEvidenceOrder(getBatchDeliveryHandoffEvidence(delivery)) >= getBatchDeliveryHandoffEvidenceOrder(evidence);
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

export const advanceBatchDeliveryHandoffEvidence = async (
  context: AuthContext,
  delivery: BatchDelivery,
  evidence: BatchDeliveryHandoffEvidence,
  patch: BatchDeliveryHandoffPatch = {},
): Promise<BatchDelivery> => {
  if (isBatchDeliveryHandoffEvidenceAtLeast(delivery, evidence)) {
    return delivery;
  }
  const updatedDelivery = withBatchDeliveryHandoffDefaults({
    ...delivery,
    ...patch,
    handoff_evidence: evidence,
    updated_at: now(),
    last_error: null,
  });
  await elUpdate(context, delivery._index ?? INDEX_INTERNAL_OBJECTS, delivery.internal_id, {
    doc: {
      ...patch,
      handoff_evidence: evidence,
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
    const queueMessage = parseBatchDeliveryQueuePayload(delivery.queue_payload, delivery.internal_id);
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

const buildBatchDeliveryChildSetFingerprint = (inputs: ReserveBatchDeliveryInput[]): string => {
  return buildBatchDeliveryPayloadFingerprint(inputs
    .map((input) => ({
      delivery_id: input.deliveryId,
      branch_kind: input.branchKind,
      branch_sequence: input.branchSequence,
      branch_ordinal: input.branchOrdinal,
      payload_fingerprint: input.payloadFingerprint,
      queue_payload_fingerprint: buildBatchDeliveryPayloadFingerprint(input.queueMessage),
    }))
    .sort((left, right) => left.delivery_id.localeCompare(right.delivery_id)));
};

const sortBatchDeliveryChildren = (children: ReserveBatchDeliveryInput[]): ReserveBatchDeliveryInput[] => {
  return [...children].sort((left, right) => {
    if (left.branchKind !== right.branchKind) {
      return left.branchKind.localeCompare(right.branchKind);
    }
    if (left.branchSequence !== right.branchSequence) {
      return left.branchSequence - right.branchSequence;
    }
    if (left.branchOrdinal !== right.branchOrdinal) {
      return left.branchOrdinal - right.branchOrdinal;
    }
    return left.deliveryId.localeCompare(right.deliveryId);
  });
};

const assertBatchDeliveryHandoffMetadata = (delivery: BatchDelivery) => {
  const normalizedDelivery = withBatchDeliveryHandoffDefaults(delivery);
  const evidence = getBatchDeliveryHandoffEvidence(normalizedDelivery);
  const childDeliveryIds = normalizedDelivery.child_delivery_ids;
  const uniqueChildDeliveryIds = new Set(childDeliveryIds);
  if (evidence === BatchDeliveryHandoffEvidence.None) {
    if (
      normalizedDelivery.child_set_fingerprint !== null
      || normalizedDelivery.child_count !== 0
      || childDeliveryIds.length !== 0
      || normalizedDelivery.children_reserved_at !== null
      || normalizedDelivery.children_published_at !== null
    ) {
      throw batchDeliveryConflict('Batch delivery has invalid empty handoff metadata', {
        delivery_id: normalizedDelivery.internal_id,
      });
    }
    return;
  }
  if (
    typeof normalizedDelivery.child_set_fingerprint !== 'string'
    || normalizedDelivery.child_set_fingerprint.length === 0
    || !Number.isInteger(normalizedDelivery.child_count)
    || normalizedDelivery.child_count <= 0
    || childDeliveryIds.length !== normalizedDelivery.child_count
    || uniqueChildDeliveryIds.size !== childDeliveryIds.length
  ) {
    throw batchDeliveryConflict('Batch delivery has invalid child handoff metadata', {
      delivery_id: normalizedDelivery.internal_id,
    });
  }
  if (
    isBatchDeliveryHandoffEvidenceAtLeast(normalizedDelivery, BatchDeliveryHandoffEvidence.ChildrenReserved)
    && normalizedDelivery.children_reserved_at === null
  ) {
    throw batchDeliveryConflict('Batch delivery is missing child reservation evidence', {
      delivery_id: normalizedDelivery.internal_id,
    });
  }
  if (
    !isBatchDeliveryHandoffEvidenceAtLeast(normalizedDelivery, BatchDeliveryHandoffEvidence.ChildrenReserved)
    && normalizedDelivery.children_reserved_at !== null
  ) {
    throw batchDeliveryConflict('Batch delivery has premature child reservation evidence', {
      delivery_id: normalizedDelivery.internal_id,
    });
  }
  if (
    isBatchDeliveryHandoffEvidenceAtLeast(normalizedDelivery, BatchDeliveryHandoffEvidence.ChildrenPublished)
    && normalizedDelivery.children_published_at === null
  ) {
    throw batchDeliveryConflict('Batch delivery is missing child publication evidence', {
      delivery_id: normalizedDelivery.internal_id,
    });
  }
  if (
    !isBatchDeliveryHandoffEvidenceAtLeast(normalizedDelivery, BatchDeliveryHandoffEvidence.ChildrenPublished)
    && normalizedDelivery.children_published_at !== null
  ) {
    throw batchDeliveryConflict('Batch delivery has premature child publication evidence', {
      delivery_id: normalizedDelivery.internal_id,
    });
  }
};

const assertBatchDeliveryParentCanReserveChildren = (parentDelivery: BatchDelivery) => {
  if (parentDelivery.required_worker_protocol !== BatchDeliveryProtocol.V2) {
    throw batchDeliveryConflict('Batch delivery protocol does not support durable child reservation', {
      delivery_id: parentDelivery.internal_id,
      required_worker_protocol: parentDelivery.required_worker_protocol,
    });
  }
};

const assertBatchDeliveryChildSetMatchesParent = (
  parentDelivery: BatchDelivery,
  childSetFingerprint: string,
  childDeliveryIds: string[],
) => {
  if (
    parentDelivery.child_set_fingerprint !== childSetFingerprint
    || parentDelivery.child_count !== childDeliveryIds.length
    || JSON.stringify(parentDelivery.child_delivery_ids) !== JSON.stringify(childDeliveryIds)
  ) {
    throw batchDeliveryConflict('Batch delivery parent is already associated with a different child set', {
      delivery_id: parentDelivery.internal_id,
      child_set_fingerprint: childSetFingerprint,
      existing_child_set_fingerprint: parentDelivery.child_set_fingerprint,
    });
  }
};

const loadBatchDeliveryChildren = async (context: AuthContext, parentDelivery: BatchDelivery): Promise<BatchDelivery[]> => {
  const childDeliveryIds = parentDelivery.child_delivery_ids;
  if (childDeliveryIds.length === 0) {
    return [];
  }
  const loadedChildren = await elFindByIds(context, SYSTEM_USER, childDeliveryIds, {
    type: ENTITY_TYPE_BATCH_DELIVERY,
    indices: READ_INDEX_INTERNAL_OBJECTS,
  }) as unknown as BatchDelivery[];
  const childById = new Map(loadedChildren.map((child) => [child.internal_id, withBatchDeliveryHandoffDefaults(child)]));
  const missingChildIds = childDeliveryIds.filter((childDeliveryId) => !childById.has(childDeliveryId));
  if (missingChildIds.length > 0) {
    throw batchDeliveryConflict('Batch delivery child set is missing durable child records', {
      delivery_id: parentDelivery.internal_id,
      missing_child_delivery_ids: missingChildIds,
    });
  }
  return childDeliveryIds.map((childDeliveryId) => childById.get(childDeliveryId) as BatchDelivery);
};

const buildBatchDeliveryHandoff = async (context: AuthContext, parentDelivery: BatchDelivery): Promise<BatchDeliveryHandoff> => {
  const normalizedParentDelivery = withBatchDeliveryHandoffDefaults(parentDelivery);
  assertBatchDeliveryHandoffMetadata(normalizedParentDelivery);
  if (!isBatchDeliveryHandoffEvidenceAtLeast(normalizedParentDelivery, BatchDeliveryHandoffEvidence.ChildrenReserved)) {
    return {
      parentDelivery: normalizedParentDelivery,
      children: [],
      pendingChildren: [],
    };
  }
  const children = await loadBatchDeliveryChildren(context, normalizedParentDelivery);
  const pendingChildren = children.filter((child) => !isBatchDeliveryStateAtLeast(child, BatchDeliveryState.Published));
  if (
    getBatchDeliveryHandoffEvidence(normalizedParentDelivery) === BatchDeliveryHandoffEvidence.ChildrenPublished
    && pendingChildren.length > 0
  ) {
    throw batchDeliveryConflict('Batch delivery child publication evidence is inconsistent with child state', {
      delivery_id: normalizedParentDelivery.internal_id,
      pending_child_delivery_ids: pendingChildren.map((child) => child.internal_id),
    });
  }
  return {
    parentDelivery: normalizedParentDelivery,
    children,
    pendingChildren,
  };
};

export const loadBatchDeliveryHandoff = async (context: AuthContext, parentDeliveryId: string): Promise<BatchDeliveryHandoff> => {
  const parentDelivery = await loadBatchDelivery(context, parentDeliveryId);
  if (!parentDelivery) {
    throw batchDeliveryConflict('Batch delivery parent cannot be found', {
      parent_delivery_id: parentDeliveryId,
    });
  }
  return buildBatchDeliveryHandoff(context, parentDelivery);
};

const buildReserveBatchDeliveryChildInputs = (
  parentDelivery: BatchDelivery,
  children: BatchDeliveryChildReservationInput[],
): ReserveBatchDeliveryInput[] => {
  if (children.length === 0) {
    throw batchDeliveryConflict('Batch delivery child set must not be empty', {
      delivery_id: parentDelivery.internal_id,
    });
  }
  const childInputs = sortBatchDeliveryChildren(children.map((child) => {
    const deliveryId = buildChildBatchDeliveryId(
      parentDelivery.internal_id,
      child.branchKind,
      child.branchSequence,
      child.branchOrdinal,
    );
    return {
      deliveryId,
      submissionId: parentDelivery.submission_id,
      parentDeliveryId: parentDelivery.internal_id,
      deliveryKind: BatchDeliveryKind.Child,
      branchKind: child.branchKind,
      branchSequence: child.branchSequence,
      branchOrdinal: child.branchOrdinal,
      payloadFingerprint: buildBatchDeliveryContentFingerprint(child.queueMessage),
      queueMessage: child.queueMessage,
      requiredWorkerProtocol: BatchDeliveryProtocol.V2,
    };
  }));
  const childDeliveryIds = childInputs.map((child) => child.deliveryId);
  if (new Set(childDeliveryIds).size !== childDeliveryIds.length) {
    throw batchDeliveryConflict('Batch delivery child set contains duplicate lineage slots', {
      delivery_id: parentDelivery.internal_id,
      child_delivery_ids: childDeliveryIds,
    });
  }
  return childInputs;
};

const buildBatchDeliveryExpectationWorkIds = (parentDelivery: BatchDelivery): string[] => {
  const queueMessage = readBatchDeliveryQueueMessage(parentDelivery);
  const additionalWorkIds = queueMessage.additional_work_ids === undefined || queueMessage.additional_work_ids === null
    ? []
    : queueMessage.additional_work_ids;
  if (
    typeof queueMessage.work_id !== 'string'
    || queueMessage.work_id.length === 0
    || !Array.isArray(additionalWorkIds)
    || additionalWorkIds.some((workId) => typeof workId !== 'string' || workId.length === 0)
  ) {
    throw batchDeliveryConflict('Batch delivery queue payload has invalid work attribution', {
      delivery_id: parentDelivery.internal_id,
    });
  }
  return [...new Set([queueMessage.work_id, ...additionalWorkIds])];
};

const getBatchDeliveryExpectationReplacementDelta = (childInputs: ReserveBatchDeliveryInput[]): number => {
  const hasReplacementBranch = childInputs.some((childInput) => BATCH_DELIVERY_EXPECTATION_REPLACEMENT_BRANCH_KINDS.has(childInput.branchKind));
  if (!hasReplacementBranch) {
    return 0;
  }
  if (!childInputs.every((childInput) => BATCH_DELIVERY_EXPECTATION_REPLACEMENT_BRANCH_KINDS.has(childInput.branchKind))) {
    throw batchDeliveryConflict('Batch delivery child set mixes expectation replacement and preservation branches', {
      child_branch_kinds: [...new Set(childInputs.map((childInput) => childInput.branchKind))],
    });
  }
  return childInputs.length - 1;
};

const applyBatchDeliveryExpectationReplacement = async (
  context: AuthContext,
  parentDelivery: BatchDelivery,
  expectationWorkIds: string[],
  expectationDelta: number,
) => {
  if (expectationDelta === 0) {
    return;
  }
  // Split handoff replaces one already-recorded parent expectation with N
  // child expectations, so only the net delta is applied here.
  for (const workId of expectationWorkIds) {
    await updateBatchExpectation(context, SYSTEM_USER, workId, expectationDelta, parentDelivery.internal_id);
  }
};

export const reserveBatchDeliveryChildren = async (
  context: AuthContext,
  parentDeliveryId: string,
  children: BatchDeliveryChildReservationInput[],
): Promise<BatchDeliveryHandoff> => {
  const lock = await lockResources([`${BATCH_DELIVERY_HANDOFF_LOCK_PREFIX}${parentDeliveryId}`]);
  try {
    let parentDelivery = await loadBatchDelivery(context, parentDeliveryId);
    if (!parentDelivery) {
      throw batchDeliveryConflict('Batch delivery parent cannot be found', {
        parent_delivery_id: parentDeliveryId,
      });
    }
    assertBatchDeliveryParentCanReserveChildren(parentDelivery);
    const childInputs = buildReserveBatchDeliveryChildInputs(parentDelivery, children);
    const childDeliveryIds = childInputs.map((child) => child.deliveryId);
    const childSetFingerprint = buildBatchDeliveryChildSetFingerprint(childInputs);
    const expectationDelta = getBatchDeliveryExpectationReplacementDelta(childInputs);
    const expectationWorkIds = expectationDelta === 0 ? [] : buildBatchDeliveryExpectationWorkIds(parentDelivery);
    if (getBatchDeliveryHandoffEvidence(parentDelivery) === BatchDeliveryHandoffEvidence.None) {
      parentDelivery = await advanceBatchDeliveryHandoffEvidence(context, parentDelivery, BatchDeliveryHandoffEvidence.Planned, {
        child_set_fingerprint: childSetFingerprint,
        child_count: childDeliveryIds.length,
        child_delivery_ids: childDeliveryIds,
      });
    } else {
      assertBatchDeliveryChildSetMatchesParent(parentDelivery, childSetFingerprint, childDeliveryIds);
    }
    if (isBatchDeliveryHandoffEvidenceAtLeast(parentDelivery, BatchDeliveryHandoffEvidence.ChildrenReserved)) {
      const existingHandoff = await buildBatchDeliveryHandoff(context, parentDelivery);
      childInputs.forEach((childInput, index) => assertBatchDeliveryReservation(existingHandoff.children[index], childInput));
      return existingHandoff;
    }
    for (const childInput of childInputs) {
      await reserveBatchDelivery(context, childInput);
    }
    await applyBatchDeliveryExpectationReplacement(context, parentDelivery, expectationWorkIds, expectationDelta);
    parentDelivery = await advanceBatchDeliveryHandoffEvidence(context, parentDelivery, BatchDeliveryHandoffEvidence.ChildrenReserved, {
      children_reserved_at: now(),
    });
    return buildBatchDeliveryHandoff(context, parentDelivery);
  } finally {
    await lock.unlock();
  }
};

export const markBatchDeliveryChildrenPublished = async (
  context: AuthContext,
  parentDeliveryId: string,
  childDeliveryIds: string[],
): Promise<BatchDeliveryHandoff> => {
  const lock = await lockResources([`${BATCH_DELIVERY_HANDOFF_LOCK_PREFIX}${parentDeliveryId}`]);
  try {
    let handoff = await loadBatchDeliveryHandoff(context, parentDeliveryId);
    if (!isBatchDeliveryHandoffEvidenceAtLeast(handoff.parentDelivery, BatchDeliveryHandoffEvidence.ChildrenReserved)) {
      throw batchDeliveryConflict('Batch delivery children cannot be published before reservation', {
        delivery_id: parentDeliveryId,
      });
    }
    const uniqueChildDeliveryIds = [...new Set(childDeliveryIds)];
    const unknownChildDeliveryIds = uniqueChildDeliveryIds.filter((childDeliveryId) => !handoff.parentDelivery.child_delivery_ids.includes(childDeliveryId));
    if (unknownChildDeliveryIds.length > 0) {
      throw batchDeliveryConflict('Batch delivery publication includes children outside the reserved set', {
        delivery_id: parentDeliveryId,
        child_delivery_ids: unknownChildDeliveryIds,
      });
    }
    const childById = new Map(handoff.children.map((child) => [child.internal_id, child]));
    for (const childDeliveryId of uniqueChildDeliveryIds) {
      const child = childById.get(childDeliveryId) as BatchDelivery;
      if (!isBatchDeliveryStateAtLeast(child, BatchDeliveryState.Published)) {
        childById.set(childDeliveryId, await advanceBatchDeliveryState(context, child, BatchDeliveryState.Published, {
          published_at: now(),
        }));
      }
    }
    const children = handoff.parentDelivery.child_delivery_ids.map((childDeliveryId) => childById.get(childDeliveryId) as BatchDelivery);
    const pendingChildren = children.filter((child) => !isBatchDeliveryStateAtLeast(child, BatchDeliveryState.Published));
    let parentDelivery = handoff.parentDelivery;
    if (pendingChildren.length === 0) {
      parentDelivery = await advanceBatchDeliveryHandoffEvidence(context, parentDelivery, BatchDeliveryHandoffEvidence.ChildrenPublished, {
        children_published_at: now(),
      });
    }
    handoff = {
      parentDelivery,
      children,
      pendingChildren,
    };
    return handoff;
  } finally {
    await lock.unlock();
  }
};
