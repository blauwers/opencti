import { FunctionalError } from '../../config/errors';
import { elIndex, elLoadById, elUpdate } from '../../database/engine';
import { INDEX_INTERNAL_OBJECTS, READ_INDEX_INTERNAL_OBJECTS } from '../../database/utils';
import { lockResources } from '../../lock/master-lock';
import { BASE_TYPE_ENTITY } from '../../schema/general';
import { getParentTypes } from '../../schema/schemaUtils';
import type { AuthContext } from '../../types/user';
import { SYSTEM_USER } from '../../utils/access';
import { now } from '../../utils/format';
import { hashSHA256 } from '../../utils/hash';
import {
  assertBatchBackendAttemptObservationIdentity,
  buildBatchBackendAttemptObservationFingerprint,
  readFreshBatchBackendAttemptObservation,
} from './batch-backend-attempt-observation-domain';
import { buildBatchExecutionReceiptLockId, loadBatchExecutionReceipt, readBatchExecutionReceiptResultMetadata } from './batch-execution-receipt-domain';
import {
  BatchAdmissionErrorCode,
  type BatchBackendAttemptObservation,
  type BatchExecutionReceipt,
  BatchExecutionReceiptFailureProof,
  BatchExecutionReceiptState,
  type BatchExecutionReconciliation,
  BatchExecutionReconciliationEvidenceClass,
  BatchExecutionReconciliationEvidenceRefType,
  BatchExecutionReconciliationOpenedReason,
  BatchExecutionReconciliationState,
  type BatchExecutionReconciliationTerminalEvidence,
  type BatchExecutionReceiptResultMetadata,
  ENTITY_TYPE_BATCH_EXECUTION_RECONCILIATION,
} from './batch-types';

const BATCH_EXECUTION_RECONCILIATION_PREFIX = 'batch-execution-reconciliation--';
const BATCH_EXECUTION_RECONCILIATION_LOCK_PREFIX = 'batch-execution-reconciliation:';
const BATCH_EXECUTION_RECONCILIATION_ALLOWED_TRANSITIONS = {
  [BatchExecutionReconciliationState.Open]: new Set([
    BatchExecutionReconciliationState.RunningObserved,
    BatchExecutionReconciliationState.MaterializationPending,
    BatchExecutionReconciliationState.Ambiguous,
    BatchExecutionReconciliationState.ResolvedCompleted,
    BatchExecutionReconciliationState.ResolvedFailedTerminal,
  ]),
  [BatchExecutionReconciliationState.RunningObserved]: new Set([
    BatchExecutionReconciliationState.MaterializationPending,
    BatchExecutionReconciliationState.Ambiguous,
    BatchExecutionReconciliationState.ResolvedCompleted,
    BatchExecutionReconciliationState.ResolvedFailedTerminal,
  ]),
  [BatchExecutionReconciliationState.MaterializationPending]: new Set([
    BatchExecutionReconciliationState.Ambiguous,
    BatchExecutionReconciliationState.ResolvedCompleted,
    BatchExecutionReconciliationState.ResolvedFailedTerminal,
  ]),
  [BatchExecutionReconciliationState.Ambiguous]: new Set([
    BatchExecutionReconciliationState.ResolvedCompleted,
    BatchExecutionReconciliationState.ResolvedFailedTerminal,
  ]),
  [BatchExecutionReconciliationState.ResolvedCompleted]: new Set<BatchExecutionReconciliationState>(),
  [BatchExecutionReconciliationState.ResolvedFailedTerminal]: new Set<BatchExecutionReconciliationState>(),
};
const BATCH_EXECUTION_RECONCILIATION_REJECTED_TERMINAL_EVIDENCE = new Set<string>([
  BatchExecutionReconciliationEvidenceClass.ActiveAttempt,
  BatchExecutionReconciliationEvidenceClass.MaterializationHandoff,
  'WORK_PROGRESS',
  'FINAL_HTTP_RESPONSE_STATE',
  'RABBITMQ_ACK_STATE',
  'ELAPSED_TIME_ONLY',
  'NON_MATERIALIZED_COMMIT',
]);

export interface ReserveBatchExecutionReconciliationInput {
  receipt: BatchExecutionReceipt;
  openedReason: BatchExecutionReconciliationOpenedReason;
  lastError?: string | null;
}

type BatchExecutionReconciliationPatch = Partial<Pick<
  BatchExecutionReconciliation,
  | 'state'
  | 'evidence_class'
  | 'evidence_ref_type'
  | 'evidence_ref_id'
  | 'evidence_fingerprint'
  | 'attempt_observation_id'
  | 'attempt_observed_at'
  | 'attempt_expires_at'
  | 'materialization_handoff_id'
  | 'materialization_handoff_state'
  | 'resolved_receipt_state'
  | 'last_observed_at'
  | 'resolved_at'
  | 'last_error'
>>;

type BatchExecutionReconciliationTerminalPatch = Pick<
  BatchExecutionReconciliation,
  'evidence_class' | 'evidence_ref_type' | 'evidence_ref_id' | 'evidence_fingerprint' | 'resolved_receipt_state'
>;

const batchExecutionReconciliationConflict = (message: string, data: Record<string, unknown> = {}) => {
  return FunctionalError(message, {
    batch_error_code: BatchAdmissionErrorCode.ExecutionReconciliationConflict,
    ...data,
  });
};

const assertBatchExecutionReconciliationState = (state: BatchExecutionReconciliationState) => {
  if (!BATCH_EXECUTION_RECONCILIATION_ALLOWED_TRANSITIONS[state]) {
    throw FunctionalError('Invalid batch execution reconciliation state', { state });
  }
};

export const assertBatchExecutionReconciliationTransition = (
  reconciliation: BatchExecutionReconciliation,
  state: BatchExecutionReconciliationState,
) => {
  assertBatchExecutionReconciliationState(reconciliation.state);
  assertBatchExecutionReconciliationState(state);
  if (reconciliation.state === state) {
    return;
  }
  if (!BATCH_EXECUTION_RECONCILIATION_ALLOWED_TRANSITIONS[reconciliation.state].has(state)) {
    throw batchExecutionReconciliationConflict('Invalid batch execution reconciliation state transition', {
      reconciliation_id: reconciliation.internal_id,
      reconciliation_state: reconciliation.state,
      next_reconciliation_state: state,
    });
  }
};

const getBatchExecutionReconciliationInitialState = (
  openedReason: BatchExecutionReconciliationOpenedReason,
): BatchExecutionReconciliationState.Open => {
  switch (openedReason) {
    case BatchExecutionReconciliationOpenedReason.ExplicitStartedReceipt:
    case BatchExecutionReconciliationOpenedReason.ExplicitRequiresReconciliationReceipt:
    case BatchExecutionReconciliationOpenedReason.CommittedWithoutDurableMaterialization:
    case BatchExecutionReconciliationOpenedReason.PostStartError:
      return BatchExecutionReconciliationState.Open;
    default:
      throw batchExecutionReconciliationConflict('Invalid batch execution reconciliation open reason', {
        opened_reason: openedReason,
      });
  }
};

const assertBatchExecutionReconciliationOpenInput = (input: ReserveBatchExecutionReconciliationInput) => {
  getBatchExecutionReconciliationInitialState(input.openedReason);
  switch (input.openedReason) {
    case BatchExecutionReconciliationOpenedReason.ExplicitStartedReceipt:
      if (input.receipt.state !== BatchExecutionReceiptState.Started) {
        throw batchExecutionReconciliationConflict('Explicit batch execution reconciliation open requires a STARTED receipt', {
          receipt_id: input.receipt.internal_id,
          receipt_state: input.receipt.state,
        });
      }
      return;
    case BatchExecutionReconciliationOpenedReason.ExplicitRequiresReconciliationReceipt:
      if (input.receipt.state !== BatchExecutionReceiptState.RequiresReconciliation) {
        throw batchExecutionReconciliationConflict('Explicit batch execution reconciliation open requires a REQUIRES_RECONCILIATION receipt', {
          receipt_id: input.receipt.internal_id,
          receipt_state: input.receipt.state,
        });
      }
      return;
    case BatchExecutionReconciliationOpenedReason.CommittedWithoutDurableMaterialization:
    case BatchExecutionReconciliationOpenedReason.PostStartError:
      if (
        input.receipt.state !== BatchExecutionReceiptState.Started
        && input.receipt.state !== BatchExecutionReceiptState.RequiresReconciliation
      ) {
        throw batchExecutionReconciliationConflict('Active batch execution reconciliation open requires a STARTED or REQUIRES_RECONCILIATION receipt', {
          receipt_id: input.receipt.internal_id,
          receipt_state: input.receipt.state,
        });
      }
      return;
    default:
      throw batchExecutionReconciliationConflict('Invalid batch execution reconciliation open reason', {
        opened_reason: input.openedReason,
      });
  }
};

export const buildBatchExecutionReconciliationId = (deliveryId: string): string => {
  return `${BATCH_EXECUTION_RECONCILIATION_PREFIX}${hashSHA256(`${BATCH_EXECUTION_RECONCILIATION_LOCK_PREFIX}${deliveryId}`)}`;
};

export const buildBatchExecutionReconciliationLockId = (deliveryId: string): string => {
  return `${BATCH_EXECUTION_RECONCILIATION_LOCK_PREFIX}${deliveryId}`;
};

export const loadBatchExecutionReconciliation = async (
  context: AuthContext,
  deliveryId: string,
): Promise<BatchExecutionReconciliation | null> => {
  const reconciliationId = buildBatchExecutionReconciliationId(deliveryId);
  const reconciliation = await elLoadById(context, SYSTEM_USER, reconciliationId, {
    type: ENTITY_TYPE_BATCH_EXECUTION_RECONCILIATION,
    indices: READ_INDEX_INTERNAL_OBJECTS,
  });
  return reconciliation ? reconciliation as unknown as BatchExecutionReconciliation : null;
};

export const assertBatchExecutionReconciliationIdentity = (
  reconciliation: BatchExecutionReconciliation,
  receipt: BatchExecutionReceipt,
): void => {
  const conflictingFields = Object.entries({
    receipt_id: reconciliation.receipt_id !== receipt.internal_id,
    delivery_id: reconciliation.delivery_id !== receipt.delivery_id,
    submission_id: reconciliation.submission_id !== receipt.submission_id,
    request_fingerprint: reconciliation.request_fingerprint !== receipt.request_fingerprint,
    request_contract_version: reconciliation.request_contract_version !== receipt.request_contract_version,
  }).filter(([, conflict]) => conflict).map(([field]) => field);
  if (conflictingFields.length > 0) {
    throw batchExecutionReconciliationConflict('Batch execution reconciliation is already associated with different immutable receipt data', {
      reconciliation_id: reconciliation.internal_id,
      delivery_id: reconciliation.delivery_id,
      conflicting_fields: conflictingFields,
    });
  }
};

export const assertBatchExecutionReconciliationReservation = (
  reconciliation: BatchExecutionReconciliation,
  input: ReserveBatchExecutionReconciliationInput,
): void => {
  assertBatchExecutionReconciliationOpenInput(input);
  assertBatchExecutionReconciliationIdentity(reconciliation, input.receipt);
  const conflictingFields = Object.entries({
    opened_reason: reconciliation.opened_reason !== input.openedReason,
  }).filter(([, conflict]) => conflict).map(([field]) => field);
  if (conflictingFields.length > 0) {
    throw batchExecutionReconciliationConflict('Batch execution reconciliation is already associated with different immutable open metadata', {
      reconciliation_id: reconciliation.internal_id,
      delivery_id: reconciliation.delivery_id,
      conflicting_fields: conflictingFields,
    });
  }
};

export const reserveBatchExecutionReconciliation = async (
  context: AuthContext,
  input: ReserveBatchExecutionReconciliationInput,
): Promise<BatchExecutionReconciliation> => {
  assertBatchExecutionReconciliationOpenInput(input);
  const existingReconciliation = await loadBatchExecutionReconciliation(context, input.receipt.delivery_id);
  if (existingReconciliation) {
    assertBatchExecutionReconciliationReservation(existingReconciliation, input);
    return existingReconciliation;
  }
  const createdAt = now();
  const reconciliationId = buildBatchExecutionReconciliationId(input.receipt.delivery_id);
  const reconciliation: BatchExecutionReconciliation = {
    id: reconciliationId,
    internal_id: reconciliationId,
    standard_id: reconciliationId,
    entity_type: ENTITY_TYPE_BATCH_EXECUTION_RECONCILIATION,
    base_type: BASE_TYPE_ENTITY,
    parent_types: getParentTypes(ENTITY_TYPE_BATCH_EXECUTION_RECONCILIATION),
    receipt_id: input.receipt.internal_id,
    delivery_id: input.receipt.delivery_id,
    submission_id: input.receipt.submission_id,
    request_fingerprint: input.receipt.request_fingerprint,
    request_contract_version: input.receipt.request_contract_version,
    opened_from_receipt_state: input.receipt.state,
    opened_reason: input.openedReason,
    state: getBatchExecutionReconciliationInitialState(input.openedReason),
    evidence_class: null,
    evidence_ref_type: null,
    evidence_ref_id: null,
    evidence_fingerprint: null,
    attempt_observation_id: null,
    attempt_observed_at: null,
    attempt_expires_at: null,
    materialization_handoff_id: null,
    materialization_handoff_state: null,
    resolved_receipt_state: null,
    opened_at: createdAt,
    last_observed_at: createdAt,
    resolved_at: null,
    created_at: createdAt,
    updated_at: createdAt,
    last_error: input.lastError ?? null,
  };
  await elIndex(INDEX_INTERNAL_OBJECTS, reconciliation, { context });
  return reconciliation;
};

const advanceBatchExecutionReconciliationState = async (
  context: AuthContext,
  reconciliation: BatchExecutionReconciliation,
  state: BatchExecutionReconciliationState,
  patch: BatchExecutionReconciliationPatch = {},
): Promise<BatchExecutionReconciliation> => {
  assertBatchExecutionReconciliationTransition(reconciliation, state);
  if (reconciliation.state === state) {
    return reconciliation;
  }
  const updatedReconciliation = {
    ...reconciliation,
    ...patch,
    state,
    updated_at: now(),
  };
  await elUpdate(context, reconciliation._index ?? INDEX_INTERNAL_OBJECTS, reconciliation.internal_id, {
    doc: {
      ...patch,
      state,
      updated_at: updatedReconciliation.updated_at,
    },
  });
  return updatedReconciliation;
};

const updateBatchExecutionReconciliationObservation = async (
  context: AuthContext,
  reconciliation: BatchExecutionReconciliation,
  patch: BatchExecutionReconciliationPatch,
): Promise<BatchExecutionReconciliation> => {
  const updatedReconciliation = {
    ...reconciliation,
    ...patch,
    updated_at: now(),
  };
  await elUpdate(context, reconciliation._index ?? INDEX_INTERNAL_OBJECTS, reconciliation.internal_id, {
    doc: {
      ...patch,
      updated_at: updatedReconciliation.updated_at,
    },
  });
  return updatedReconciliation;
};

const recordBatchExecutionReconciliationAmbiguousUnlocked = async (
  context: AuthContext,
  reconciliation: BatchExecutionReconciliation,
  error: unknown,
): Promise<BatchExecutionReconciliation> => {
  const lastObservedAt = now();
  const lastError = error instanceof Error ? error.message : String(error);
  if (reconciliation.state === BatchExecutionReconciliationState.Ambiguous) {
    return updateBatchExecutionReconciliationObservation(context, reconciliation, {
      last_observed_at: lastObservedAt,
      last_error: lastError,
    });
  }
  return advanceBatchExecutionReconciliationState(context, reconciliation, BatchExecutionReconciliationState.Ambiguous, {
    ...(reconciliation.state === BatchExecutionReconciliationState.RunningObserved ? {
      evidence_class: null,
      evidence_ref_type: null,
      evidence_ref_id: null,
      evidence_fingerprint: null,
    } : {}),
    last_observed_at: lastObservedAt,
    last_error: lastError,
  });
};

const buildBatchExecutionReconciliationActiveAttemptPatch = (
  observation: BatchBackendAttemptObservation,
): BatchExecutionReconciliationPatch => ({
  evidence_class: BatchExecutionReconciliationEvidenceClass.ActiveAttempt,
  evidence_ref_type: BatchExecutionReconciliationEvidenceRefType.BackendAttemptObservation,
  evidence_ref_id: observation.observation_id,
  evidence_fingerprint: buildBatchBackendAttemptObservationFingerprint(observation),
  attempt_observation_id: observation.observation_id,
  attempt_observed_at: observation.observed_at,
  attempt_expires_at: observation.expires_at,
  last_observed_at: observation.observed_at,
  last_error: null,
});

const assertBatchExecutionReconciliationActiveAttemptEvidenceIdentity = (
  reconciliation: BatchExecutionReconciliation,
  patch: BatchExecutionReconciliationPatch,
) => {
  const conflictingFields = Object.entries({
    evidence_class: reconciliation.evidence_class !== patch.evidence_class,
    evidence_ref_type: reconciliation.evidence_ref_type !== patch.evidence_ref_type,
    evidence_ref_id: reconciliation.evidence_ref_id !== patch.evidence_ref_id,
    evidence_fingerprint: reconciliation.evidence_fingerprint !== patch.evidence_fingerprint,
    attempt_observation_id: reconciliation.attempt_observation_id !== patch.attempt_observation_id,
  }).filter(([, conflict]) => conflict).map(([field]) => field);
  if (conflictingFields.length > 0) {
    throw batchExecutionReconciliationConflict('Batch execution reconciliation active attempt evidence changed after persistence', {
      reconciliation_id: reconciliation.internal_id,
      conflicting_fields: conflictingFields,
    });
  }
};

const recordBatchExecutionReconciliationRunningObservedUnlocked = async (
  context: AuthContext,
  reconciliation: BatchExecutionReconciliation,
  receipt: BatchExecutionReceipt,
  observation: BatchBackendAttemptObservation,
): Promise<BatchExecutionReconciliation> => {
  assertBatchExecutionReconciliationIdentity(reconciliation, receipt);
  assertBatchBackendAttemptObservationIdentity(observation, receipt);
  if (
    reconciliation.state !== BatchExecutionReconciliationState.Open
    && reconciliation.state !== BatchExecutionReconciliationState.RunningObserved
  ) {
    return reconciliation;
  }
  const patch = buildBatchExecutionReconciliationActiveAttemptPatch(observation);
  if (reconciliation.state === BatchExecutionReconciliationState.RunningObserved) {
    assertBatchExecutionReconciliationActiveAttemptEvidenceIdentity(reconciliation, patch);
    return updateBatchExecutionReconciliationObservation(context, reconciliation, patch);
  }
  return advanceBatchExecutionReconciliationState(context, reconciliation, BatchExecutionReconciliationState.RunningObserved, patch);
};

const withBatchExecutionReconciliationLock = async <T>(
  deliveryId: string,
  executeWithLock: () => Promise<T>,
): Promise<T> => {
  const lock = await lockResources([buildBatchExecutionReconciliationLockId(deliveryId)]);
  try {
    return await executeWithLock();
  } finally {
    await lock.unlock();
  }
};

const assertBatchExecutionReconciliationRecordIdentity = (
  reconciliation: BatchExecutionReconciliation,
  expectedReconciliation: BatchExecutionReconciliation,
) => {
  const conflictingFields = Object.entries({
    internal_id: reconciliation.internal_id !== expectedReconciliation.internal_id,
    receipt_id: reconciliation.receipt_id !== expectedReconciliation.receipt_id,
    delivery_id: reconciliation.delivery_id !== expectedReconciliation.delivery_id,
    submission_id: reconciliation.submission_id !== expectedReconciliation.submission_id,
    request_fingerprint: reconciliation.request_fingerprint !== expectedReconciliation.request_fingerprint,
    request_contract_version: reconciliation.request_contract_version !== expectedReconciliation.request_contract_version,
    opened_from_receipt_state: reconciliation.opened_from_receipt_state !== expectedReconciliation.opened_from_receipt_state,
    opened_reason: reconciliation.opened_reason !== expectedReconciliation.opened_reason,
  }).filter(([, conflict]) => conflict).map(([field]) => field);
  if (conflictingFields.length > 0) {
    throw batchExecutionReconciliationConflict('Batch execution reconciliation identity changed after persistence', {
      reconciliation_id: reconciliation.internal_id,
      conflicting_fields: conflictingFields,
    });
  }
};

const loadRequiredBatchExecutionReconciliation = async (
  context: AuthContext,
  deliveryId: string,
): Promise<BatchExecutionReconciliation> => {
  const reconciliation = await loadBatchExecutionReconciliation(context, deliveryId);
  if (!reconciliation) {
    throw batchExecutionReconciliationConflict('Batch execution reconciliation cannot be found', {
      delivery_id: deliveryId,
    });
  }
  return reconciliation;
};

const withCurrentBatchExecutionReconciliationLock = async <T>(
  context: AuthContext,
  reconciliation: BatchExecutionReconciliation,
  executeWithLock: (currentReconciliation: BatchExecutionReconciliation) => Promise<T>,
): Promise<T> => {
  return withBatchExecutionReconciliationLock(reconciliation.delivery_id, async () => {
    const currentReconciliation = await loadRequiredBatchExecutionReconciliation(context, reconciliation.delivery_id);
    assertBatchExecutionReconciliationRecordIdentity(currentReconciliation, reconciliation);
    return executeWithLock(currentReconciliation);
  });
};

export const recordBatchExecutionReconciliationAmbiguous = async (
  context: AuthContext,
  reconciliation: BatchExecutionReconciliation,
  error: unknown,
): Promise<BatchExecutionReconciliation> => {
  return withCurrentBatchExecutionReconciliationLock(context, reconciliation, async (currentReconciliation) => {
    return recordBatchExecutionReconciliationAmbiguousUnlocked(context, currentReconciliation, error);
  });
};

export const recordBatchExecutionReconciliationRunningObserved = async (
  context: AuthContext,
  reconciliation: BatchExecutionReconciliation,
  receipt: BatchExecutionReceipt,
): Promise<BatchExecutionReconciliation> => {
  return withCurrentBatchExecutionReconciliationLock(context, reconciliation, async (currentReconciliation) => {
    const observation = await readFreshBatchBackendAttemptObservation(receipt);
    return recordBatchExecutionReconciliationRunningObservedUnlocked(context, currentReconciliation, receipt, observation);
  });
};

export const ensureBatchExecutionReconciliationBeforeRequiresReconciliation = async (
  context: AuthContext,
  receipt: BatchExecutionReceipt,
  openedReason: Exclude<
    BatchExecutionReconciliationOpenedReason,
    BatchExecutionReconciliationOpenedReason.ExplicitStartedReceipt | BatchExecutionReconciliationOpenedReason.ExplicitRequiresReconciliationReceipt
  >,
  error: unknown,
): Promise<BatchExecutionReconciliation> => {
  if (receipt.state !== BatchExecutionReceiptState.Started) {
    throw batchExecutionReconciliationConflict('Batch execution reconciliation reservation requires a STARTED receipt', {
      receipt_id: receipt.internal_id,
      receipt_state: receipt.state,
    });
  }
  return withBatchExecutionReconciliationLock(receipt.delivery_id, async () => {
    const existingReconciliation = await loadBatchExecutionReconciliation(context, receipt.delivery_id);
    if (existingReconciliation) {
      assertBatchExecutionReconciliationIdentity(existingReconciliation, receipt);
      return existingReconciliation;
    }
    return reserveBatchExecutionReconciliation(context, {
      receipt,
      openedReason,
      lastError: error instanceof Error ? error.message : String(error),
    });
  });
};

export const ensureBatchExecutionReconciliationForRequiresReconciliation = async (
  context: AuthContext,
  receipt: BatchExecutionReceipt,
  openedReason: Exclude<
    BatchExecutionReconciliationOpenedReason,
    BatchExecutionReconciliationOpenedReason.ExplicitStartedReceipt | BatchExecutionReconciliationOpenedReason.ExplicitRequiresReconciliationReceipt
  >,
  error: unknown,
): Promise<BatchExecutionReconciliation> => {
  if (receipt.state !== BatchExecutionReceiptState.RequiresReconciliation) {
    throw batchExecutionReconciliationConflict('Batch execution reconciliation observation requires a REQUIRES_RECONCILIATION receipt', {
      receipt_id: receipt.internal_id,
      receipt_state: receipt.state,
    });
  }
  return withBatchExecutionReconciliationLock(receipt.delivery_id, async () => {
    let reconciliation = await loadBatchExecutionReconciliation(context, receipt.delivery_id);
    if (reconciliation) {
      assertBatchExecutionReconciliationIdentity(reconciliation, receipt);
    } else {
      reconciliation = await reserveBatchExecutionReconciliation(context, {
        receipt,
        openedReason,
        lastError: error instanceof Error ? error.message : String(error),
      });
    }
    if (openedReason === BatchExecutionReconciliationOpenedReason.PostStartError) {
      return recordBatchExecutionReconciliationAmbiguousUnlocked(context, reconciliation, error);
    }
    return reconciliation;
  });
};

const getExplicitBatchExecutionReconciliationOpenedReason = (receipt: BatchExecutionReceipt) => {
  switch (receipt.state) {
    case BatchExecutionReceiptState.Started:
      return BatchExecutionReconciliationOpenedReason.ExplicitStartedReceipt;
    case BatchExecutionReceiptState.RequiresReconciliation:
      return BatchExecutionReconciliationOpenedReason.ExplicitRequiresReconciliationReceipt;
    default:
      throw batchExecutionReconciliationConflict('Explicit batch execution reconciliation open requires a STARTED or REQUIRES_RECONCILIATION receipt', {
        receipt_id: receipt.internal_id,
        receipt_state: receipt.state,
      });
  }
};

export const openBatchExecutionReconciliation = async (
  context: AuthContext,
  deliveryId: string,
): Promise<BatchExecutionReconciliation> => {
  const receiptLock = await lockResources([buildBatchExecutionReceiptLockId(deliveryId)]);
  try {
    const receipt = await loadBatchExecutionReceipt(context, deliveryId);
    if (!receipt) {
      throw batchExecutionReconciliationConflict('Batch execution receipt cannot be found for reconciliation', {
        delivery_id: deliveryId,
      });
    }
    const openedReason = getExplicitBatchExecutionReconciliationOpenedReason(receipt);
    return withBatchExecutionReconciliationLock(deliveryId, async () => {
      const existingReconciliation = await loadBatchExecutionReconciliation(context, deliveryId);
      if (existingReconciliation) {
        assertBatchExecutionReconciliationIdentity(existingReconciliation, receipt);
        return existingReconciliation;
      }
      return reserveBatchExecutionReconciliation(context, {
        receipt,
        openedReason,
      });
    });
  } finally {
    await receiptLock.unlock();
  }
};

const assertBatchExecutionReconciliationEvidenceMatchesReceipt = (
  reconciliation: BatchExecutionReconciliation,
  receipt: BatchExecutionReceipt | null | undefined,
): BatchExecutionReceipt => {
  if (!receipt) {
    throw batchExecutionReconciliationConflict('Batch execution reconciliation terminal evidence requires receipt metadata', {
      reconciliation_id: reconciliation.internal_id,
    });
  }
  assertBatchExecutionReconciliationIdentity(reconciliation, receipt);
  return receipt;
};

const assertBatchExecutionReconciliationResultMetadataMatchesReceipt = (
  reconciliation: BatchExecutionReconciliation,
  receipt: BatchExecutionReceipt,
  resultMetadata: BatchExecutionReceiptResultMetadata | null | undefined,
  requestFingerprint: string | null | undefined,
) => {
  if (!resultMetadata) {
    throw batchExecutionReconciliationConflict('Batch execution reconciliation completion requires durable worker-visible result metadata', {
      reconciliation_id: reconciliation.internal_id,
    });
  }
  if (
    requestFingerprint !== receipt.request_fingerprint
    || requestFingerprint !== reconciliation.request_fingerprint
    || resultMetadata.operationCount !== receipt.operation_count
    || resultMetadata.executionMode !== receipt.execution_mode
    || resultMetadata.waitUntil !== receipt.wait_until
    || resultMetadata.materialized !== true
  ) {
    throw batchExecutionReconciliationConflict('Batch execution reconciliation completion metadata does not match the receipt request identity', {
      reconciliation_id: reconciliation.internal_id,
      receipt_id: receipt.internal_id,
    });
  }
};

const assertBatchExecutionReconciliationTerminalEvidenceClass = (
  reconciliation: BatchExecutionReconciliation,
  evidenceClass: BatchExecutionReconciliationTerminalEvidence['evidenceClass'],
  allowedEvidenceClasses: Set<BatchExecutionReconciliationEvidenceClass>,
) => {
  if (BATCH_EXECUTION_RECONCILIATION_REJECTED_TERMINAL_EVIDENCE.has(evidenceClass)) {
    throw batchExecutionReconciliationConflict('Batch execution reconciliation terminal evidence is not admissible', {
      reconciliation_id: reconciliation.internal_id,
      evidence_class: evidenceClass,
    });
  }
  if (!allowedEvidenceClasses.has(evidenceClass as BatchExecutionReconciliationEvidenceClass)) {
    throw batchExecutionReconciliationConflict('Batch execution reconciliation terminal evidence class is not supported', {
      reconciliation_id: reconciliation.internal_id,
      evidence_class: evidenceClass,
    });
  }
};

const buildExistingTerminalReceiptEvidencePatch = (
  receipt: BatchExecutionReceipt,
): BatchExecutionReconciliationTerminalPatch => ({
  evidence_class: BatchExecutionReconciliationEvidenceClass.ExistingTerminalReceipt,
  evidence_ref_type: BatchExecutionReconciliationEvidenceRefType.BatchExecutionReceipt,
  evidence_ref_id: receipt.internal_id,
  evidence_fingerprint: receipt.result_fingerprint ?? receipt.failure_fingerprint,
  resolved_receipt_state: receipt.state,
});

const assertBatchExecutionReconciliationCompletionEvidence = (
  reconciliation: BatchExecutionReconciliation,
  evidence: BatchExecutionReconciliationTerminalEvidence,
): BatchExecutionReconciliationTerminalPatch => {
  assertBatchExecutionReconciliationTerminalEvidenceClass(reconciliation, evidence.evidenceClass, new Set([
    BatchExecutionReconciliationEvidenceClass.ExistingTerminalReceipt,
    BatchExecutionReconciliationEvidenceClass.MaterializationTerminal,
  ]));
  if (evidence.materialized !== undefined && evidence.materialized !== true) {
    throw batchExecutionReconciliationConflict('Batch execution reconciliation completion requires materialized terminal evidence', {
      reconciliation_id: reconciliation.internal_id,
    });
  }
  const receipt = assertBatchExecutionReconciliationEvidenceMatchesReceipt(reconciliation, evidence.receipt);
  if (evidence.evidenceClass === BatchExecutionReconciliationEvidenceClass.ExistingTerminalReceipt) {
    if (receipt.state !== BatchExecutionReceiptState.Completed) {
      throw batchExecutionReconciliationConflict('Batch execution reconciliation completion requires a COMPLETED receipt', {
        reconciliation_id: reconciliation.internal_id,
        receipt_id: receipt.internal_id,
        receipt_state: receipt.state,
      });
    }
    const resultMetadata = readBatchExecutionReceiptResultMetadata(receipt);
    assertBatchExecutionReconciliationResultMetadataMatchesReceipt(
      reconciliation,
      receipt,
      resultMetadata,
      receipt.request_fingerprint,
    );
    if (!receipt.result_fingerprint) {
      throw batchExecutionReconciliationConflict('Batch execution reconciliation completion requires a terminal result fingerprint', {
        reconciliation_id: reconciliation.internal_id,
        receipt_id: receipt.internal_id,
      });
    }
    return buildExistingTerminalReceiptEvidencePatch(receipt);
  }
  assertBatchExecutionReconciliationResultMetadataMatchesReceipt(
    reconciliation,
    receipt,
    evidence.resultMetadata,
    evidence.requestFingerprint,
  );
  throw batchExecutionReconciliationConflict('Batch execution reconciliation materialization terminal evidence is not enabled without a reviewed producer', {
    reconciliation_id: reconciliation.internal_id,
    evidence_class: evidence.evidenceClass,
  });
};

const assertBatchExecutionReconciliationFailureEvidence = (
  reconciliation: BatchExecutionReconciliation,
  evidence: BatchExecutionReconciliationTerminalEvidence,
): BatchExecutionReconciliationTerminalPatch => {
  assertBatchExecutionReconciliationTerminalEvidenceClass(reconciliation, evidence.evidenceClass, new Set([
    BatchExecutionReconciliationEvidenceClass.ExistingTerminalReceipt,
    BatchExecutionReconciliationEvidenceClass.NoEffectTerminal,
  ]));
  const receipt = assertBatchExecutionReconciliationEvidenceMatchesReceipt(reconciliation, evidence.receipt);
  if (evidence.evidenceClass === BatchExecutionReconciliationEvidenceClass.ExistingTerminalReceipt) {
    if (
      receipt.state !== BatchExecutionReceiptState.FailedTerminal
      || receipt.failure_proof !== BatchExecutionReceiptFailureProof.NoEffectTerminal
      || !receipt.failure_fingerprint
    ) {
      throw batchExecutionReconciliationConflict('Batch execution reconciliation terminal failure requires no-effect terminal receipt evidence', {
        reconciliation_id: reconciliation.internal_id,
        receipt_id: receipt.internal_id,
        receipt_state: receipt.state,
        failure_proof: receipt.failure_proof,
      });
    }
    return buildExistingTerminalReceiptEvidencePatch(receipt);
  }
  throw batchExecutionReconciliationConflict('Batch execution reconciliation no-effect terminal evidence is not enabled without a reviewed producer', {
    reconciliation_id: reconciliation.internal_id,
    evidence_class: evidence.evidenceClass,
  });
};

const assertBatchExecutionReconciliationTerminalPatch = (
  reconciliation: BatchExecutionReconciliation,
  patch: BatchExecutionReconciliationTerminalPatch,
) => {
  const conflictingFields = Object.entries({
    evidence_class: reconciliation.evidence_class !== patch.evidence_class,
    evidence_ref_type: reconciliation.evidence_ref_type !== patch.evidence_ref_type,
    evidence_ref_id: reconciliation.evidence_ref_id !== patch.evidence_ref_id,
    evidence_fingerprint: reconciliation.evidence_fingerprint !== patch.evidence_fingerprint,
    resolved_receipt_state: reconciliation.resolved_receipt_state !== patch.resolved_receipt_state,
  }).filter(([, conflict]) => conflict).map(([field]) => field);
  if (conflictingFields.length > 0) {
    throw batchExecutionReconciliationConflict('Batch execution reconciliation terminal evidence changed after persistence', {
      reconciliation_id: reconciliation.internal_id,
      conflicting_fields: conflictingFields,
    });
  }
};

const recordBatchExecutionReconciliationResolvedCompletedUnlocked = async (
  context: AuthContext,
  reconciliation: BatchExecutionReconciliation,
  evidence: BatchExecutionReconciliationTerminalEvidence,
): Promise<BatchExecutionReconciliation> => {
  const patch = assertBatchExecutionReconciliationCompletionEvidence(reconciliation, evidence);
  if (reconciliation.state === BatchExecutionReconciliationState.ResolvedCompleted) {
    assertBatchExecutionReconciliationTerminalPatch(reconciliation, patch);
    return reconciliation;
  }
  return advanceBatchExecutionReconciliationState(context, reconciliation, BatchExecutionReconciliationState.ResolvedCompleted, {
    ...patch,
    resolved_at: now(),
    last_observed_at: now(),
    last_error: null,
  });
};

export const recordBatchExecutionReconciliationResolvedCompleted = async (
  context: AuthContext,
  reconciliation: BatchExecutionReconciliation,
  evidence: BatchExecutionReconciliationTerminalEvidence,
): Promise<BatchExecutionReconciliation> => {
  return withCurrentBatchExecutionReconciliationLock(context, reconciliation, async (currentReconciliation) => {
    return recordBatchExecutionReconciliationResolvedCompletedUnlocked(context, currentReconciliation, evidence);
  });
};

const recordBatchExecutionReconciliationResolvedFailedTerminalUnlocked = async (
  context: AuthContext,
  reconciliation: BatchExecutionReconciliation,
  evidence: BatchExecutionReconciliationTerminalEvidence,
): Promise<BatchExecutionReconciliation> => {
  const patch = assertBatchExecutionReconciliationFailureEvidence(reconciliation, evidence);
  if (reconciliation.state === BatchExecutionReconciliationState.ResolvedFailedTerminal) {
    assertBatchExecutionReconciliationTerminalPatch(reconciliation, patch);
    return reconciliation;
  }
  return advanceBatchExecutionReconciliationState(context, reconciliation, BatchExecutionReconciliationState.ResolvedFailedTerminal, {
    ...patch,
    resolved_at: now(),
    last_observed_at: now(),
    last_error: null,
  });
};

export const recordBatchExecutionReconciliationResolvedFailedTerminal = async (
  context: AuthContext,
  reconciliation: BatchExecutionReconciliation,
  evidence: BatchExecutionReconciliationTerminalEvidence,
): Promise<BatchExecutionReconciliation> => {
  return withCurrentBatchExecutionReconciliationLock(context, reconciliation, async (currentReconciliation) => {
    return recordBatchExecutionReconciliationResolvedFailedTerminalUnlocked(context, currentReconciliation, evidence);
  });
};

export const inspectBatchExecutionReconciliationAttemptObservation = async (
  context: AuthContext,
  deliveryId: string,
): Promise<BatchExecutionReconciliation> => {
  const receiptLock = await lockResources([buildBatchExecutionReceiptLockId(deliveryId)]);
  try {
    const receipt = await loadBatchExecutionReceipt(context, deliveryId);
    if (!receipt) {
      throw batchExecutionReconciliationConflict('Batch execution receipt cannot be found for reconciliation inspection', {
        delivery_id: deliveryId,
      });
    }
    return withBatchExecutionReconciliationLock(deliveryId, async () => {
      const reconciliation = await loadRequiredBatchExecutionReconciliation(context, deliveryId);
      assertBatchExecutionReconciliationIdentity(reconciliation, receipt);
      if (receipt.state === BatchExecutionReceiptState.Completed) {
        return recordBatchExecutionReconciliationResolvedCompletedUnlocked(context, reconciliation, {
          evidenceClass: BatchExecutionReconciliationEvidenceClass.ExistingTerminalReceipt,
          receipt,
        });
      }
      if (
        receipt.state === BatchExecutionReceiptState.FailedTerminal
        && receipt.failure_proof === BatchExecutionReceiptFailureProof.NoEffectTerminal
      ) {
        return recordBatchExecutionReconciliationResolvedFailedTerminalUnlocked(context, reconciliation, {
          evidenceClass: BatchExecutionReconciliationEvidenceClass.ExistingTerminalReceipt,
          receipt,
        });
      }
      let observation: BatchBackendAttemptObservation;
      try {
        observation = await readFreshBatchBackendAttemptObservation(receipt);
      } catch (error) {
        if (reconciliation.state === BatchExecutionReconciliationState.RunningObserved) {
          return recordBatchExecutionReconciliationAmbiguousUnlocked(context, reconciliation, error);
        }
        return reconciliation;
      }
      return recordBatchExecutionReconciliationRunningObservedUnlocked(context, reconciliation, receipt, observation);
    });
  } finally {
    await receiptLock.unlock();
  }
};
