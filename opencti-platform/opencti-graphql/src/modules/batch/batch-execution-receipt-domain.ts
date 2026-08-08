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
import { loadBatchDelivery, readBatchDeliveryQueueMessage } from './batch-delivery-domain';
import {
  BatchAdmissionErrorCode,
  type BatchDirectDeliveryContext,
  type BatchExecutionReceipt,
  BatchExecutionReceiptCompletionBoundary,
  BatchExecutionReceiptFailureProof,
  type BatchExecutionReceiptRequestInput,
  type BatchExecutionReceiptRequestMetadata,
  type BatchExecutionReceiptResultMetadata,
  BatchExecutionReceiptState,
  type BatchExecutionReceiptTerminalFailure,
  BatchDeliveryProtocol,
  ENTITY_TYPE_BATCH_EXECUTION_RECEIPT,
} from './batch-types';

const BATCH_EXECUTION_RECEIPT_PREFIX = 'batch-execution-receipt--';
const BATCH_EXECUTION_RECEIPT_LOCK_PREFIX = 'batch-execution-receipt:';
const BATCH_EXECUTION_RECEIPT_REQUEST_CONTRACT_VERSION = 1;
const BATCH_EXECUTION_RECEIPT_RESULT_VERSION = 1;
const BATCH_EXECUTION_RECEIPT_ALLOWED_TRANSITIONS = {
  [BatchExecutionReceiptState.Prepared]: new Set([
    BatchExecutionReceiptState.Started,
    BatchExecutionReceiptState.FailedTerminal,
  ]),
  [BatchExecutionReceiptState.Started]: new Set([
    BatchExecutionReceiptState.Completed,
    BatchExecutionReceiptState.FailedTerminal,
    BatchExecutionReceiptState.RequiresReconciliation,
  ]),
  [BatchExecutionReceiptState.Completed]: new Set<BatchExecutionReceiptState>(),
  [BatchExecutionReceiptState.FailedTerminal]: new Set<BatchExecutionReceiptState>(),
  [BatchExecutionReceiptState.RequiresReconciliation]: new Set([
    BatchExecutionReceiptState.Completed,
    BatchExecutionReceiptState.FailedTerminal,
  ]),
};

export interface ReserveBatchExecutionReceiptInput {
  deliveryId: string;
  submissionId: string;
  deliveryPayloadFingerprint: string;
  executionMode: BatchExecutionReceipt['execution_mode'];
  waitUntil: BatchExecutionReceipt['wait_until'];
  requestMetadata: BatchExecutionReceiptRequestMetadata;
}

type BatchExecutionReceiptPatch = Partial<Pick<
  BatchExecutionReceipt,
  | 'started_at'
  | 'completed_at'
  | 'materialized_at'
  | 'result_fingerprint'
  | 'result_version'
  | 'result_operation_count'
  | 'result_operation_errors'
  | 'result_execution_mode'
  | 'result_wait_until'
  | 'result_side_effect_kinds'
  | 'result_materialized'
  | 'completion_boundary'
  | 'side_effect_kind_counts'
  | 'failure_stage'
  | 'failure_code'
  | 'failure_message'
  | 'failure_fingerprint'
  | 'failure_retryable'
  | 'failure_proof'
  | 'failed_at'
  | 'reconciliation_required_at'
  | 'last_error'
>>;

const canonicalizeOrThrow = (value: unknown, message: string): string => {
  const canonicalValue = jsonCanonicalize(value);
  if (typeof canonicalValue !== 'string') {
    throw FunctionalError(message);
  }
  return canonicalValue;
};

const batchExecutionReceiptConflict = (message: string, data: Record<string, unknown> = {}) => {
  return FunctionalError(message, {
    batch_error_code: BatchAdmissionErrorCode.ExecutionReceiptConflict,
    ...data,
  });
};

export const buildBatchExecutionReceiptRequiresReconciliationError = (
  receipt: BatchExecutionReceipt,
) => {
  return FunctionalError('Batch execution receipt requires reconciliation before replay', {
    batch_error_code: BatchAdmissionErrorCode.ExecutionRequiresReconciliation,
    receipt_id: receipt.internal_id,
    delivery_id: receipt.delivery_id,
    receipt_state: receipt.state,
  });
};

export const buildBatchExecutionReceiptTerminalFailureError = (
  receipt: BatchExecutionReceipt,
) => {
  return FunctionalError(receipt.failure_message ?? 'Batch execution receipt failed terminally', {
    batch_error_code: BatchAdmissionErrorCode.ExecutionFailedTerminal,
    receipt_id: receipt.internal_id,
    delivery_id: receipt.delivery_id,
    receipt_state: receipt.state,
    failure_stage: receipt.failure_stage,
    failure_code: receipt.failure_code,
    failure_proof: receipt.failure_proof,
  });
};

const assertBatchExecutionReceiptState = (state: BatchExecutionReceiptState) => {
  if (!BATCH_EXECUTION_RECEIPT_ALLOWED_TRANSITIONS[state]) {
    throw FunctionalError('Invalid batch execution receipt state', { state });
  }
};

const assertBatchExecutionReceiptTransition = (
  receipt: BatchExecutionReceipt,
  state: BatchExecutionReceiptState,
) => {
  assertBatchExecutionReceiptState(receipt.state);
  assertBatchExecutionReceiptState(state);
  if (receipt.state === state) {
    return;
  }
  if (!BATCH_EXECUTION_RECEIPT_ALLOWED_TRANSITIONS[receipt.state].has(state)) {
    throw batchExecutionReceiptConflict('Invalid batch execution receipt state transition', {
      receipt_id: receipt.internal_id,
      receipt_state: receipt.state,
      next_receipt_state: state,
    });
  }
};

const countValues = (values: string[]): Record<string, number> => {
  const counts = new Map<string, number>();
  values.forEach((value) => counts.set(value, (counts.get(value) ?? 0) + 1));
  return Object.fromEntries(counts);
};

const normalizeResultMetadata = (
  result: BatchExecutionReceiptResultMetadata,
): BatchExecutionReceiptResultMetadata => ({
  operationCount: result.operationCount,
  operationErrors: result.operationErrors.map((operationError) => ({
    ...(operationError.code ? { code: operationError.code } : {}),
    message: operationError.message,
    ...(operationError.objectId ? { objectId: operationError.objectId } : {}),
    operationIndex: operationError.operationIndex,
    retryable: operationError.retryable,
  })),
  executionMode: result.executionMode,
  waitUntil: result.waitUntil,
  sideEffectKinds: [...result.sideEffectKinds],
  materialized: true,
});

const buildBatchExecutionReceiptResultFingerprint = (
  result: BatchExecutionReceiptResultMetadata,
): string => {
  return hashSHA256(canonicalizeOrThrow(normalizeResultMetadata(result), 'Invalid batch execution receipt result metadata'));
};

const buildBatchExecutionReceiptFailureFingerprint = (
  failure: BatchExecutionReceiptTerminalFailure,
): string => {
  return hashSHA256(canonicalizeOrThrow({
    stage: failure.stage,
    code: failure.code ?? null,
    message: failure.message,
    proof: failure.proof,
    retryable: false,
  }, 'Invalid batch execution receipt terminal failure'));
};

export const buildBatchExecutionReceiptId = (deliveryId: string): string => {
  return `${BATCH_EXECUTION_RECEIPT_PREFIX}${hashSHA256(deliveryId)}`;
};

export const buildBatchExecutionReceiptLockId = (deliveryId: string): string => {
  return `${BATCH_EXECUTION_RECEIPT_LOCK_PREFIX}${deliveryId}`;
};

export const buildBatchExecutionReceiptRequestMetadata = (
  input: BatchExecutionReceiptRequestInput,
): BatchExecutionReceiptRequestMetadata => {
  const batchPlanFingerprint = hashSHA256(canonicalizeOrThrow(input.batchPlan, 'Invalid batch execution receipt batch plan'));
  const operationManifestFingerprint = hashSHA256(canonicalizeOrThrow(input.operations, 'Invalid batch execution receipt operation manifest'));
  const requestFingerprint = hashSHA256(canonicalizeOrThrow({
    delivery_id: input.delivery.internal_id,
    delivery_payload_fingerprint: input.delivery.payload_fingerprint,
    execution_mode: input.executionMode,
    wait_until: input.waitUntil,
    batch_plan: input.batchPlan,
    operations: input.operations,
  }, 'Invalid batch execution receipt request'));
  return {
    requestContractVersion: BATCH_EXECUTION_RECEIPT_REQUEST_CONTRACT_VERSION,
    requestFingerprint,
    batchPlanFingerprint,
    operationManifestFingerprint,
    operationCount: input.operations.length,
  };
};

export const assertBatchDirectDeliveryContext = async (
  context: AuthContext,
  directDeliveryContext: BatchDirectDeliveryContext,
) => {
  if (directDeliveryContext.delivery_protocol_version !== BatchDeliveryProtocol.V2) {
    throw batchExecutionReceiptConflict('Batch direct execution requires a protocol-v2 delivery context', {
      delivery_id: directDeliveryContext.delivery_id,
      delivery_protocol_version: directDeliveryContext.delivery_protocol_version,
    });
  }
  const delivery = await loadBatchDelivery(context, directDeliveryContext.delivery_id);
  if (!delivery) {
    throw batchExecutionReceiptConflict('Batch direct execution delivery cannot be found', {
      delivery_id: directDeliveryContext.delivery_id,
    });
  }
  const queueMessage = readBatchDeliveryQueueMessage(delivery);
  const conflictingFields = Object.entries({
    submission_id: delivery.submission_id !== directDeliveryContext.submission_id,
    parent_delivery_id: delivery.parent_delivery_id !== directDeliveryContext.parent_delivery_id,
    delivery_kind: delivery.delivery_kind !== directDeliveryContext.delivery_kind,
    delivery_protocol_version: delivery.required_worker_protocol !== BatchDeliveryProtocol.V2,
    delivery_branch_kind: delivery.branch_kind !== directDeliveryContext.delivery_branch_kind,
    delivery_branch_sequence: delivery.branch_sequence !== directDeliveryContext.delivery_branch_sequence,
    delivery_branch_ordinal: delivery.branch_ordinal !== directDeliveryContext.delivery_branch_ordinal,
    queue_submission_id: queueMessage.submission_id !== directDeliveryContext.submission_id,
  }).filter(([, conflict]) => conflict).map(([field]) => field);
  if (conflictingFields.length > 0) {
    throw batchExecutionReceiptConflict('Batch direct execution context does not match the durable delivery', {
      delivery_id: delivery.internal_id,
      conflicting_fields: conflictingFields,
    });
  }
  return delivery;
};

export const loadBatchExecutionReceipt = async (
  context: AuthContext,
  deliveryId: string,
): Promise<BatchExecutionReceipt | null> => {
  const receiptId = buildBatchExecutionReceiptId(deliveryId);
  const receipt = await elLoadById(context, SYSTEM_USER, receiptId, {
    type: ENTITY_TYPE_BATCH_EXECUTION_RECEIPT,
    indices: READ_INDEX_INTERNAL_OBJECTS,
  });
  return receipt ? receipt as unknown as BatchExecutionReceipt : null;
};

export const assertBatchExecutionReceiptReservation = (
  receipt: BatchExecutionReceipt,
  input: ReserveBatchExecutionReceiptInput,
): void => {
  const conflictingFields = Object.entries({
    delivery_id: receipt.delivery_id !== input.deliveryId,
    submission_id: receipt.submission_id !== input.submissionId,
    delivery_payload_fingerprint: receipt.delivery_payload_fingerprint !== input.deliveryPayloadFingerprint,
    request_contract_version: receipt.request_contract_version !== input.requestMetadata.requestContractVersion,
    request_fingerprint: receipt.request_fingerprint !== input.requestMetadata.requestFingerprint,
    batch_plan_fingerprint: receipt.batch_plan_fingerprint !== input.requestMetadata.batchPlanFingerprint,
    operation_manifest_fingerprint: receipt.operation_manifest_fingerprint !== input.requestMetadata.operationManifestFingerprint,
    operation_count: receipt.operation_count !== input.requestMetadata.operationCount,
    execution_mode: receipt.execution_mode !== input.executionMode,
    wait_until: receipt.wait_until !== input.waitUntil,
  }).filter(([, conflict]) => conflict).map(([field]) => field);
  if (conflictingFields.length > 0) {
    throw batchExecutionReceiptConflict('Batch execution receipt is already associated with different immutable request data', {
      receipt_id: receipt.internal_id,
      delivery_id: receipt.delivery_id,
      conflicting_fields: conflictingFields,
    });
  }
};

export const reserveBatchExecutionReceipt = async (
  context: AuthContext,
  input: ReserveBatchExecutionReceiptInput,
): Promise<BatchExecutionReceipt> => {
  const existingReceipt = await loadBatchExecutionReceipt(context, input.deliveryId);
  if (existingReceipt) {
    assertBatchExecutionReceiptReservation(existingReceipt, input);
    return existingReceipt;
  }
  const createdAt = now();
  const receiptId = buildBatchExecutionReceiptId(input.deliveryId);
  const receipt: BatchExecutionReceipt = {
    id: receiptId,
    internal_id: receiptId,
    standard_id: receiptId,
    entity_type: ENTITY_TYPE_BATCH_EXECUTION_RECEIPT,
    base_type: BASE_TYPE_ENTITY,
    parent_types: getParentTypes(ENTITY_TYPE_BATCH_EXECUTION_RECEIPT),
    delivery_id: input.deliveryId,
    submission_id: input.submissionId,
    delivery_payload_fingerprint: input.deliveryPayloadFingerprint,
    request_contract_version: input.requestMetadata.requestContractVersion,
    request_fingerprint: input.requestMetadata.requestFingerprint,
    batch_plan_fingerprint: input.requestMetadata.batchPlanFingerprint,
    operation_manifest_fingerprint: input.requestMetadata.operationManifestFingerprint,
    operation_count: input.requestMetadata.operationCount,
    execution_mode: input.executionMode,
    wait_until: input.waitUntil,
    state: BatchExecutionReceiptState.Prepared,
    result_fingerprint: null,
    result_version: null,
    result_operation_count: null,
    result_operation_errors: null,
    result_execution_mode: null,
    result_wait_until: null,
    result_side_effect_kinds: [],
    result_materialized: null,
    completion_boundary: null,
    side_effect_kind_counts: null,
    prepared_at: createdAt,
    started_at: null,
    completed_at: null,
    materialized_at: null,
    failure_stage: null,
    failure_code: null,
    failure_message: null,
    failure_fingerprint: null,
    failure_retryable: null,
    failure_proof: null,
    failed_at: null,
    reconciliation_required_at: null,
    created_at: createdAt,
    updated_at: createdAt,
    last_error: null,
  };
  await elIndex(INDEX_INTERNAL_OBJECTS, receipt, { context });
  return receipt;
};

const advanceBatchExecutionReceiptState = async (
  context: AuthContext,
  receipt: BatchExecutionReceipt,
  state: BatchExecutionReceiptState,
  patch: BatchExecutionReceiptPatch = {},
): Promise<BatchExecutionReceipt> => {
  assertBatchExecutionReceiptTransition(receipt, state);
  if (receipt.state === state) {
    return receipt;
  }
  const updatedReceipt = {
    ...receipt,
    ...patch,
    state,
    updated_at: now(),
  };
  await elUpdate(context, receipt._index ?? INDEX_INTERNAL_OBJECTS, receipt.internal_id, {
    doc: {
      ...patch,
      state,
      updated_at: updatedReceipt.updated_at,
    },
  });
  return updatedReceipt;
};

export const recordBatchExecutionReceiptStarted = async (
  context: AuthContext,
  receipt: BatchExecutionReceipt,
): Promise<BatchExecutionReceipt> => {
  return advanceBatchExecutionReceiptState(context, receipt, BatchExecutionReceiptState.Started, {
    started_at: receipt.started_at ?? now(),
    last_error: null,
  });
};

export const readBatchExecutionReceiptResultMetadata = (
  receipt: BatchExecutionReceipt,
): BatchExecutionReceiptResultMetadata | null => {
  if (
    receipt.result_operation_count === null
    || receipt.result_operation_errors === null
    || receipt.result_execution_mode === null
    || receipt.result_wait_until === null
    || receipt.result_materialized !== true
  ) {
    return null;
  }
  try {
    const operationErrors = JSON.parse(receipt.result_operation_errors);
    if (!Array.isArray(operationErrors)) {
      throw new Error('operation errors must be an array');
    }
    return {
      operationCount: receipt.result_operation_count,
      operationErrors,
      executionMode: receipt.result_execution_mode,
      waitUntil: receipt.result_wait_until,
      sideEffectKinds: Array.isArray(receipt.result_side_effect_kinds) ? receipt.result_side_effect_kinds : [],
      materialized: true,
    };
  } catch (cause) {
    throw batchExecutionReceiptConflict('Batch execution receipt has invalid cached result metadata', {
      receipt_id: receipt.internal_id,
      cause,
    });
  }
};

export const recordBatchExecutionReceiptCompletion = async (
  context: AuthContext,
  receipt: BatchExecutionReceipt,
  result: BatchExecutionReceiptResultMetadata,
): Promise<BatchExecutionReceipt> => {
  if (result.materialized !== true) {
    throw batchExecutionReceiptConflict('Batch execution receipt completion requires materialized terminal evidence', {
      receipt_id: receipt.internal_id,
    });
  }
  const normalizedResult = normalizeResultMetadata(result);
  const resultFingerprint = buildBatchExecutionReceiptResultFingerprint(normalizedResult);
  if (receipt.state === BatchExecutionReceiptState.Completed) {
    if (receipt.result_fingerprint !== resultFingerprint) {
      throw batchExecutionReceiptConflict('Batch execution receipt completion metadata changed after terminal persistence', {
        receipt_id: receipt.internal_id,
      });
    }
    return receipt;
  }
  const completedAt = now();
  return advanceBatchExecutionReceiptState(context, receipt, BatchExecutionReceiptState.Completed, {
    result_fingerprint: resultFingerprint,
    result_version: BATCH_EXECUTION_RECEIPT_RESULT_VERSION,
    result_operation_count: normalizedResult.operationCount,
    result_operation_errors: canonicalizeOrThrow(normalizedResult.operationErrors, 'Invalid batch execution receipt operation errors'),
    result_execution_mode: normalizedResult.executionMode,
    result_wait_until: normalizedResult.waitUntil,
    result_side_effect_kinds: normalizedResult.sideEffectKinds,
    result_materialized: true,
    completion_boundary: BatchExecutionReceiptCompletionBoundary.Materialized,
    side_effect_kind_counts: canonicalizeOrThrow(countValues(normalizedResult.sideEffectKinds), 'Invalid batch execution receipt side effect counts'),
    completed_at: completedAt,
    materialized_at: completedAt,
    last_error: null,
  });
};

export const recordBatchExecutionReceiptTerminalFailure = async (
  context: AuthContext,
  receipt: BatchExecutionReceipt,
  failure: BatchExecutionReceiptTerminalFailure,
): Promise<BatchExecutionReceipt> => {
  if (
    failure.proof !== BatchExecutionReceiptFailureProof.PreStartValidation
    && failure.proof !== BatchExecutionReceiptFailureProof.NoEffectTerminal
  ) {
    throw batchExecutionReceiptConflict('Batch execution receipt terminal failure proof is invalid', {
      receipt_id: receipt.internal_id,
      failure_proof: failure.proof,
    });
  }
  if (
    failure.proof === BatchExecutionReceiptFailureProof.PreStartValidation
    && receipt.state !== BatchExecutionReceiptState.Prepared
  ) {
    throw batchExecutionReceiptConflict('Pre-start terminal failure evidence cannot be recorded after execution starts', {
      receipt_id: receipt.internal_id,
      receipt_state: receipt.state,
    });
  }
  const failureFingerprint = buildBatchExecutionReceiptFailureFingerprint(failure);
  if (receipt.state === BatchExecutionReceiptState.FailedTerminal) {
    if (receipt.failure_fingerprint !== failureFingerprint) {
      throw batchExecutionReceiptConflict('Batch execution receipt terminal failure metadata changed after persistence', {
        receipt_id: receipt.internal_id,
      });
    }
    return receipt;
  }
  const failedAt = now();
  return advanceBatchExecutionReceiptState(context, receipt, BatchExecutionReceiptState.FailedTerminal, {
    failure_stage: failure.stage,
    failure_code: failure.code ?? null,
    failure_message: failure.message,
    failure_fingerprint: failureFingerprint,
    failure_retryable: false,
    failure_proof: failure.proof,
    failed_at: failedAt,
    last_error: failure.message,
  });
};

export const recordBatchExecutionReceiptRequiresReconciliation = async (
  context: AuthContext,
  receipt: BatchExecutionReceipt,
  error: unknown,
): Promise<BatchExecutionReceipt> => {
  if (receipt.state === BatchExecutionReceiptState.RequiresReconciliation) {
    return receipt;
  }
  const errorMessage = error instanceof Error ? error.message : String(error);
  return advanceBatchExecutionReceiptState(context, receipt, BatchExecutionReceiptState.RequiresReconciliation, {
    reconciliation_required_at: now(),
    last_error: errorMessage,
  });
};
