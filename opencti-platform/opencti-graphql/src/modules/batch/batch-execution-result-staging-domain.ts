import jsonCanonicalize from 'canonicalize';
import { FunctionalError } from '../../config/errors';
import { elIndex, elLoadById } from '../../database/engine';
import { INDEX_INTERNAL_OBJECTS, READ_INDEX_INTERNAL_OBJECTS } from '../../database/utils';
import { lockResources } from '../../lock/master-lock';
import { BASE_TYPE_ENTITY } from '../../schema/general';
import { getParentTypes } from '../../schema/schemaUtils';
import type { AuthContext } from '../../types/user';
import { SYSTEM_USER } from '../../utils/access';
import { now } from '../../utils/format';
import { hashSHA256 } from '../../utils/hash';
import { BatchSideEffectKind } from './batch-executor';
import {
  BatchAdmissionErrorCode,
  type BatchExecutionReceipt,
  type BatchExecutionReceiptOperationError,
  type BatchExecutionResultStaging,
  type BatchExecutionResultStagingPayload,
  BatchExecutionMode,
  BatchWaitUntil,
  ENTITY_TYPE_BATCH_EXECUTION_RESULT_STAGING,
} from './batch-types';

const BATCH_EXECUTION_RESULT_STAGING_ID_SCOPE = 'batch-execution-result-staging:';
const BATCH_EXECUTION_RESULT_STAGING_LOCK_PREFIX = 'batch-execution-result-staging:';
const BATCH_EXECUTION_RESULT_STAGING_DELIVERY_ID_MAX_LENGTH = 128;
const SHA256_HEX_PATTERN = /^[a-f0-9]{64}$/;
const STAGING_DRAFT_FIELDS = new Set([
  'staging_id',
  'receipt_id',
  'delivery_id',
  'submission_id',
  'request_fingerprint',
  'request_contract_version',
  'result_version',
  'operation_count',
  'operation_errors',
  'execution_mode',
  'wait_until',
  'side_effect_kinds',
  'serialized_bytes',
  'staging_fingerprint',
]);
const STAGING_RECORD_FIELDS = new Set([
  '_id',
  '_index',
  'id',
  'internal_id',
  'sort',
  'standard_id',
  'entity_type',
  'base_type',
  'parent_types',
  ...STAGING_DRAFT_FIELDS,
  'staged_at',
  'created_at',
  'updated_at',
]);
const OPERATION_ERROR_FIELDS = new Set([
  'code',
  'message',
  'objectId',
  'operationIndex',
  'retryable',
]);
const RESULT_INPUT_FIELDS = new Set([
  'operationCount',
  'operationErrors',
  'executionMode',
  'waitUntil',
  'sideEffectKinds',
]);
const EXECUTION_MODE_VALUES = new Set(Object.values(BatchExecutionMode));
const WAIT_UNTIL_VALUES = new Set(Object.values(BatchWaitUntil));
const SIDE_EFFECT_KIND_VALUES = new Set(Object.values(BatchSideEffectKind));

export const BATCH_EXECUTION_RESULT_STAGING_VERSION = 1;
export const BATCH_EXECUTION_RESULT_STAGING_MAX_SERIALIZED_BYTES = 1024 * 1024;

export interface BuildBatchExecutionResultStagingInput {
  receipt: Pick<
    BatchExecutionReceipt,
    'internal_id' | 'delivery_id' | 'submission_id' | 'request_fingerprint' | 'request_contract_version'
  >;
  result: BatchExecutionResultStagingPayload;
}

export interface BatchExecutionResultStagingDraft {
  staging_id: string;
  receipt_id: string;
  delivery_id: string;
  submission_id: string;
  request_fingerprint: string;
  request_contract_version: number;
  result_version: number;
  operation_count: number;
  operation_errors: BatchExecutionReceiptOperationError[];
  execution_mode: BatchExecutionMode;
  wait_until: BatchWaitUntil;
  side_effect_kinds: string[];
  serialized_bytes: number;
  staging_fingerprint: string;
}

type BatchExecutionResultStagingFingerprintInput = Pick<
  BatchExecutionResultStagingDraft,
  | 'receipt_id'
  | 'delivery_id'
  | 'submission_id'
  | 'request_fingerprint'
  | 'request_contract_version'
  | 'result_version'
  | 'operation_count'
  | 'operation_errors'
  | 'execution_mode'
  | 'wait_until'
  | 'side_effect_kinds'
>;

const batchExecutionResultStagingConflict = (message: string, data: Record<string, unknown> = {}) => {
  return FunctionalError(message, {
    batch_error_code: BatchAdmissionErrorCode.ExecutionResultStagingConflict,
    ...data,
  });
};

const canonicalizeOrThrow = (value: unknown, message: string): string => {
  const canonicalValue = jsonCanonicalize(value);
  if (typeof canonicalValue !== 'string') {
    throw batchExecutionResultStagingConflict(message);
  }
  return canonicalValue;
};

const isRecord = (value: unknown): value is Record<string, unknown> => {
  return !!value && typeof value === 'object' && !Array.isArray(value);
};

const isNonEmptyString = (value: unknown): value is string => typeof value === 'string' && value.length > 0;

const assertBoundedNonEmptyString = (value: string, field: string, maxBytes: number) => {
  if (!isNonEmptyString(value) || Buffer.byteLength(value) > maxBytes) {
    throw batchExecutionResultStagingConflict('Invalid batch execution result staging identity', {
      field,
      value,
    });
  }
};

const assertSha256Hex = (value: string, field: string) => {
  if (!SHA256_HEX_PATTERN.test(value)) {
    throw batchExecutionResultStagingConflict('Invalid batch execution result staging fingerprint', {
      field,
      value,
    });
  }
};

const assertNonNegativeInteger = (value: number, field: string) => {
  if (!Number.isInteger(value) || value < 0) {
    throw batchExecutionResultStagingConflict('Invalid batch execution result staging integer field', {
      field,
      value,
    });
  }
};

const assertPositiveInteger = (value: number, field: string) => {
  if (!Number.isInteger(value) || value <= 0) {
    throw batchExecutionResultStagingConflict('Invalid batch execution result staging integer field', {
      field,
      value,
    });
  }
};

const assertIsoTimestamp = (value: string, field: string) => {
  if (!isNonEmptyString(value) || Number.isNaN(Date.parse(value))) {
    throw batchExecutionResultStagingConflict('Invalid batch execution result staging timestamp', {
      field,
      value,
    });
  }
};

const assertStagingIdentityInput = (
  input: Pick<
    BatchExecutionResultStagingDraft,
    'receipt_id' | 'delivery_id' | 'submission_id' | 'request_fingerprint' | 'request_contract_version'
  >,
) => {
  assertBoundedNonEmptyString(input.delivery_id, 'delivery_id', BATCH_EXECUTION_RESULT_STAGING_DELIVERY_ID_MAX_LENGTH);
  if (!isNonEmptyString(input.receipt_id) || !isNonEmptyString(input.submission_id)) {
    throw batchExecutionResultStagingConflict('Invalid batch execution result staging identity', {
      receipt_id: input.receipt_id,
      submission_id: input.submission_id,
    });
  }
  assertSha256Hex(input.request_fingerprint, 'request_fingerprint');
  assertPositiveInteger(input.request_contract_version, 'request_contract_version');
};

function assertExecutionMode(value: unknown): asserts value is BatchExecutionMode {
  if (typeof value !== 'string' || !EXECUTION_MODE_VALUES.has(value as BatchExecutionMode)) {
    throw batchExecutionResultStagingConflict('Invalid batch execution result staging execution mode', {
      execution_mode: value,
    });
  }
}

function assertWaitUntil(value: unknown): asserts value is BatchWaitUntil {
  if (typeof value !== 'string' || !WAIT_UNTIL_VALUES.has(value as BatchWaitUntil)) {
    throw batchExecutionResultStagingConflict('Invalid batch execution result staging wait_until value', {
      wait_until: value,
    });
  }
}

function assertResultInput(value: unknown): asserts value is BatchExecutionResultStagingPayload {
  if (!isRecord(value)) {
    throw batchExecutionResultStagingConflict('Batch execution result staging input payload is malformed');
  }
  const unexpectedFields = Object.keys(value).filter((field) => !RESULT_INPUT_FIELDS.has(field));
  if (unexpectedFields.length > 0) {
    throw batchExecutionResultStagingConflict('Batch execution result staging input payload is malformed', {
      unexpected_fields: unexpectedFields,
    });
  }
}

const normalizeOperationError = (
  value: unknown,
  operationCount: number,
  errorIndex: number,
): BatchExecutionReceiptOperationError => {
  if (!isRecord(value)) {
    throw batchExecutionResultStagingConflict('Batch execution result staging operation error payload is malformed', {
      error_index: errorIndex,
    });
  }
  const unexpectedFields = Object.keys(value).filter((field) => !OPERATION_ERROR_FIELDS.has(field));
  if (unexpectedFields.length > 0) {
    throw batchExecutionResultStagingConflict('Batch execution result staging operation error payload is malformed', {
      error_index: errorIndex,
      unexpected_fields: unexpectedFields,
    });
  }
  if (
    (value.code !== undefined && typeof value.code !== 'string')
    || typeof value.message !== 'string'
    || (value.objectId !== undefined && typeof value.objectId !== 'string')
    || !Number.isInteger(value.operationIndex)
    || typeof value.retryable !== 'boolean'
  ) {
    throw batchExecutionResultStagingConflict('Batch execution result staging operation error payload is malformed', {
      error_index: errorIndex,
    });
  }
  const operationIndex = value.operationIndex as number;
  if (operationIndex < 0 || operationIndex >= operationCount) {
    throw batchExecutionResultStagingConflict('Batch execution result staging operation error index is outside the operation range', {
      error_index: errorIndex,
      operation_count: operationCount,
      operation_index: operationIndex,
    });
  }
  return {
    ...(typeof value.code === 'string' ? { code: value.code } : {}),
    message: value.message,
    ...(typeof value.objectId === 'string' ? { objectId: value.objectId } : {}),
    operationIndex,
    retryable: value.retryable,
  };
};

const normalizeOperationErrors = (
  value: unknown,
  operationCount: number,
): BatchExecutionReceiptOperationError[] => {
  if (!Array.isArray(value)) {
    throw batchExecutionResultStagingConflict('Batch execution result staging operation errors must be an array');
  }
  for (let index = 0; index < value.length; index += 1) {
    if (!Object.prototype.hasOwnProperty.call(value, index)) {
      throw batchExecutionResultStagingConflict('Batch execution result staging operation errors must be a dense array', {
        error_index: index,
      });
    }
  }
  if (value.length > operationCount) {
    throw batchExecutionResultStagingConflict('Batch execution result staging operation error count exceeds the operation count', {
      operation_count: operationCount,
      operation_error_count: value.length,
    });
  }
  let previousOperationIndex = -1;
  return value.map((operationError, errorIndex) => {
    const normalizedError = normalizeOperationError(operationError, operationCount, errorIndex);
    if (normalizedError.operationIndex <= previousOperationIndex) {
      throw batchExecutionResultStagingConflict('Batch execution result staging operation errors are not ordered by operation index', {
        error_index: errorIndex,
        operation_index: normalizedError.operationIndex,
        previous_operation_index: previousOperationIndex,
      });
    }
    previousOperationIndex = normalizedError.operationIndex;
    return normalizedError;
  });
};

const normalizeSideEffectKinds = (value: unknown): string[] => {
  if (!Array.isArray(value)) {
    throw batchExecutionResultStagingConflict('Batch execution result staging side effect kinds must be an array');
  }
  for (let index = 0; index < value.length; index += 1) {
    if (!Object.prototype.hasOwnProperty.call(value, index)) {
      throw batchExecutionResultStagingConflict('Batch execution result staging side effect kinds must be a dense array', {
        side_effect_kind_index: index,
      });
    }
  }
  return value.map((sideEffectKind, index) => {
    if (typeof sideEffectKind !== 'string' || !SIDE_EFFECT_KIND_VALUES.has(sideEffectKind as BatchSideEffectKind)) {
      throw batchExecutionResultStagingConflict('Batch execution result staging side effect kind is invalid', {
        side_effect_kind: sideEffectKind,
        side_effect_kind_index: index,
      });
    }
    return sideEffectKind;
  });
};

const buildBatchExecutionResultStagingFingerprintPayload = (
  input: BatchExecutionResultStagingFingerprintInput,
): Record<string, unknown> => ({
  result_version: input.result_version,
  receipt_id: input.receipt_id,
  delivery_id: input.delivery_id,
  submission_id: input.submission_id,
  request_fingerprint: input.request_fingerprint,
  request_contract_version: input.request_contract_version,
  operation_count: input.operation_count,
  operation_errors: input.operation_errors,
  execution_mode: input.execution_mode,
  wait_until: input.wait_until,
  side_effect_kinds: input.side_effect_kinds,
});

const buildBatchExecutionResultStagingCanonicalPayload = (
  input: BatchExecutionResultStagingFingerprintInput,
): string => {
  return canonicalizeOrThrow(
    buildBatchExecutionResultStagingFingerprintPayload(input),
    'Invalid batch execution result staging payload',
  );
};

export const buildBatchExecutionResultStagingId = (deliveryId: string): string => {
  assertBoundedNonEmptyString(deliveryId, 'delivery_id', BATCH_EXECUTION_RESULT_STAGING_DELIVERY_ID_MAX_LENGTH);
  return hashSHA256(`${BATCH_EXECUTION_RESULT_STAGING_ID_SCOPE}${deliveryId}`);
};

export const buildBatchExecutionResultStagingLockId = (deliveryId: string): string => {
  assertBoundedNonEmptyString(deliveryId, 'delivery_id', BATCH_EXECUTION_RESULT_STAGING_DELIVERY_ID_MAX_LENGTH);
  return `${BATCH_EXECUTION_RESULT_STAGING_LOCK_PREFIX}${deliveryId}`;
};

export const buildBatchExecutionResultStagingFingerprint = (
  input: BatchExecutionResultStagingFingerprintInput,
): string => {
  return hashSHA256(buildBatchExecutionResultStagingCanonicalPayload(input));
};

export const buildBatchExecutionResultStagingDraft = (
  input: BuildBatchExecutionResultStagingInput,
): BatchExecutionResultStagingDraft => {
  const identity = {
    receipt_id: input.receipt.internal_id,
    delivery_id: input.receipt.delivery_id,
    submission_id: input.receipt.submission_id,
    request_fingerprint: input.receipt.request_fingerprint,
    request_contract_version: input.receipt.request_contract_version,
  };
  assertStagingIdentityInput(identity);
  assertResultInput(input.result);
  assertNonNegativeInteger(input.result.operationCount, 'operation_count');
  const operationErrors = normalizeOperationErrors(input.result.operationErrors, input.result.operationCount);
  assertExecutionMode(input.result.executionMode);
  assertWaitUntil(input.result.waitUntil);
  const sideEffectKinds = normalizeSideEffectKinds(input.result.sideEffectKinds);
  const fingerprintInput = {
    ...identity,
    result_version: BATCH_EXECUTION_RESULT_STAGING_VERSION,
    operation_count: input.result.operationCount,
    operation_errors: operationErrors,
    execution_mode: input.result.executionMode,
    wait_until: input.result.waitUntil,
    side_effect_kinds: sideEffectKinds,
  };
  const canonicalPayload = buildBatchExecutionResultStagingCanonicalPayload(fingerprintInput);
  const serializedBytes = Buffer.byteLength(canonicalPayload);
  if (serializedBytes > BATCH_EXECUTION_RESULT_STAGING_MAX_SERIALIZED_BYTES) {
    throw batchExecutionResultStagingConflict('Batch execution result staging serialized byte cap exceeded', {
      serialized_bytes: serializedBytes,
      max_serialized_bytes: BATCH_EXECUTION_RESULT_STAGING_MAX_SERIALIZED_BYTES,
    });
  }
  return {
    staging_id: buildBatchExecutionResultStagingId(identity.delivery_id),
    ...fingerprintInput,
    serialized_bytes: serializedBytes,
    staging_fingerprint: hashSHA256(canonicalPayload),
  };
};

export const assertBatchExecutionResultStagingDraft = (
  value: unknown,
): BatchExecutionResultStagingDraft => {
  if (!isRecord(value)) {
    throw batchExecutionResultStagingConflict('Batch execution result staging payload is malformed');
  }
  const unexpectedFields = Object.keys(value).filter((field) => !STAGING_DRAFT_FIELDS.has(field));
  if (unexpectedFields.length > 0) {
    throw batchExecutionResultStagingConflict('Batch execution result staging payload is malformed', {
      unexpected_fields: unexpectedFields,
    });
  }
  const staging = value as unknown as BatchExecutionResultStagingDraft;
  assertStagingIdentityInput(staging);
  if (staging.staging_id !== buildBatchExecutionResultStagingId(staging.delivery_id)) {
    throw batchExecutionResultStagingConflict('Batch execution result staging id does not match its delivery identity', {
      staging_id: staging.staging_id,
      delivery_id: staging.delivery_id,
    });
  }
  if (staging.result_version !== BATCH_EXECUTION_RESULT_STAGING_VERSION) {
    throw batchExecutionResultStagingConflict('Batch execution result staging version is invalid', {
      result_version: staging.result_version,
    });
  }
  assertNonNegativeInteger(staging.operation_count, 'operation_count');
  const operationErrors = normalizeOperationErrors(staging.operation_errors, staging.operation_count);
  assertExecutionMode(staging.execution_mode);
  assertWaitUntil(staging.wait_until);
  const sideEffectKinds = normalizeSideEffectKinds(staging.side_effect_kinds);
  assertNonNegativeInteger(staging.serialized_bytes, 'serialized_bytes');
  assertSha256Hex(staging.staging_fingerprint, 'staging_fingerprint');
  const expectedFingerprintInput = {
    receipt_id: staging.receipt_id,
    delivery_id: staging.delivery_id,
    submission_id: staging.submission_id,
    request_fingerprint: staging.request_fingerprint,
    request_contract_version: staging.request_contract_version,
    result_version: staging.result_version,
    operation_count: staging.operation_count,
    operation_errors: operationErrors,
    execution_mode: staging.execution_mode,
    wait_until: staging.wait_until,
    side_effect_kinds: sideEffectKinds,
  };
  const canonicalPayload = buildBatchExecutionResultStagingCanonicalPayload(expectedFingerprintInput);
  const expectedSerializedBytes = Buffer.byteLength(canonicalPayload);
  if (staging.serialized_bytes !== expectedSerializedBytes) {
    throw batchExecutionResultStagingConflict('Batch execution result staging serialized byte count changed after persistence', {
      serialized_bytes: staging.serialized_bytes,
      expected_serialized_bytes: expectedSerializedBytes,
    });
  }
  if (staging.serialized_bytes > BATCH_EXECUTION_RESULT_STAGING_MAX_SERIALIZED_BYTES) {
    throw batchExecutionResultStagingConflict('Batch execution result staging serialized byte cap exceeded', {
      serialized_bytes: staging.serialized_bytes,
      max_serialized_bytes: BATCH_EXECUTION_RESULT_STAGING_MAX_SERIALIZED_BYTES,
    });
  }
  const expectedFingerprint = hashSHA256(canonicalPayload);
  if (staging.staging_fingerprint !== expectedFingerprint) {
    throw batchExecutionResultStagingConflict('Batch execution result staging fingerprint changed after persistence', {
      staging_fingerprint: staging.staging_fingerprint,
      expected_staging_fingerprint: expectedFingerprint,
    });
  }
  return {
    ...staging,
    operation_errors: operationErrors,
    side_effect_kinds: sideEffectKinds,
  };
};

const assertBatchExecutionResultStagingRecord = (
  value: unknown,
): BatchExecutionResultStaging => {
  if (!isRecord(value)) {
    throw batchExecutionResultStagingConflict('Batch execution result staging payload is malformed');
  }
  const unexpectedFields = Object.keys(value).filter((field) => !STAGING_RECORD_FIELDS.has(field));
  if (unexpectedFields.length > 0) {
    throw batchExecutionResultStagingConflict('Batch execution result staging payload is malformed', {
      unexpected_fields: unexpectedFields,
    });
  }
  const staging = value as unknown as BatchExecutionResultStaging;
  if (
    !isNonEmptyString(staging.id)
    || !isNonEmptyString(staging.internal_id)
    || !isNonEmptyString(staging.standard_id)
    || staging.entity_type !== ENTITY_TYPE_BATCH_EXECUTION_RESULT_STAGING
    || staging.base_type !== BASE_TYPE_ENTITY
    || !Array.isArray(staging.parent_types)
    || !isNonEmptyString(staging.staged_at)
    || !isNonEmptyString(staging.created_at)
    || !isNonEmptyString(staging.updated_at)
  ) {
    throw batchExecutionResultStagingConflict('Batch execution result staging payload is malformed');
  }
  if (
    staging.id !== staging.staging_id
    || staging.internal_id !== staging.staging_id
    || staging.standard_id !== staging.staging_id
    || (staging._id !== undefined && staging._id !== staging.staging_id)
  ) {
    throw batchExecutionResultStagingConflict('Batch execution result staging record identity is malformed', {
      staging_id: staging.staging_id,
    });
  }
  assertIsoTimestamp(staging.staged_at, 'staged_at');
  assertIsoTimestamp(staging.created_at, 'created_at');
  assertIsoTimestamp(staging.updated_at, 'updated_at');
  const normalizedDraft = assertBatchExecutionResultStagingDraft({
    staging_id: staging.staging_id,
    receipt_id: staging.receipt_id,
    delivery_id: staging.delivery_id,
    submission_id: staging.submission_id,
    request_fingerprint: staging.request_fingerprint,
    request_contract_version: staging.request_contract_version,
    result_version: staging.result_version,
    operation_count: staging.operation_count,
    operation_errors: staging.operation_errors,
    execution_mode: staging.execution_mode,
    wait_until: staging.wait_until,
    side_effect_kinds: staging.side_effect_kinds,
    serialized_bytes: staging.serialized_bytes,
    staging_fingerprint: staging.staging_fingerprint,
  });
  return {
    ...staging,
    operation_errors: normalizedDraft.operation_errors,
    side_effect_kinds: normalizedDraft.side_effect_kinds,
  };
};

export const readBatchExecutionResultStagingPayload = (
  staging: BatchExecutionResultStaging,
): BatchExecutionResultStagingPayload => {
  const validatedStaging = assertBatchExecutionResultStagingRecord(staging);
  return {
    operationCount: validatedStaging.operation_count,
    operationErrors: validatedStaging.operation_errors.map((operationError) => ({ ...operationError })),
    executionMode: validatedStaging.execution_mode,
    waitUntil: validatedStaging.wait_until,
    sideEffectKinds: [...validatedStaging.side_effect_kinds],
  };
};

export const loadBatchExecutionResultStaging = async (
  context: AuthContext,
  deliveryId: string,
): Promise<BatchExecutionResultStaging | null> => {
  const stagingId = buildBatchExecutionResultStagingId(deliveryId);
  const staging = await elLoadById(context, SYSTEM_USER, stagingId, {
    type: ENTITY_TYPE_BATCH_EXECUTION_RESULT_STAGING,
    indices: READ_INDEX_INTERNAL_OBJECTS,
  });
  return staging ? assertBatchExecutionResultStagingRecord(staging) : null;
};

export const assertBatchExecutionResultStagingReservation = (
  staging: BatchExecutionResultStaging,
  input: BatchExecutionResultStagingDraft,
) => {
  const normalizedInput = assertBatchExecutionResultStagingDraft(input);
  const conflictingFields = Object.entries({
    staging_id: staging.staging_id !== normalizedInput.staging_id,
    receipt_id: staging.receipt_id !== normalizedInput.receipt_id,
    delivery_id: staging.delivery_id !== normalizedInput.delivery_id,
    submission_id: staging.submission_id !== normalizedInput.submission_id,
    request_fingerprint: staging.request_fingerprint !== normalizedInput.request_fingerprint,
    request_contract_version: staging.request_contract_version !== normalizedInput.request_contract_version,
    result_version: staging.result_version !== normalizedInput.result_version,
    operation_count: staging.operation_count !== normalizedInput.operation_count,
    operation_errors: canonicalizeOrThrow(staging.operation_errors, 'Invalid batch execution result staging operation errors')
      !== canonicalizeOrThrow(normalizedInput.operation_errors, 'Invalid batch execution result staging operation errors'),
    execution_mode: staging.execution_mode !== normalizedInput.execution_mode,
    wait_until: staging.wait_until !== normalizedInput.wait_until,
    side_effect_kinds: canonicalizeOrThrow(staging.side_effect_kinds, 'Invalid batch execution result staging side effect kinds')
      !== canonicalizeOrThrow(normalizedInput.side_effect_kinds, 'Invalid batch execution result staging side effect kinds'),
    serialized_bytes: staging.serialized_bytes !== normalizedInput.serialized_bytes,
    staging_fingerprint: staging.staging_fingerprint !== normalizedInput.staging_fingerprint,
  }).filter(([, conflict]) => conflict).map(([field]) => field);
  if (conflictingFields.length > 0) {
    throw batchExecutionResultStagingConflict('Batch execution result staging is already associated with different immutable data', {
      staging_id: staging.internal_id,
      delivery_id: staging.delivery_id,
      conflicting_fields: conflictingFields,
    });
  }
};

export const reserveBatchExecutionResultStaging = async (
  context: AuthContext,
  input: BatchExecutionResultStagingDraft,
): Promise<BatchExecutionResultStaging> => {
  const normalizedInput = assertBatchExecutionResultStagingDraft(input);
  const lock = await lockResources([buildBatchExecutionResultStagingLockId(normalizedInput.delivery_id)]);
  try {
    const existingStaging = await loadBatchExecutionResultStaging(context, normalizedInput.delivery_id);
    if (existingStaging) {
      assertBatchExecutionResultStagingReservation(existingStaging, normalizedInput);
      return existingStaging;
    }
    const stagedAt = now();
    const staging: BatchExecutionResultStaging = {
      id: normalizedInput.staging_id,
      internal_id: normalizedInput.staging_id,
      standard_id: normalizedInput.staging_id,
      entity_type: ENTITY_TYPE_BATCH_EXECUTION_RESULT_STAGING,
      base_type: BASE_TYPE_ENTITY,
      parent_types: getParentTypes(ENTITY_TYPE_BATCH_EXECUTION_RESULT_STAGING),
      ...normalizedInput,
      staged_at: stagedAt,
      created_at: stagedAt,
      updated_at: stagedAt,
    };
    await elIndex(INDEX_INTERNAL_OBJECTS, staging, { context });
    return staging;
  } finally {
    await lock.unlock();
  }
};
