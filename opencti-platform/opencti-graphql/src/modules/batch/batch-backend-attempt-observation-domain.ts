import jsonCanonicalize from 'canonicalize';
import { logApp, PLATFORM_INSTANCE_ID } from '../../config/conf';
import { FunctionalError } from '../../config/errors';
import { redisDeleteBatchBackendAttemptObservation, redisReadBatchBackendAttemptObservation } from '../../database/redis';
import { redisRefreshBatchBackendAttemptObservation, redisWriteBatchBackendAttemptObservation } from '../../database/redis';
import { RedisBatchBackendAttemptObservationDeleteResult, RedisBatchBackendAttemptObservationWriteResult } from '../../database/redis';
import { now } from '../../utils/format';
import { hashSHA256 } from '../../utils/hash';
import { BatchAdmissionErrorCode, type BatchBackendAttemptObservation, type BatchExecutionReceipt, BatchExecutionReceiptState } from './batch-types';

const BATCH_BACKEND_ATTEMPT_OBSERVATION_ID_SCOPE = 'backend-attempt-observation:';
const BATCH_BACKEND_ATTEMPT_OBSERVATION_KEY_PREFIX = 'batch_backend_attempt_observation:';
const BATCH_BACKEND_ATTEMPT_OBSERVATION_VERSION = 1;
const BATCH_BACKEND_ATTEMPT_OBSERVATION_FIELDS = new Set([
  'observation_id',
  'receipt_id',
  'delivery_id',
  'submission_id',
  'request_fingerprint',
  'request_contract_version',
  'receipt_started_at',
  'backend_node_id',
  'observed_at',
  'expires_at',
  'observation_version',
]);
export const BATCH_BACKEND_ATTEMPT_OBSERVATION_TTL_SECONDS = 120;
export const BATCH_BACKEND_ATTEMPT_OBSERVATION_REFRESH_INTERVAL_MS = 30000;

export interface BatchBackendAttemptObservationSnapshot {
  observation: BatchBackendAttemptObservation | null;
  ttlSeconds: number;
}

export interface BatchBackendAttemptObservationRefreshLoop {
  stop: () => Promise<void>;
}

export interface BatchBackendAttemptObservationRefreshOptions {
  backendNodeId?: string;
  refreshIntervalMs?: number;
  ttlSeconds?: number;
}

const canonicalizeOrThrow = (value: unknown, message: string): string => {
  const canonicalValue = jsonCanonicalize(value);
  if (typeof canonicalValue !== 'string') {
    throw FunctionalError(message);
  }
  return canonicalValue;
};

const backendAttemptObservationConflict = (message: string, data: Record<string, unknown> = {}) => {
  return FunctionalError(message, {
    batch_error_code: BatchAdmissionErrorCode.ExecutionReconciliationConflict,
    ...data,
  });
};

const assertPositiveInteger = (value: number, field: string) => {
  if (!Number.isInteger(value) || value <= 0) {
    throw backendAttemptObservationConflict('Batch backend attempt observation timing must be a positive integer', {
      field,
      value,
    });
  }
};

const assertObservationTiming = (ttlSeconds: number, refreshIntervalMs?: number) => {
  assertPositiveInteger(ttlSeconds, 'ttl_seconds');
  if (refreshIntervalMs === undefined) {
    return;
  }
  assertPositiveInteger(refreshIntervalMs, 'refresh_interval_ms');
  if (refreshIntervalMs > (ttlSeconds * 1000) / 3) {
    throw backendAttemptObservationConflict('Batch backend attempt observation refresh interval must not exceed one third of its TTL', {
      refresh_interval_ms: refreshIntervalMs,
      ttl_seconds: ttlSeconds,
    });
  }
};

const assertStartedReceipt = (receipt: BatchExecutionReceipt): string => {
  if (receipt.state !== BatchExecutionReceiptState.Started || !receipt.started_at) {
    throw backendAttemptObservationConflict('Batch backend attempt observation requires a durably STARTED receipt', {
      receipt_id: receipt.internal_id,
      receipt_state: receipt.state,
    });
  }
  return receipt.started_at;
};

const assertBackendNodeId = (backendNodeId: string) => {
  if (backendNodeId.length === 0) {
    throw backendAttemptObservationConflict('Batch backend attempt observation requires a backend node identifier');
  }
};

const isNonEmptyString = (value: unknown): value is string => typeof value === 'string' && value.length > 0;

const assertBatchBackendAttemptObservationShape = (
  value: unknown,
): BatchBackendAttemptObservation => {
  if (!value || typeof value !== 'object') {
    throw backendAttemptObservationConflict('Batch backend attempt observation payload is malformed');
  }
  const observation = value as Partial<BatchBackendAttemptObservation>;
  const unexpectedFields = Object.keys(observation).filter((field) => !BATCH_BACKEND_ATTEMPT_OBSERVATION_FIELDS.has(field));
  if (
    unexpectedFields.length > 0
    || !isNonEmptyString(observation.observation_id)
    || !isNonEmptyString(observation.receipt_id)
    || !isNonEmptyString(observation.delivery_id)
    || !isNonEmptyString(observation.submission_id)
    || !isNonEmptyString(observation.request_fingerprint)
    || !Number.isInteger(observation.request_contract_version)
    || !isNonEmptyString(observation.receipt_started_at)
    || !isNonEmptyString(observation.backend_node_id)
    || !isNonEmptyString(observation.observed_at)
    || !isNonEmptyString(observation.expires_at)
    || observation.observation_version !== BATCH_BACKEND_ATTEMPT_OBSERVATION_VERSION
  ) {
    throw backendAttemptObservationConflict('Batch backend attempt observation payload is malformed', {
      unexpected_fields: unexpectedFields,
    });
  }
  const observedAt = Date.parse(observation.observed_at);
  const expiresAt = Date.parse(observation.expires_at);
  if (Number.isNaN(observedAt) || Number.isNaN(expiresAt) || expiresAt <= observedAt) {
    throw backendAttemptObservationConflict('Batch backend attempt observation payload has invalid timestamps', {
      observation_id: observation.observation_id,
    });
  }
  return observation as BatchBackendAttemptObservation;
};

export const buildBatchBackendAttemptObservationId = (deliveryId: string, receiptId: string): string => {
  return hashSHA256(`${BATCH_BACKEND_ATTEMPT_OBSERVATION_ID_SCOPE}${deliveryId}:${receiptId}`);
};

export const buildBatchBackendAttemptObservationRedisKey = (deliveryId: string): string => {
  return `${BATCH_BACKEND_ATTEMPT_OBSERVATION_KEY_PREFIX}${deliveryId}`;
};

export const buildBatchBackendAttemptObservationFingerprint = (
  observation: BatchBackendAttemptObservation,
): string => {
  return hashSHA256(canonicalizeOrThrow({
    observation_id: observation.observation_id,
    receipt_id: observation.receipt_id,
    delivery_id: observation.delivery_id,
    submission_id: observation.submission_id,
    request_fingerprint: observation.request_fingerprint,
    request_contract_version: observation.request_contract_version,
    receipt_started_at: observation.receipt_started_at,
    backend_node_id: observation.backend_node_id,
    observation_version: observation.observation_version,
  }, 'Invalid batch backend attempt observation identity'));
};

export const buildBatchBackendAttemptObservation = (
  receipt: BatchExecutionReceipt,
  options: Pick<BatchBackendAttemptObservationRefreshOptions, 'backendNodeId' | 'ttlSeconds'> = {},
): BatchBackendAttemptObservation => {
  const receiptStartedAt = assertStartedReceipt(receipt);
  const backendNodeId = options.backendNodeId ?? PLATFORM_INSTANCE_ID;
  const ttlSeconds = options.ttlSeconds ?? BATCH_BACKEND_ATTEMPT_OBSERVATION_TTL_SECONDS;
  assertBackendNodeId(backendNodeId);
  assertObservationTiming(ttlSeconds);
  const observedAt = now();
  return {
    observation_id: buildBatchBackendAttemptObservationId(receipt.delivery_id, receipt.internal_id),
    receipt_id: receipt.internal_id,
    delivery_id: receipt.delivery_id,
    submission_id: receipt.submission_id,
    request_fingerprint: receipt.request_fingerprint,
    request_contract_version: receipt.request_contract_version,
    receipt_started_at: receiptStartedAt,
    backend_node_id: backendNodeId,
    observed_at: observedAt,
    expires_at: new Date(Date.parse(observedAt) + (ttlSeconds * 1000)).toISOString(),
    observation_version: BATCH_BACKEND_ATTEMPT_OBSERVATION_VERSION,
  };
};

export const assertBatchBackendAttemptObservationIdentity = (
  observation: BatchBackendAttemptObservation,
  receipt: BatchExecutionReceipt,
  expectedBackendNodeId?: string,
): void => {
  const conflictingFields = Object.entries({
    observation_id: observation.observation_id !== buildBatchBackendAttemptObservationId(receipt.delivery_id, receipt.internal_id),
    receipt_id: observation.receipt_id !== receipt.internal_id,
    delivery_id: observation.delivery_id !== receipt.delivery_id,
    submission_id: observation.submission_id !== receipt.submission_id,
    request_fingerprint: observation.request_fingerprint !== receipt.request_fingerprint,
    request_contract_version: observation.request_contract_version !== receipt.request_contract_version,
    receipt_started_at: observation.receipt_started_at !== receipt.started_at,
    backend_node_id: expectedBackendNodeId !== undefined && observation.backend_node_id !== expectedBackendNodeId,
    observation_version: observation.observation_version !== BATCH_BACKEND_ATTEMPT_OBSERVATION_VERSION,
  }).filter(([, conflict]) => conflict).map(([field]) => field);
  if (conflictingFields.length > 0) {
    throw backendAttemptObservationConflict('Batch backend attempt observation is already associated with different immutable attempt data', {
      observation_id: observation.observation_id,
      delivery_id: receipt.delivery_id,
      conflicting_fields: conflictingFields,
    });
  }
};

export const readBatchBackendAttemptObservation = async (
  deliveryId: string,
): Promise<BatchBackendAttemptObservationSnapshot> => {
  const snapshot = await redisReadBatchBackendAttemptObservation(buildBatchBackendAttemptObservationRedisKey(deliveryId));
  if (!snapshot.rawValue) {
    return {
      observation: null,
      ttlSeconds: snapshot.ttlSeconds,
    };
  }
  let parsed: unknown;
  try {
    parsed = JSON.parse(snapshot.rawValue);
  } catch (cause) {
    throw backendAttemptObservationConflict('Batch backend attempt observation payload is malformed', {
      delivery_id: deliveryId,
      cause,
    });
  }
  return {
    observation: assertBatchBackendAttemptObservationShape(parsed),
    ttlSeconds: snapshot.ttlSeconds,
  };
};

export const readFreshBatchBackendAttemptObservation = async (
  receipt: BatchExecutionReceipt,
): Promise<BatchBackendAttemptObservation> => {
  const snapshot = await readBatchBackendAttemptObservation(receipt.delivery_id);
  if (!snapshot.observation) {
    throw backendAttemptObservationConflict('Batch backend attempt observation is missing', {
      receipt_id: receipt.internal_id,
      delivery_id: receipt.delivery_id,
    });
  }
  assertBatchBackendAttemptObservationIdentity(snapshot.observation, receipt);
  if (snapshot.ttlSeconds <= 0 || Date.parse(snapshot.observation.expires_at) <= Date.now()) {
    throw backendAttemptObservationConflict('Batch backend attempt observation has expired', {
      observation_id: snapshot.observation.observation_id,
      receipt_id: receipt.internal_id,
      delivery_id: receipt.delivery_id,
      ttl_seconds: snapshot.ttlSeconds,
    });
  }
  return snapshot.observation;
};

const assertRedisObservationWriteResult = (
  result: RedisBatchBackendAttemptObservationWriteResult,
  observation: BatchBackendAttemptObservation,
) => {
  if (result === RedisBatchBackendAttemptObservationWriteResult.Malformed) {
    throw backendAttemptObservationConflict('Batch backend attempt observation storage contains malformed data', {
      observation_id: observation.observation_id,
      delivery_id: observation.delivery_id,
    });
  }
  if (result === RedisBatchBackendAttemptObservationWriteResult.Conflict) {
    throw backendAttemptObservationConflict('Batch backend attempt observation storage contains conflicting immutable attempt data', {
      observation_id: observation.observation_id,
      delivery_id: observation.delivery_id,
    });
  }
  if (
    result !== RedisBatchBackendAttemptObservationWriteResult.Created
    && result !== RedisBatchBackendAttemptObservationWriteResult.Refreshed
  ) {
    throw backendAttemptObservationConflict('Batch backend attempt observation storage returned an invalid write result', {
      observation_id: observation.observation_id,
      delivery_id: observation.delivery_id,
      result,
    });
  }
};

const persistBatchBackendAttemptObservation = async (
  receipt: BatchExecutionReceipt,
  writer: typeof redisWriteBatchBackendAttemptObservation,
  options: Pick<BatchBackendAttemptObservationRefreshOptions, 'backendNodeId' | 'ttlSeconds'> = {},
): Promise<BatchBackendAttemptObservation> => {
  const observation = buildBatchBackendAttemptObservation(receipt, options);
  const existing = await readBatchBackendAttemptObservation(receipt.delivery_id);
  if (existing.observation) {
    assertBatchBackendAttemptObservationIdentity(existing.observation, receipt, observation.backend_node_id);
  }
  const ttlSeconds = options.ttlSeconds ?? BATCH_BACKEND_ATTEMPT_OBSERVATION_TTL_SECONDS;
  const result = await writer(buildBatchBackendAttemptObservationRedisKey(receipt.delivery_id), observation, ttlSeconds);
  assertRedisObservationWriteResult(result, observation);
  return observation;
};

export const writeBatchBackendAttemptObservation = async (
  receipt: BatchExecutionReceipt,
  options: Pick<BatchBackendAttemptObservationRefreshOptions, 'backendNodeId' | 'ttlSeconds'> = {},
): Promise<BatchBackendAttemptObservation> => {
  return persistBatchBackendAttemptObservation(receipt, redisWriteBatchBackendAttemptObservation, options);
};

export const refreshBatchBackendAttemptObservation = async (
  receipt: BatchExecutionReceipt,
  options: Pick<BatchBackendAttemptObservationRefreshOptions, 'backendNodeId' | 'ttlSeconds'> = {},
): Promise<BatchBackendAttemptObservation> => {
  return persistBatchBackendAttemptObservation(receipt, redisRefreshBatchBackendAttemptObservation, options);
};

export const deleteBatchBackendAttemptObservationBestEffort = async (
  observation: BatchBackendAttemptObservation,
): Promise<boolean> => {
  try {
    const result = await redisDeleteBatchBackendAttemptObservation(
      buildBatchBackendAttemptObservationRedisKey(observation.delivery_id),
      observation,
    );
    if (result === RedisBatchBackendAttemptObservationDeleteResult.Malformed) {
      logApp.warn('[BATCH] Unable to delete malformed backend attempt observation key', {
        observation_id: observation.observation_id,
        delivery_id: observation.delivery_id,
      });
      return false;
    }
    return result === RedisBatchBackendAttemptObservationDeleteResult.Deleted;
  } catch (error) {
    logApp.warn('[BATCH] Unable to delete backend attempt observation key', {
      cause: error,
      observation_id: observation.observation_id,
      delivery_id: observation.delivery_id,
    });
    return false;
  }
};

export const startBatchBackendAttemptObservationRefreshLoop = async (
  receipt: BatchExecutionReceipt,
  options: BatchBackendAttemptObservationRefreshOptions = {},
): Promise<BatchBackendAttemptObservationRefreshLoop> => {
  assertStartedReceipt(receipt);
  const backendNodeId = options.backendNodeId ?? PLATFORM_INSTANCE_ID;
  const ttlSeconds = options.ttlSeconds ?? BATCH_BACKEND_ATTEMPT_OBSERVATION_TTL_SECONDS;
  const refreshIntervalMs = options.refreshIntervalMs ?? BATCH_BACKEND_ATTEMPT_OBSERVATION_REFRESH_INTERVAL_MS;
  assertBackendNodeId(backendNodeId);
  assertObservationTiming(ttlSeconds, refreshIntervalMs);

  let stopped = false;
  let timer: ReturnType<typeof setTimeout> | null = null;
  let inFlightRefresh: Promise<void> | null = null;
  let currentObservation: BatchBackendAttemptObservation | null = null;

  const persistWithoutChangingExecutionOutcome = async (
    phase: 'initial_write' | 'refresh',
    persist: () => Promise<BatchBackendAttemptObservation>,
  ) => {
    try {
      currentObservation = await persist();
    } catch (error) {
      logApp.warn('[BATCH] Unable to persist backend attempt observation; direct execution continues without fresh liveness evidence', {
        cause: error,
        phase,
        receipt_id: receipt.internal_id,
        delivery_id: receipt.delivery_id,
      });
    }
  };

  const scheduleNextRefresh = () => {
    if (stopped) {
      return;
    }
    timer = setTimeout(() => {
      timer = null;
      inFlightRefresh = persistWithoutChangingExecutionOutcome('refresh', () => refreshBatchBackendAttemptObservation(receipt, {
        backendNodeId,
        ttlSeconds,
      })).finally(() => {
        inFlightRefresh = null;
        scheduleNextRefresh();
      });
    }, refreshIntervalMs);
  };

  await persistWithoutChangingExecutionOutcome('initial_write', () => writeBatchBackendAttemptObservation(receipt, {
    backendNodeId,
    ttlSeconds,
  }));
  scheduleNextRefresh();

  return {
    stop: async () => {
      if (stopped) {
        return;
      }
      stopped = true;
      if (timer) {
        clearTimeout(timer);
        timer = null;
      }
      await inFlightRefresh;
      if (currentObservation) {
        await deleteBatchBackendAttemptObservationBestEffort(currentObservation);
      }
    },
  };
};
