import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';
import {
  redisDeleteBatchBackendAttemptObservation,
  redisReadBatchBackendAttemptObservation,
  redisRefreshBatchBackendAttemptObservation,
  redisWriteBatchBackendAttemptObservation,
  RedisBatchBackendAttemptObservationDeleteResult,
  RedisBatchBackendAttemptObservationWriteResult,
} from '../../../../src/database/redis';
import {
  buildBatchBackendAttemptObservationId,
  buildBatchBackendAttemptObservationRedisKey,
  deleteBatchBackendAttemptObservationBestEffort,
  readFreshBatchBackendAttemptObservation,
  refreshBatchBackendAttemptObservation,
  startBatchBackendAttemptObservationRefreshLoop,
  writeBatchBackendAttemptObservation,
} from '../../../../src/modules/batch/batch-backend-attempt-observation-domain';
import {
  BatchExecutionMode,
  BatchExecutionReceiptState,
  BatchWaitUntil,
  type BatchBackendAttemptObservation,
  type BatchExecutionReceipt,
} from '../../../../src/modules/batch/batch-types';

vi.mock('../../../../src/database/redis', async (importOriginal) => {
  const actual = await importOriginal<typeof import('../../../../src/database/redis')>();
  return {
    ...actual,
    redisDeleteBatchBackendAttemptObservation: vi.fn(),
    redisReadBatchBackendAttemptObservation: vi.fn(),
    redisRefreshBatchBackendAttemptObservation: vi.fn(),
    redisWriteBatchBackendAttemptObservation: vi.fn(),
  };
});

const startedReceipt = {
  id: 'batch-execution-receipt--observation-test',
  internal_id: 'batch-execution-receipt--observation-test',
  standard_id: 'batch-execution-receipt--observation-test',
  entity_type: 'BatchExecutionReceipt',
  base_type: 'ENTITY',
  parent_types: ['Basic-Object', 'Internal-Object'],
  delivery_id: 'batch-delivery--observation-test',
  submission_id: 'batch-submission--observation-test',
  delivery_payload_fingerprint: 'payload-fingerprint-observation-test',
  request_contract_version: 1,
  request_fingerprint: 'request-fingerprint-observation-test',
  batch_plan_fingerprint: 'batch-plan-fingerprint-observation-test',
  operation_manifest_fingerprint: 'operation-manifest-fingerprint-observation-test',
  operation_count: 1,
  execution_mode: BatchExecutionMode.Bulk,
  wait_until: BatchWaitUntil.Materialized,
  state: BatchExecutionReceiptState.Started,
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
  prepared_at: '2026-08-08T00:00:00.000Z',
  started_at: '2026-08-08T00:00:01.000Z',
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
  created_at: '2026-08-08T00:00:00.000Z',
  updated_at: '2026-08-08T00:00:01.000Z',
  last_error: null,
} as BatchExecutionReceipt;

const immutableObservationFields = [
  'observation_id',
  'receipt_id',
  'delivery_id',
  'submission_id',
  'request_fingerprint',
  'request_contract_version',
  'receipt_started_at',
  'backend_node_id',
  'observation_version',
] as const;

const sameObservationIdentity = (
  left: BatchBackendAttemptObservation,
  right: BatchBackendAttemptObservation,
) => immutableObservationFields.every((field) => left[field] === right[field]);

describe('batch backend attempt observation domain', () => {
  let storedRawValue: string | null;
  let storedExpiresAtMs: number | null;

  const readStoredSnapshot = () => {
    if (!storedRawValue || storedExpiresAtMs === null) {
      return { rawValue: null, ttlSeconds: -2 };
    }
    const ttlSeconds = Math.ceil((storedExpiresAtMs - Date.now()) / 1000);
    if (ttlSeconds <= 0) {
      storedRawValue = null;
      storedExpiresAtMs = null;
      return { rawValue: null, ttlSeconds: -2 };
    }
    return { rawValue: storedRawValue, ttlSeconds };
  };

  const upsertObservation = async (
    _key: string,
    observation: BatchBackendAttemptObservation,
    ttlSeconds: number,
  ) => {
    const current = readStoredSnapshot();
    if (current.rawValue) {
      const currentObservation = JSON.parse(current.rawValue) as BatchBackendAttemptObservation;
      if (!sameObservationIdentity(currentObservation, observation)) {
        return RedisBatchBackendAttemptObservationWriteResult.Conflict;
      }
    }
    storedRawValue = JSON.stringify(observation);
    storedExpiresAtMs = Date.now() + (ttlSeconds * 1000);
    return current.rawValue
      ? RedisBatchBackendAttemptObservationWriteResult.Refreshed
      : RedisBatchBackendAttemptObservationWriteResult.Created;
  };

  beforeEach(() => {
    vi.clearAllMocks();
    vi.useFakeTimers();
    vi.setSystemTime(new Date('2026-08-08T00:00:02.000Z'));
    storedRawValue = null;
    storedExpiresAtMs = null;
    vi.mocked(redisReadBatchBackendAttemptObservation).mockImplementation(async () => readStoredSnapshot());
    vi.mocked(redisWriteBatchBackendAttemptObservation).mockImplementation(upsertObservation);
    vi.mocked(redisRefreshBatchBackendAttemptObservation).mockImplementation(upsertObservation);
    vi.mocked(redisDeleteBatchBackendAttemptObservation).mockImplementation(async (_key, observation) => {
      const current = readStoredSnapshot();
      if (!current.rawValue) {
        return RedisBatchBackendAttemptObservationDeleteResult.MissingOrConflict;
      }
      const currentObservation = JSON.parse(current.rawValue) as BatchBackendAttemptObservation;
      if (!sameObservationIdentity(currentObservation, observation)) {
        return RedisBatchBackendAttemptObservationDeleteResult.MissingOrConflict;
      }
      storedRawValue = null;
      storedExpiresAtMs = null;
      return RedisBatchBackendAttemptObservationDeleteResult.Deleted;
    });
  });

  afterEach(() => {
    vi.useRealTimers();
  });

  it('builds deterministic identifiers and rejects writes before STARTED is durable', async () => {
    expect(buildBatchBackendAttemptObservationId(startedReceipt.delivery_id, startedReceipt.internal_id))
      .toBe(buildBatchBackendAttemptObservationId(startedReceipt.delivery_id, startedReceipt.internal_id));
    expect(buildBatchBackendAttemptObservationRedisKey(startedReceipt.delivery_id))
      .toBe(`batch_backend_attempt_observation:${startedReceipt.delivery_id}`);

    await expect(writeBatchBackendAttemptObservation({
      ...startedReceipt,
      state: BatchExecutionReceiptState.Prepared,
      started_at: null,
    }, {
      backendNodeId: 'platform:instance:test-node',
    })).rejects.toThrowError('Batch backend attempt observation requires a durably STARTED receipt');
    expect(redisWriteBatchBackendAttemptObservation).not.toHaveBeenCalled();
  });

  it('writes and refreshes one bounded current observation while extending its TTL', async () => {
    const first = await writeBatchBackendAttemptObservation(startedReceipt, {
      backendNodeId: 'platform:instance:test-node',
      ttlSeconds: 120,
    });
    expect(first).toMatchObject({
      observation_id: buildBatchBackendAttemptObservationId(startedReceipt.delivery_id, startedReceipt.internal_id),
      receipt_id: startedReceipt.internal_id,
      delivery_id: startedReceipt.delivery_id,
      submission_id: startedReceipt.submission_id,
      request_fingerprint: startedReceipt.request_fingerprint,
      request_contract_version: startedReceipt.request_contract_version,
      receipt_started_at: startedReceipt.started_at,
      backend_node_id: 'platform:instance:test-node',
      observed_at: '2026-08-08T00:00:02.000Z',
      expires_at: '2026-08-08T00:02:02.000Z',
      observation_version: 1,
    });

    await vi.advanceTimersByTimeAsync(30000);
    const refreshed = await refreshBatchBackendAttemptObservation(startedReceipt, {
      backendNodeId: 'platform:instance:test-node',
      ttlSeconds: 120,
    });
    expect(refreshed).toMatchObject({
      observation_id: first.observation_id,
      observed_at: '2026-08-08T00:00:32.000Z',
      expires_at: '2026-08-08T00:02:32.000Z',
    });
    expect(await readFreshBatchBackendAttemptObservation(startedReceipt)).toStrictEqual(refreshed);
    expect(redisWriteBatchBackendAttemptObservation).toHaveBeenCalledTimes(1);
    expect(redisRefreshBatchBackendAttemptObservation).toHaveBeenCalledTimes(1);
  });

  it('rejects changed receipt identity and backend node ownership without overwriting the live key', async () => {
    const first = await writeBatchBackendAttemptObservation(startedReceipt, {
      backendNodeId: 'platform:instance:test-node',
    });

    await expect(refreshBatchBackendAttemptObservation({
      ...startedReceipt,
      request_fingerprint: 'different-request-fingerprint',
    }, {
      backendNodeId: 'platform:instance:test-node',
    })).rejects.toThrowError('Batch backend attempt observation is already associated with different immutable attempt data');
    await expect(refreshBatchBackendAttemptObservation(startedReceipt, {
      backendNodeId: 'platform:instance:other-node',
    })).rejects.toThrowError('Batch backend attempt observation is already associated with different immutable attempt data');

    expect(await readFreshBatchBackendAttemptObservation(startedReceipt)).toStrictEqual(first);
  });

  it('fails closed on missing, expired, malformed, and mismatched observation data', async () => {
    await expect(readFreshBatchBackendAttemptObservation(startedReceipt))
      .rejects.toThrowError('Batch backend attempt observation is missing');

    const observation = await writeBatchBackendAttemptObservation(startedReceipt, {
      backendNodeId: 'platform:instance:test-node',
      ttlSeconds: 120,
    });
    storedRawValue = JSON.stringify({
      ...observation,
      observed_at: '2026-08-08T00:00:00.000Z',
      expires_at: '2026-08-08T00:00:01.000Z',
    });
    await expect(readFreshBatchBackendAttemptObservation(startedReceipt))
      .rejects.toThrowError('Batch backend attempt observation has expired');

    storedRawValue = '{';
    await expect(readFreshBatchBackendAttemptObservation(startedReceipt))
      .rejects.toThrowError('Batch backend attempt observation payload is malformed');

    storedRawValue = JSON.stringify({
      ...observation,
      heartbeat_history: [],
    });
    await expect(readFreshBatchBackendAttemptObservation(startedReceipt))
      .rejects.toThrowError('Batch backend attempt observation payload is malformed');

    storedRawValue = JSON.stringify({
      ...observation,
      request_fingerprint: 'different-request-fingerprint',
    });
    await expect(readFreshBatchBackendAttemptObservation(startedReceipt))
      .rejects.toThrowError('Batch backend attempt observation is already associated with different immutable attempt data');
  });

  it('runs a non-overlapping refresh loop and treats heartbeat or cleanup failures as best effort', async () => {
    let releaseRefresh: (() => void) | undefined;
    let activeRefreshes = 0;
    let maxActiveRefreshes = 0;
    vi.mocked(redisRefreshBatchBackendAttemptObservation).mockImplementation(async (...args) => {
      activeRefreshes += 1;
      maxActiveRefreshes = Math.max(maxActiveRefreshes, activeRefreshes);
      await new Promise<void>((resolve) => {
        releaseRefresh = resolve;
      });
      activeRefreshes -= 1;
      return upsertObservation(...args);
    });

    const loop = await startBatchBackendAttemptObservationRefreshLoop(startedReceipt, {
      backendNodeId: 'platform:instance:test-node',
      refreshIntervalMs: 1000,
      ttlSeconds: 3,
    });
    await vi.advanceTimersByTimeAsync(1000);
    await vi.advanceTimersByTimeAsync(5000);
    expect(redisRefreshBatchBackendAttemptObservation).toHaveBeenCalledTimes(1);
    expect(maxActiveRefreshes).toBe(1);

    releaseRefresh?.();
    await Promise.resolve();
    vi.mocked(redisDeleteBatchBackendAttemptObservation).mockRejectedValueOnce(new Error('redis delete unavailable'));
    await expect(loop.stop()).resolves.toBeUndefined();
    expect(redisDeleteBatchBackendAttemptObservation).toHaveBeenCalledTimes(1);
  });

  it('keeps refreshing after a transient heartbeat write failure', async () => {
    vi.mocked(redisRefreshBatchBackendAttemptObservation)
      .mockRejectedValueOnce(new Error('redis refresh unavailable'))
      .mockImplementation(upsertObservation);

    const loop = await startBatchBackendAttemptObservationRefreshLoop(startedReceipt, {
      backendNodeId: 'platform:instance:test-node',
      refreshIntervalMs: 1000,
      ttlSeconds: 3,
    });
    await vi.advanceTimersByTimeAsync(1000);
    expect(redisRefreshBatchBackendAttemptObservation).toHaveBeenCalledTimes(1);

    await vi.advanceTimersByTimeAsync(1000);
    expect(redisRefreshBatchBackendAttemptObservation).toHaveBeenCalledTimes(2);
    await expect(readFreshBatchBackendAttemptObservation(startedReceipt)).resolves.toMatchObject({
      observed_at: '2026-08-08T00:00:04.000Z',
      expires_at: '2026-08-08T00:00:07.000Z',
    });

    await expect(loop.stop()).resolves.toBeUndefined();
  });
});
