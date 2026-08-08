import { beforeEach, describe, expect, it, vi } from 'vitest';
import {
  BatchAdmissionErrorCode,
  BatchExecutionMode,
  BatchExecutionPreference,
  BatchExecutionReason,
  BatchSubmissionState,
  BatchSubmissionWorkOrigin,
  BatchWaitUntil,
} from '../../../src/modules/batch/batch-types';
import {
  buildBatchAdmission,
  buildBatchQueueMessage,
  prepareBundleSubmission,
} from '../../../src/modules/batch/batch-domain';
import {
  advanceBatchSubmissionState,
  buildBatchSubmissionId,
  loadBatchSubmission,
  recordBatchSubmissionError,
  reserveBatchSubmission,
} from '../../../src/modules/batch/batch-submission-domain';
import { sendStixBundle, submitStixBundle } from '../../../src/domain/stix';
import { ADMIN_USER, testContext } from '../../utils/testQuery';
import { pushToWorkerForConnector } from '../../../src/database/rabbitmq';
import { createWork, loadWorkById, updateBatchSubmissionExpectation, updateExpectationsNumber } from '../../../src/domain/work';
import { lockResources } from '../../../src/lock/master-lock';

const { mockStoreLoadById } = vi.hoisted(() => ({
  mockStoreLoadById: vi.fn(),
}));

vi.mock('../../../src/database/middleware-loader', async (importOriginal) => {
  const actual: object = await importOriginal();
  return {
    ...actual,
    storeLoadById: mockStoreLoadById,
  };
});

vi.mock('../../../src/database/rabbitmq', async (importOriginal) => {
  const actual: object = await importOriginal();
  return {
    ...actual,
    pushToWorkerForConnector: vi.fn(),
  };
});

vi.mock('../../../src/domain/work', async (importOriginal) => {
  const actual: object = await importOriginal();
  return {
    ...actual,
    createWork: vi.fn(),
    loadWorkById: vi.fn(),
    updateBatchSubmissionExpectation: vi.fn(),
    updateExpectationsNumber: vi.fn(),
  };
});

vi.mock('../../../src/modules/batch/batch-submission-domain', async (importOriginal) => {
  const actual: object = await importOriginal();
  return {
    ...actual,
    advanceBatchSubmissionState: vi.fn(),
    loadBatchSubmission: vi.fn(),
    recordBatchSubmissionError: vi.fn(),
    reserveBatchSubmission: vi.fn(),
  };
});

vi.mock('../../../src/lock/master-lock', async (importOriginal) => {
  const actual: object = await importOriginal();
  return {
    ...actual,
    lockResources: vi.fn(),
  };
});

const bundle = JSON.stringify({
  type: 'bundle',
  id: 'bundle--11111111-1111-4111-8111-111111111111',
  objects: [
    { type: 'identity', id: 'identity--11111111-1111-4111-8111-111111111111' },
    { type: 'indicator', id: 'indicator--11111111-1111-4111-8111-111111111111', created_by_ref: 'identity--11111111-1111-4111-8111-111111111111' },
    { type: 'indicator', id: 'indicator--22222222-2222-4222-8222-222222222222', created_by_ref: 'identity--11111111-1111-4111-8111-111111111111' },
  ],
});

const reorderedBundle = JSON.stringify({
  objects: [
    { id: 'identity--11111111-1111-4111-8111-111111111111', type: 'identity' },
    { created_by_ref: 'identity--11111111-1111-4111-8111-111111111111', id: 'indicator--11111111-1111-4111-8111-111111111111', type: 'indicator' },
    { created_by_ref: 'identity--11111111-1111-4111-8111-111111111111', id: 'indicator--22222222-2222-4222-8222-222222222222', type: 'indicator' },
  ],
  id: 'bundle--11111111-1111-4111-8111-111111111111',
  type: 'bundle',
});

const conflictingBundle = JSON.stringify({
  type: 'bundle',
  id: 'bundle--11111111-1111-4111-8111-111111111111',
  objects: [
    { type: 'identity', id: 'identity--11111111-1111-4111-8111-111111111111' },
    { type: 'indicator', id: 'indicator--11111111-1111-4111-8111-111111111111', created_by_ref: 'identity--11111111-1111-4111-8111-111111111111' },
    { type: 'indicator', id: 'indicator--33333333-3333-4333-8333-333333333333', created_by_ref: 'identity--11111111-1111-4111-8111-111111111111' },
  ],
});

describe('batch admission contract', () => {
  it('classifies identity plus indicator bundles on the generic bulk path by default', () => {
    const prepared = prepareBundleSubmission(bundle);

    expect(prepared.bundleId).toBe('bundle--11111111-1111-4111-8111-111111111111');
    expect(prepared.objectCount).toBe(3);
    expect(prepared.objectTypes).toEqual(['identity', 'indicator']);
    expect(prepared.executionPreference).toBe(BatchExecutionPreference.Auto);
    expect(prepared.executionMode).toBe(BatchExecutionMode.Bulk);
    expect(prepared.executionReason).toBe(BatchExecutionReason.GenericBulkCompatible);
    expect(prepared.eligibleExecutionModes).toEqual([
      BatchExecutionMode.Bulk,
      BatchExecutionMode.Compatibility,
    ]);
    expect(prepared.waitUntil).toBe(BatchWaitUntil.Materialized);
    expect(prepared.idempotencyKey).toBe(prepared.bundleId);
  });

  it('maps explicit legacy split requests onto the legacy execution mode', () => {
    const prepared = prepareBundleSubmission(bundle, {
      splitBundles: true,
      waitUntil: BatchWaitUntil.Committed,
      idempotencyKey: 'feed-run-2026-08-01',
    });

    expect(prepared.executionPreference).toBe(BatchExecutionPreference.LegacySplit);
    expect(prepared.executionMode).toBe(BatchExecutionMode.LegacySplit);
    expect(prepared.executionReason).toBe(BatchExecutionReason.ExplicitLegacySplit);
    expect(prepared.waitUntil).toBe(BatchWaitUntil.Committed);
    expect(prepared.idempotencyKey).toBe('feed-run-2026-08-01');
  });

  it('fingerprints equivalent normalized payloads consistently while distinguishing conflicting payloads', () => {
    const first = prepareBundleSubmission(bundle, { idempotencyKey: 'feed-run-2026-08-08' });
    const reordered = prepareBundleSubmission(reorderedBundle, { idempotencyKey: 'feed-run-2026-08-08' });
    const conflicting = prepareBundleSubmission(conflictingBundle, { idempotencyKey: 'feed-run-2026-08-08' });

    expect(reordered.payloadFingerprint).toBe(first.payloadFingerprint);
    expect(conflicting.payloadFingerprint).not.toBe(first.payloadFingerprint);
  });

  it('assigns a stable bundle id when a caller omits one', () => {
    const prepared = prepareBundleSubmission(JSON.stringify({
      type: 'bundle',
      objects: [{ type: 'identity', id: 'identity--11111111-1111-4111-8111-111111111111' }],
    }));

    expect(prepared.bundleId).toMatch(/^bundle--[0-9a-f-]+$/);
    expect(JSON.parse(prepared.bundle).id).toBe(prepared.bundleId);
    expect(prepared.idempotencyKey).toBe(prepared.bundleId);
  });

  it('classifies generic non-operational bundles as bulk-compatible', () => {
    const prepared = prepareBundleSubmission(JSON.stringify({
      type: 'bundle',
      id: 'bundle--22222222-2222-4222-8222-222222222222',
      objects: [
        { type: 'identity', id: 'identity--11111111-1111-4111-8111-111111111111' },
        { type: 'malware', id: 'malware--11111111-1111-4111-8111-111111111111' },
      ],
    }));

    expect(prepared.executionMode).toBe(BatchExecutionMode.Bulk);
    expect(prepared.executionReason).toBe(BatchExecutionReason.GenericBulkCompatible);
    expect(prepared.eligibleExecutionModes).toEqual([
      BatchExecutionMode.Bulk,
      BatchExecutionMode.Compatibility,
    ]);
  });

  it('keeps operational bundles on the normal bulk execution path', () => {
    const prepared = prepareBundleSubmission(JSON.stringify({
      type: 'bundle',
      id: 'bundle--33333333-3333-4333-8333-333333333333',
      objects: [
        {
          type: 'indicator',
          id: 'indicator--11111111-1111-4111-8111-111111111111',
          extensions: {
            'extension-definition--test': { opencti_operation: 'patch' },
          },
        },
      ],
    }));

    expect(prepared.executionMode).toBe(BatchExecutionMode.Bulk);
    expect(prepared.executionReason).toBe(BatchExecutionReason.GenericBulkCompatible);
    expect(prepared.eligibleExecutionModes).toEqual([
      BatchExecutionMode.Bulk,
      BatchExecutionMode.Compatibility,
    ]);
  });

  it('keeps compatibility execution available only when explicitly requested', () => {
    const prepared = prepareBundleSubmission(JSON.stringify({
      type: 'bundle',
      id: 'bundle--44444444-4444-4444-8444-444444444444',
      objects: [
        {
          type: 'indicator',
          id: 'indicator--11111111-1111-4111-8111-111111111111',
          extensions: {
            'extension-definition--test': { opencti_operation: 'patch' },
          },
        },
      ],
    }), { executionPreference: BatchExecutionPreference.Compatibility });

    expect(prepared.executionMode).toBe(BatchExecutionMode.Compatibility);
    expect(prepared.executionReason).toBe(BatchExecutionReason.ExplicitCompatibility);
  });

  it('rejects explicit execution preferences when the bundle is not eligible', () => {
    expect(() => prepareBundleSubmission(JSON.stringify({
      type: 'bundle',
      id: 'bundle--33333333-3333-4333-8333-333333333333',
      objects: [{ type: 'malware', id: 'malware--11111111-1111-4111-8111-111111111111' }],
    }), { executionPreference: BatchExecutionPreference.Atomic }))
      .toThrowError(expect.objectContaining({
        extensions: expect.objectContaining({
          data: expect.objectContaining({
            batch_error_code: BatchAdmissionErrorCode.ExecutionPreferenceNotEligible,
          }),
        }),
      }));
  });

  it('builds queue messages with batch metadata and intact transport flags', () => {
    const prepared = prepareBundleSubmission(bundle);
    const admission = buildBatchAdmission('connector-1', 'work-1', prepared, 'batch-submission--1');
    const message = buildBatchQueueMessage(admission, 'user-1');

    expect(message).toMatchObject({
      type: 'bundle',
      applicant_id: 'user-1',
      work_id: 'work-1',
      submission_id: 'batch-submission--1',
      no_split: true,
      split_bundles: false,
      batch_id: prepared.bundleId,
      batch_execution_mode: BatchExecutionMode.Bulk,
      batch_execution_reason: BatchExecutionReason.GenericBulkCompatible,
      batch_eligible_execution_modes: [
        BatchExecutionMode.Bulk,
        BatchExecutionMode.Compatibility,
      ],
      batch_wait_until: BatchWaitUntil.Materialized,
      batch_idempotency_key: prepared.bundleId,
      batch_plan: {
        version: 1,
        object_count: 3,
        planned_object_count: 3,
        ignored_object_count: 0,
        incompatible_object_ids: [],
        ordered_object_ids: [
          'identity--11111111-1111-4111-8111-111111111111',
          'indicator--11111111-1111-4111-8111-111111111111',
          'indicator--22222222-2222-4222-8222-222222222222',
        ],
        object_normalizations: [],
        execution_phases: [
          {
            phase: 0,
            object_ids: ['identity--11111111-1111-4111-8111-111111111111'],
          },
          {
            phase: 1,
            object_ids: [
              'indicator--11111111-1111-4111-8111-111111111111',
              'indicator--22222222-2222-4222-8222-222222222222',
            ],
          },
        ],
      },
    });
    expect(JSON.parse(Buffer.from(message.content, 'base64').toString('utf-8'))).toEqual(JSON.parse(prepared.bundle));
  });
});

describe('submitStixBundle', () => {
  let submission: Record<string, any> | null;
  let generatedWork: Record<string, any> | null;

  beforeEach(() => {
    vi.clearAllMocks();
    submission = null;
    generatedWork = null;
    mockStoreLoadById.mockResolvedValue({
      id: 'connector-1',
      internal_id: 'connector-1',
      name: 'Connector 1',
    });
    vi.mocked(loadBatchSubmission).mockImplementation(async () => submission as any);
    vi.mocked(reserveBatchSubmission).mockImplementation(async (_context, input: any) => {
      submission = {
        id: input.admission.submissionId,
        internal_id: input.admission.submissionId,
        connector_id: input.admission.connectorId,
        idempotency_key: input.admission.idempotencyKey,
        payload_fingerprint: input.payloadFingerprint,
        bundle_id: input.admission.bundleId,
        work_id: input.admission.workId,
        work_origin: input.workOrigin,
        work_timestamp: input.workTimestamp ?? null,
        execution_preference: input.admission.executionPreference,
        execution_mode: input.admission.executionMode,
        execution_reason: input.admission.executionReason,
        eligible_execution_modes: input.admission.eligibleExecutionModes,
        wait_until: input.admission.waitUntil,
        cleanup_inconsistent_bundle: input.admission.cleanupInconsistentBundle,
        state: BatchSubmissionState.Reserved,
        queue_payload: JSON.stringify(input.queueMessage),
        queue_message_version: 1,
        created_at: '2026-08-08T18:00:00.000Z',
        expectation_recorded_at: null,
        published_at: null,
        last_error: null,
      };
      return submission as any;
    });
    vi.mocked(advanceBatchSubmissionState).mockImplementation(async (_context, currentSubmission: any, state: any, patch: any = {}) => {
      submission = {
        ...currentSubmission,
        ...patch,
        state,
      };
      return submission as any;
    });
    vi.mocked(loadWorkById).mockImplementation(async (_context, _user, workId) => (generatedWork?.id === workId ? generatedWork as any : null as any));
    vi.mocked(createWork).mockImplementation(async (_context, _user, _connector, _workName, _sourceId, args: any) => {
      generatedWork = {
        id: args?.preallocatedWork?.id ?? 'work-created',
        internal_id: args?.preallocatedWork?.id ?? 'work-created',
      };
      return generatedWork as any;
    });
    vi.mocked(lockResources).mockResolvedValue({ unlock: vi.fn() } as any);
  });

  it('returns a structured admission and forwards batch metadata to the worker', async () => {
    const admission = await submitStixBundle(testContext, ADMIN_USER, 'connector-1', bundle, 'work-1');

    expect(admission).toMatchObject({
      batchId: 'bundle--11111111-1111-4111-8111-111111111111',
      bundleId: 'bundle--11111111-1111-4111-8111-111111111111',
      workId: 'work-1',
      objectCount: 3,
      executionMode: BatchExecutionMode.Bulk,
      executionReason: BatchExecutionReason.GenericBulkCompatible,
      waitUntil: BatchWaitUntil.Materialized,
    });
    expect(updateExpectationsNumber).toHaveBeenCalledWith(testContext, ADMIN_USER, 'work-1', 1);
    expect(pushToWorkerForConnector).toHaveBeenCalledWith(
      'connector-1',
      expect.objectContaining({
        batch_id: admission.batchId,
        batch_execution_mode: BatchExecutionMode.Bulk,
        batch_execution_reason: BatchExecutionReason.GenericBulkCompatible,
        batch_wait_until: BatchWaitUntil.Materialized,
        no_split: true,
        split_bundles: false,
      }),
    );
  });

  it('creates a work when the caller does not provide one', async () => {
    const admission = await submitStixBundle(testContext, ADMIN_USER, 'connector-1', bundle, undefined);

    expect(createWork).toHaveBeenCalledTimes(1);
    expect(admission.workId).toBe('work-created');
    expect(updateExpectationsNumber).toHaveBeenCalledWith(testContext, ADMIN_USER, 'work-created', 1);
  });

  it('reuses one durable submission identity, generated work, and publication for an explicit-key replay', async () => {
    const options = { idempotencyKey: 'feed-run-2026-08-08' };

    const firstAdmission = await submitStixBundle(testContext, ADMIN_USER, 'connector-1', bundle, undefined, options);
    const replayAdmission = await submitStixBundle(testContext, ADMIN_USER, 'connector-1', reorderedBundle, undefined, options);

    expect(replayAdmission).toMatchObject({
      batchId: firstAdmission.batchId,
      bundleId: firstAdmission.bundleId,
      workId: firstAdmission.workId,
      idempotencyKey: firstAdmission.idempotencyKey,
      submissionId: firstAdmission.submissionId,
    });
    expect(firstAdmission.submissionId).toBe(buildBatchSubmissionId('connector-1', options.idempotencyKey));
    expect(reserveBatchSubmission).toHaveBeenCalledTimes(1);
    expect(createWork).toHaveBeenCalledTimes(1);
    expect(updateBatchSubmissionExpectation).toHaveBeenCalledTimes(1);
    expect(pushToWorkerForConnector).toHaveBeenCalledTimes(1);
  });

  it('reuses one durable submission identity when the caller supplies a work id', async () => {
    const options = { idempotencyKey: 'feed-run-2026-08-08' };

    const firstAdmission = await submitStixBundle(testContext, ADMIN_USER, 'connector-1', bundle, 'work-caller-1', options);
    const replayAdmission = await submitStixBundle(testContext, ADMIN_USER, 'connector-1', reorderedBundle, 'work-caller-2', options);

    expect(replayAdmission).toMatchObject({
      workId: 'work-caller-1',
      submissionId: firstAdmission.submissionId,
    });
    expect(submission?.work_origin).toBe(BatchSubmissionWorkOrigin.CallerProvided);
    expect(reserveBatchSubmission).toHaveBeenCalledTimes(1);
    expect(createWork).not.toHaveBeenCalled();
    expect(updateBatchSubmissionExpectation).toHaveBeenCalledTimes(1);
    expect(pushToWorkerForConnector).toHaveBeenCalledTimes(1);
  });

  it('rejects conflicting normalized payload reuse before duplicating expectation or publication', async () => {
    const options = { idempotencyKey: 'feed-run-2026-08-08' };

    await submitStixBundle(testContext, ADMIN_USER, 'connector-1', bundle, 'work-caller-1', options);

    await expect(submitStixBundle(testContext, ADMIN_USER, 'connector-1', conflictingBundle, 'work-caller-2', options))
      .rejects.toThrowError(expect.objectContaining({
        extensions: expect.objectContaining({
          data: expect.objectContaining({
            batch_error_code: BatchAdmissionErrorCode.IdempotencyKeyConflict,
          }),
        }),
      }));
    expect(reserveBatchSubmission).toHaveBeenCalledTimes(1);
    expect(createWork).not.toHaveBeenCalled();
    expect(updateBatchSubmissionExpectation).toHaveBeenCalledTimes(1);
    expect(pushToWorkerForConnector).toHaveBeenCalledTimes(1);
  });

  it('resumes a persisted pre-publication submission without creating a second logical handoff', async () => {
    const options = { idempotencyKey: 'feed-run-2026-08-08' };
    vi.mocked(pushToWorkerForConnector)
      .mockRejectedValueOnce(new Error('rabbit unavailable'))
      .mockResolvedValueOnce(true as any);

    await expect(submitStixBundle(testContext, ADMIN_USER, 'connector-1', bundle, 'work-caller-1', options))
      .rejects.toThrow('rabbit unavailable');

    const retryAdmission = await submitStixBundle(testContext, ADMIN_USER, 'connector-1', reorderedBundle, 'work-caller-2', options);

    expect(retryAdmission).toMatchObject({
      workId: 'work-caller-1',
      submissionId: buildBatchSubmissionId('connector-1', options.idempotencyKey),
    });
    expect(reserveBatchSubmission).toHaveBeenCalledTimes(1);
    expect(updateBatchSubmissionExpectation).toHaveBeenCalledTimes(1);
    expect(pushToWorkerForConnector).toHaveBeenCalledTimes(2);
    expect(pushToWorkerForConnector).toHaveBeenLastCalledWith(
      'connector-1',
      expect.objectContaining({
        submission_id: retryAdmission.submissionId,
      }),
    );
    expect(recordBatchSubmissionError).toHaveBeenCalledTimes(1);
    expect(submission?.state).toBe(BatchSubmissionState.Published);
  });

  it('lets the compatibility mutation request committed-only execution', async () => {
    await expect(sendStixBundle(
      testContext,
      ADMIN_USER,
      'connector-1',
      bundle,
      'work-1',
      false,
      false,
      BatchWaitUntil.Committed,
    )).resolves.toBe(true);

    expect(pushToWorkerForConnector).toHaveBeenCalledWith(
      'connector-1',
      expect.objectContaining({
        batch_wait_until: BatchWaitUntil.Committed,
      }),
    );
  });
});
