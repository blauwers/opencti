import { beforeEach, describe, expect, it, vi } from 'vitest';
import {
  BatchAdmissionErrorCode,
  BatchExecutionMode,
  BatchExecutionPreference,
  BatchExecutionReason,
  BatchWaitUntil,
} from '../../../src/modules/batch/batch-types';
import {
  buildBatchAdmission,
  buildBatchQueueMessage,
  prepareBundleSubmission,
} from '../../../src/modules/batch/batch-domain';
import { sendStixBundle, submitStixBundle } from '../../../src/domain/stix';
import { ADMIN_USER, testContext } from '../../utils/testQuery';
import { pushToWorkerForConnector } from '../../../src/database/rabbitmq';
import { createWork, updateExpectationsNumber } from '../../../src/domain/work';

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
    updateExpectationsNumber: vi.fn(),
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
    const admission = buildBatchAdmission('connector-1', 'work-1', prepared);
    const message = buildBatchQueueMessage(admission, 'user-1');

    expect(message).toMatchObject({
      type: 'bundle',
      applicant_id: 'user-1',
      work_id: 'work-1',
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
  beforeEach(() => {
    vi.clearAllMocks();
    mockStoreLoadById.mockResolvedValue({
      id: 'connector-1',
      internal_id: 'connector-1',
      name: 'Connector 1',
    });
    vi.mocked(createWork).mockResolvedValue({ id: 'work-created' } as any);
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
