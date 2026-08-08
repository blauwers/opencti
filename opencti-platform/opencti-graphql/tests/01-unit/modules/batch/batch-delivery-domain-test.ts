import { beforeEach, describe, expect, it, vi } from 'vitest';
import {
  assertBatchDeliveryReservation,
  buildBatchDeliveryPayloadFingerprint,
  buildChildBatchDeliveryEnvelope,
  buildChildBatchDeliveryId,
  buildRootBatchDeliveryEnvelope,
  buildRootBatchDeliveryId,
  reserveBatchDelivery,
} from '../../../../src/modules/batch/batch-delivery-domain';
import {
  BatchAdmissionErrorCode,
  BatchDeliveryBranchKind,
  BatchDeliveryKind,
  BatchDeliveryProtocol,
  BatchDeliveryState,
  type BatchQueueMessage,
} from '../../../../src/modules/batch/batch-types';
import { elIndex, elLoadById } from '../../../../src/database/engine';
import { testContext } from '../../../utils/testQuery';

vi.mock('../../../../src/database/engine', () => ({
  elIndex: vi.fn(),
  elLoadById: vi.fn(),
  elUpdate: vi.fn(),
}));

const queueMessage = {
  type: 'bundle',
  applicant_id: 'user-1',
  content: 'eyJ0eXBlIjoiYnVuZGxlIn0=',
  work_id: 'work-1',
  update: true,
  no_split: true,
  split_bundles: false,
  cleanup_inconsistent_bundle: false,
  batch_id: 'bundle--1',
  batch_execution_mode: 'BULK',
  batch_execution_reason: 'GENERIC_BULK_COMPATIBLE',
  batch_eligible_execution_modes: ['BULK', 'COMPATIBILITY'],
  batch_wait_until: 'MATERIALIZED',
  batch_idempotency_key: 'feed-run-1',
  submission_id: 'batch-submission--1',
  batch_plan: {
    version: 1,
    object_count: 1,
    planned_object_count: 1,
    ignored_object_count: 0,
    incompatible_object_ids: [],
    ordered_object_ids: ['indicator--1'],
    object_normalizations: [],
    execution_phases: [{ phase: 0, object_ids: ['indicator--1'] }],
  },
} as unknown as BatchQueueMessage;

describe('batch delivery domain', () => {
  let deliveries: Map<string, any>;

  beforeEach(() => {
    vi.clearAllMocks();
    deliveries = new Map();
    vi.mocked(elLoadById).mockImplementation(async (_context, _user, id) => deliveries.get(id) ?? null);
    vi.mocked(elIndex).mockImplementation(async (_index, document) => {
      deliveries.set(document.internal_id, document);
      return document;
    });
  });

  it('builds deterministic root and child delivery ids without sibling or replay collisions', () => {
    const rootId = buildRootBatchDeliveryId('batch-submission--1');
    const firstSplitId = buildChildBatchDeliveryId(rootId, BatchDeliveryBranchKind.LegacySplit, 0, 0);
    const secondSplitId = buildChildBatchDeliveryId(rootId, BatchDeliveryBranchKind.LegacySplit, 0, 1);
    const replayId = buildChildBatchDeliveryId(rootId, BatchDeliveryBranchKind.IntactReplay, 1, 0);

    expect(rootId).toBe(buildRootBatchDeliveryId('batch-submission--1'));
    expect(rootId).not.toBe('batch-submission--1');
    expect(firstSplitId).toBe(buildChildBatchDeliveryId(rootId, BatchDeliveryBranchKind.LegacySplit, 0, 0));
    expect(new Set([firstSplitId, secondSplitId, replayId]).size).toBe(3);
  });

  it('canonicalizes equivalent delivery payloads before hashing', () => {
    expect(buildBatchDeliveryPayloadFingerprint({ b: 2, a: 1 })).toBe(
      buildBatchDeliveryPayloadFingerprint({ a: 1, b: 2 }),
    );
    expect(buildBatchDeliveryPayloadFingerprint({ a: 1 })).not.toBe(
      buildBatchDeliveryPayloadFingerprint({ a: 2 }),
    );
  });

  it('builds bounded v2 root and child envelope metadata', () => {
    const rootEnvelope = buildRootBatchDeliveryEnvelope('batch-submission--1');
    const childEnvelope = buildChildBatchDeliveryEnvelope(
      rootEnvelope.delivery_id,
      BatchDeliveryBranchKind.LegacySplit,
      0,
      1,
    );

    expect(rootEnvelope).toEqual({
      delivery_id: buildRootBatchDeliveryId('batch-submission--1'),
      parent_delivery_id: null,
      delivery_kind: BatchDeliveryKind.Root,
      delivery_protocol_version: BatchDeliveryProtocol.V2,
      delivery_branch_kind: BatchDeliveryBranchKind.Root,
      delivery_branch_sequence: 0,
      delivery_branch_ordinal: 0,
    });
    expect(childEnvelope).toEqual({
      delivery_id: buildChildBatchDeliveryId(rootEnvelope.delivery_id, BatchDeliveryBranchKind.LegacySplit, 0, 1),
      parent_delivery_id: rootEnvelope.delivery_id,
      delivery_kind: BatchDeliveryKind.Child,
      delivery_protocol_version: BatchDeliveryProtocol.V2,
      delivery_branch_kind: BatchDeliveryBranchKind.LegacySplit,
      delivery_branch_sequence: 0,
      delivery_branch_ordinal: 1,
    });
  });

  it('reserves one durable root delivery and returns it on an identical retry', async () => {
    const input = {
      deliveryId: buildRootBatchDeliveryId('batch-submission--1'),
      submissionId: 'batch-submission--1',
      parentDeliveryId: null,
      deliveryKind: BatchDeliveryKind.Root,
      branchKind: BatchDeliveryBranchKind.Root,
      branchSequence: 0,
      branchOrdinal: 0,
      payloadFingerprint: 'fingerprint-1',
      queueMessage,
      requiredWorkerProtocol: BatchDeliveryProtocol.V1,
    };

    const first = await reserveBatchDelivery(testContext, input);
    const replay = await reserveBatchDelivery(testContext, input);

    expect(first).toMatchObject({
      internal_id: input.deliveryId,
      submission_id: 'batch-submission--1',
      delivery_kind: BatchDeliveryKind.Root,
      branch_kind: BatchDeliveryBranchKind.Root,
      required_worker_protocol: BatchDeliveryProtocol.V1,
      state: BatchDeliveryState.Ready,
    });
    expect(replay).toBe(first);
    expect(elIndex).toHaveBeenCalledTimes(1);
  });

  it('rejects a delivery id that does not match the lineage tuple', async () => {
    await expect(reserveBatchDelivery(testContext, {
      deliveryId: 'batch-delivery--wrong',
      submissionId: 'batch-submission--1',
      parentDeliveryId: null,
      deliveryKind: BatchDeliveryKind.Root,
      branchKind: BatchDeliveryBranchKind.Root,
      branchSequence: 0,
      branchOrdinal: 0,
      payloadFingerprint: 'fingerprint-1',
      queueMessage,
      requiredWorkerProtocol: BatchDeliveryProtocol.V1,
    })).rejects.toThrowError(expect.objectContaining({
      extensions: expect.objectContaining({
        data: expect.objectContaining({
          batch_error_code: BatchAdmissionErrorCode.DeliveryIdentityConflict,
        }),
      }),
    }));
    expect(elIndex).not.toHaveBeenCalled();
  });

  it('fails closed when a root or child slot is reused with a different payload', async () => {
    const rootEnvelope = buildRootBatchDeliveryEnvelope('batch-submission--1');
    const rootInput = {
      deliveryId: rootEnvelope.delivery_id,
      submissionId: 'batch-submission--1',
      parentDeliveryId: null,
      deliveryKind: BatchDeliveryKind.Root,
      branchKind: BatchDeliveryBranchKind.Root,
      branchSequence: 0,
      branchOrdinal: 0,
      payloadFingerprint: 'fingerprint-1',
      queueMessage,
      requiredWorkerProtocol: BatchDeliveryProtocol.V2,
    };
    const root = await reserveBatchDelivery(testContext, rootInput);
    const childInput = {
      deliveryId: buildChildBatchDeliveryId(root.internal_id, BatchDeliveryBranchKind.LegacySplit, 0, 0),
      submissionId: root.submission_id,
      parentDeliveryId: root.internal_id,
      deliveryKind: BatchDeliveryKind.Child,
      branchKind: BatchDeliveryBranchKind.LegacySplit,
      branchSequence: 0,
      branchOrdinal: 0,
      payloadFingerprint: 'child-fingerprint-1',
      queueMessage: {
        ...queueMessage,
        ...buildChildBatchDeliveryEnvelope(root.internal_id, BatchDeliveryBranchKind.LegacySplit, 0, 0),
      },
      requiredWorkerProtocol: BatchDeliveryProtocol.V2,
    };
    const child = await reserveBatchDelivery(testContext, childInput);

    expect(() => assertBatchDeliveryReservation(root, { ...rootInput, payloadFingerprint: 'fingerprint-2' }))
      .toThrowError(expect.objectContaining({
        extensions: expect.objectContaining({
          data: expect.objectContaining({
            batch_error_code: BatchAdmissionErrorCode.DeliveryIdentityConflict,
          }),
        }),
      }));
    expect(() => assertBatchDeliveryReservation(child, { ...childInput, payloadFingerprint: 'child-fingerprint-2' }))
      .toThrowError(expect.objectContaining({
        extensions: expect.objectContaining({
          data: expect.objectContaining({
            batch_error_code: BatchAdmissionErrorCode.DeliveryIdentityConflict,
          }),
        }),
      }));
  });
});
