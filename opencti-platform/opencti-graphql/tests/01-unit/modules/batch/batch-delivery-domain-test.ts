import { beforeEach, describe, expect, it, vi } from 'vitest';
import {
  assertBatchDeliveryReservation,
  buildBatchDeliveryCandidateId,
  buildBatchDeliveryPayloadFingerprint,
  buildChildBatchDeliveryEnvelope,
  buildChildBatchDeliveryId,
  buildRootBatchDeliveryEnvelope,
  buildRootBatchDeliveryId,
  loadBatchDeliveryHandoff,
  markBatchDeliveryChildrenPublished,
  promoteBatchDeliveryCandidateRoot,
  readBatchDeliveryQueueMessage,
  reserveBatchDelivery,
  reserveBatchDeliveryChildren,
} from '../../../../src/modules/batch/batch-delivery-domain';
import {
  BatchAdmissionErrorCode,
  BatchDeliveryBranchKind,
  BatchDeliveryHandoffEvidence,
  BatchDeliveryKind,
  BatchDeliveryProtocol,
  BatchDeliveryState,
  type BatchDeliveryChildReservationInput,
  type BatchQueueMessage,
} from '../../../../src/modules/batch/batch-types';
import { elFindByIds, elIndex, elLoadById, elUpdate } from '../../../../src/database/engine';
import { updateBatchExpectation } from '../../../../src/domain/work';
import { lockResources } from '../../../../src/lock/master-lock';
import { SYSTEM_USER } from '../../../../src/utils/access';
import { testContext } from '../../../utils/testQuery';

vi.mock('../../../../src/database/engine', () => ({
  elFindByIds: vi.fn(),
  elIndex: vi.fn(),
  elLoadById: vi.fn(),
  elUpdate: vi.fn(),
}));

vi.mock('../../../../src/lock/master-lock', () => ({
  lockResources: vi.fn(),
}));

vi.mock('../../../../src/domain/work', () => ({
  updateBatchExpectation: vi.fn(),
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
    vi.mocked(elFindByIds).mockImplementation(async (_context, _user, ids) => {
      return (Array.isArray(ids) ? ids : [ids])
        .map((id) => deliveries.get(id))
        .filter((delivery) => delivery !== undefined);
    });
    vi.mocked(elIndex).mockImplementation(async (_index, document) => {
      deliveries.set(document.internal_id, document);
      return document;
    });
    vi.mocked(elUpdate).mockImplementation(async (_context, _index, id, update) => {
      const current = deliveries.get(id);
      const next = {
        ...current,
        ...(update as any).doc,
      };
      deliveries.set(id, next);
      return next;
    });
    vi.mocked(lockResources).mockResolvedValue({ unlock: vi.fn() } as any);
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

  it('rejects unknown child branch kinds at the domain boundary', () => {
    expect(() => buildChildBatchDeliveryId(
      buildRootBatchDeliveryId('batch-submission--1'),
      'NOT_A_BRANCH' as Exclude<BatchDeliveryBranchKind, BatchDeliveryBranchKind.Root>,
      0,
      0,
    )).toThrowError(expect.objectContaining({
      extensions: expect.objectContaining({
        data: expect.objectContaining({
          batch_error_code: BatchAdmissionErrorCode.DeliveryIdentityConflict,
        }),
      }),
    }));
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

  it('promotes one candidate-bearing root payload into durable v2 state and replays idempotently', async () => {
    const candidateId = buildBatchDeliveryCandidateId();
    const payloadFingerprint = buildBatchDeliveryPayloadFingerprint({ type: 'bundle' });
    const promotion = {
      candidateId,
      payloadFingerprint,
      workId: 'work-1',
      additionalWorkIds: ['work-2'],
    };

    const first = await promoteBatchDeliveryCandidateRoot(testContext, promotion);
    const replay = await promoteBatchDeliveryCandidateRoot(testContext, promotion);

    expect(first).toMatchObject({
      internal_id: buildRootBatchDeliveryId(candidateId),
      submission_id: candidateId,
      delivery_kind: BatchDeliveryKind.Root,
      branch_kind: BatchDeliveryBranchKind.Root,
      payload_fingerprint: payloadFingerprint,
      required_worker_protocol: BatchDeliveryProtocol.V2,
    });
    expect(JSON.parse(first.queue_payload)).toMatchObject({
      type: 'batch_delivery_handoff_anchor',
      batch_delivery_candidate_id: candidateId,
      batch_delivery_candidate_payload_fingerprint: payloadFingerprint,
      work_id: 'work-1',
      additional_work_ids: ['work-2'],
      submission_id: candidateId,
      delivery_id: buildRootBatchDeliveryId(candidateId),
      parent_delivery_id: null,
      delivery_kind: BatchDeliveryKind.Root,
      delivery_protocol_version: BatchDeliveryProtocol.V2,
      delivery_branch_kind: BatchDeliveryBranchKind.Root,
    });
    expect(() => readBatchDeliveryQueueMessage(first)).toThrowError('Batch delivery queue payload is a non-executable handoff anchor');
    expect(replay).toBe(first);
    expect(elIndex).toHaveBeenCalledTimes(1);
  });

  it('rejects candidate promotion when the candidate or payload identity is invalid', async () => {
    const candidateId = buildBatchDeliveryCandidateId();
    const payloadFingerprint = buildBatchDeliveryPayloadFingerprint({ type: 'bundle' });

    await expect(promoteBatchDeliveryCandidateRoot(testContext, {
      candidateId: 'not-a-candidate',
      payloadFingerprint,
      workId: 'work-1',
    }))
      .rejects.toThrowError(expect.objectContaining({
        extensions: expect.objectContaining({
          data: expect.objectContaining({
            batch_error_code: BatchAdmissionErrorCode.DeliveryIdentityConflict,
          }),
        }),
      }));
    await expect(promoteBatchDeliveryCandidateRoot(testContext, {
      candidateId,
      payloadFingerprint: 'not-a-fingerprint',
      workId: 'work-1',
    }))
      .rejects.toThrowError(expect.objectContaining({
        extensions: expect.objectContaining({
          data: expect.objectContaining({
            batch_error_code: BatchAdmissionErrorCode.DeliveryIdentityConflict,
          }),
        }),
      }));
    expect(elIndex).not.toHaveBeenCalled();
  });

  it('rejects candidate promotion replay when compact metadata changes', async () => {
    const candidateId = buildBatchDeliveryCandidateId();
    const payloadFingerprint = buildBatchDeliveryPayloadFingerprint({ type: 'bundle' });

    await promoteBatchDeliveryCandidateRoot(testContext, {
      candidateId,
      payloadFingerprint,
      workId: 'work-1',
    });

    await expect(promoteBatchDeliveryCandidateRoot(testContext, {
      candidateId,
      payloadFingerprint,
      workId: 'work-2',
    }))
      .rejects.toThrowError(expect.objectContaining({
        extensions: expect.objectContaining({
          data: expect.objectContaining({
            batch_error_code: BatchAdmissionErrorCode.DeliveryIdentityConflict,
          }),
        }),
      }));
  });

  it('uses compact promoted roots for platform-owned split expectation replacement', async () => {
    const candidateId = buildBatchDeliveryCandidateId();
    const root = await promoteBatchDeliveryCandidateRoot(testContext, {
      candidateId,
      payloadFingerprint: buildBatchDeliveryPayloadFingerprint({ type: 'bundle' }),
      workId: 'work-1',
      additionalWorkIds: ['work-2'],
    });

    await reserveBatchDeliveryChildren(testContext, root.internal_id, [
      {
        branchKind: BatchDeliveryBranchKind.OversizedChunk,
        branchSequence: 0,
        branchOrdinal: 0,
        queueMessage: {
          ...queueMessage,
          submission_id: candidateId,
          content: Buffer.from(JSON.stringify({ type: 'bundle', id: 'bundle--1', objects: [{ id: 'indicator--1' }] })).toString('base64'),
          ...buildChildBatchDeliveryEnvelope(root.internal_id, BatchDeliveryBranchKind.OversizedChunk, 0, 0),
        },
      },
      {
        branchKind: BatchDeliveryBranchKind.OversizedChunk,
        branchSequence: 0,
        branchOrdinal: 1,
        queueMessage: {
          ...queueMessage,
          submission_id: candidateId,
          content: Buffer.from(JSON.stringify({ type: 'bundle', id: 'bundle--1', objects: [{ id: 'indicator--2' }] })).toString('base64'),
          ...buildChildBatchDeliveryEnvelope(root.internal_id, BatchDeliveryBranchKind.OversizedChunk, 0, 1),
        },
      },
    ]);

    expect(updateBatchExpectation).toHaveBeenCalledTimes(2);
    expect(updateBatchExpectation).toHaveBeenNthCalledWith(1, testContext, SYSTEM_USER, 'work-1', 1, root.internal_id);
    expect(updateBatchExpectation).toHaveBeenNthCalledWith(2, testContext, SYSTEM_USER, 'work-2', 1, root.internal_id);
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
      queueMessage: {
        ...queueMessage,
        ...rootEnvelope,
      },
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

  it('reserves one immutable child set before publication and reuses it on replay', async () => {
    const rootEnvelope = buildRootBatchDeliveryEnvelope('batch-submission--1');
    const root = await reserveBatchDelivery(testContext, {
      deliveryId: rootEnvelope.delivery_id,
      submissionId: 'batch-submission--1',
      parentDeliveryId: null,
      deliveryKind: BatchDeliveryKind.Root,
      branchKind: BatchDeliveryBranchKind.Root,
      branchSequence: 0,
      branchOrdinal: 0,
      payloadFingerprint: buildBatchDeliveryPayloadFingerprint({ type: 'bundle', id: 'bundle--1' }),
      queueMessage: {
        ...queueMessage,
        content: Buffer.from(JSON.stringify({ type: 'bundle', id: 'bundle--1' })).toString('base64'),
        ...rootEnvelope,
      },
      requiredWorkerProtocol: BatchDeliveryProtocol.V2,
    });
    const childInputs: BatchDeliveryChildReservationInput[] = [
      {
        branchKind: BatchDeliveryBranchKind.LegacySplit,
        branchSequence: 0,
        branchOrdinal: 0,
        queueMessage: {
          ...queueMessage,
          content: Buffer.from(JSON.stringify({ type: 'bundle', id: 'bundle--1', objects: [{ id: 'indicator--1' }] })).toString('base64'),
          ...buildChildBatchDeliveryEnvelope(root.internal_id, BatchDeliveryBranchKind.LegacySplit, 0, 0),
        },
      },
      {
        branchKind: BatchDeliveryBranchKind.LegacySplit,
        branchSequence: 0,
        branchOrdinal: 1,
        queueMessage: {
          ...queueMessage,
          content: Buffer.from(JSON.stringify({ type: 'bundle', id: 'bundle--1', objects: [{ id: 'indicator--2' }] })).toString('base64'),
          ...buildChildBatchDeliveryEnvelope(root.internal_id, BatchDeliveryBranchKind.LegacySplit, 0, 1),
        },
      },
    ];

    const first = await reserveBatchDeliveryChildren(testContext, root.internal_id, childInputs);
    const replay = await reserveBatchDeliveryChildren(testContext, root.internal_id, childInputs);

    expect(first.parentDelivery).toMatchObject({
      internal_id: root.internal_id,
      handoff_evidence: BatchDeliveryHandoffEvidence.ChildrenReserved,
      child_count: 2,
      child_delivery_ids: [
        buildChildBatchDeliveryId(root.internal_id, BatchDeliveryBranchKind.LegacySplit, 0, 0),
        buildChildBatchDeliveryId(root.internal_id, BatchDeliveryBranchKind.LegacySplit, 0, 1),
      ],
    });
    expect(first.pendingChildren.map((child) => child.internal_id)).toEqual(first.parentDelivery.child_delivery_ids);
    expect(replay.parentDelivery.child_set_fingerprint).toBe(first.parentDelivery.child_set_fingerprint);
    expect(replay.children.map((child) => child.internal_id)).toEqual(first.children.map((child) => child.internal_id));
    expect(elIndex).toHaveBeenCalledTimes(3);
    expect(updateBatchExpectation).toHaveBeenCalledTimes(1);
    expect(updateBatchExpectation).toHaveBeenCalledWith(testContext, SYSTEM_USER, 'work-1', 1, root.internal_id);
  });

  it('applies one split replacement delta to every attributed work id', async () => {
    const rootEnvelope = buildRootBatchDeliveryEnvelope('batch-submission--1');
    const root = await reserveBatchDelivery(testContext, {
      deliveryId: rootEnvelope.delivery_id,
      submissionId: 'batch-submission--1',
      parentDeliveryId: null,
      deliveryKind: BatchDeliveryKind.Root,
      branchKind: BatchDeliveryBranchKind.Root,
      branchSequence: 0,
      branchOrdinal: 0,
      payloadFingerprint: buildBatchDeliveryPayloadFingerprint({ type: 'bundle', id: 'bundle--1' }),
      queueMessage: {
        ...queueMessage,
        additional_work_ids: ['work-2', 'work-1'],
        content: Buffer.from(JSON.stringify({ type: 'bundle', id: 'bundle--1' })).toString('base64'),
        ...rootEnvelope,
      },
      requiredWorkerProtocol: BatchDeliveryProtocol.V2,
    });

    await reserveBatchDeliveryChildren(testContext, root.internal_id, [
      {
        branchKind: BatchDeliveryBranchKind.OversizedChunk,
        branchSequence: 0,
        branchOrdinal: 0,
        queueMessage: {
          ...queueMessage,
          content: Buffer.from(JSON.stringify({ type: 'bundle', id: 'bundle--1', objects: [{ id: 'indicator--1' }] })).toString('base64'),
          ...buildChildBatchDeliveryEnvelope(root.internal_id, BatchDeliveryBranchKind.OversizedChunk, 0, 0),
        },
      },
      {
        branchKind: BatchDeliveryBranchKind.OversizedChunk,
        branchSequence: 0,
        branchOrdinal: 1,
        queueMessage: {
          ...queueMessage,
          content: Buffer.from(JSON.stringify({ type: 'bundle', id: 'bundle--1', objects: [{ id: 'indicator--2' }] })).toString('base64'),
          ...buildChildBatchDeliveryEnvelope(root.internal_id, BatchDeliveryBranchKind.OversizedChunk, 0, 1),
        },
      },
    ]);

    expect(updateBatchExpectation).toHaveBeenCalledTimes(2);
    expect(updateBatchExpectation).toHaveBeenNthCalledWith(1, testContext, SYSTEM_USER, 'work-1', 1, root.internal_id);
    expect(updateBatchExpectation).toHaveBeenNthCalledWith(2, testContext, SYSTEM_USER, 'work-2', 1, root.internal_id);
  });

  it('fails closed when an already planned child set changes membership or payload', async () => {
    const rootEnvelope = buildRootBatchDeliveryEnvelope('batch-submission--1');
    const root = await reserveBatchDelivery(testContext, {
      deliveryId: rootEnvelope.delivery_id,
      submissionId: 'batch-submission--1',
      parentDeliveryId: null,
      deliveryKind: BatchDeliveryKind.Root,
      branchKind: BatchDeliveryBranchKind.Root,
      branchSequence: 0,
      branchOrdinal: 0,
      payloadFingerprint: buildBatchDeliveryPayloadFingerprint({ type: 'bundle', id: 'bundle--1' }),
      queueMessage: {
        ...queueMessage,
        content: Buffer.from(JSON.stringify({ type: 'bundle', id: 'bundle--1' })).toString('base64'),
        ...rootEnvelope,
      },
      requiredWorkerProtocol: BatchDeliveryProtocol.V2,
    });
    const child: BatchDeliveryChildReservationInput = {
      branchKind: BatchDeliveryBranchKind.IntactReplay,
      branchSequence: 1,
      branchOrdinal: 0,
      queueMessage: {
        ...queueMessage,
        content: Buffer.from(JSON.stringify({ type: 'bundle', id: 'bundle--1', objects: [{ id: 'indicator--1' }] })).toString('base64'),
        ...buildChildBatchDeliveryEnvelope(root.internal_id, BatchDeliveryBranchKind.IntactReplay, 1, 0),
      },
    };
    await reserveBatchDeliveryChildren(testContext, root.internal_id, [child]);

    await expect(reserveBatchDeliveryChildren(testContext, root.internal_id, [{
      ...child,
      queueMessage: {
        ...child.queueMessage,
        content: Buffer.from(JSON.stringify({ type: 'bundle', id: 'bundle--1', objects: [{ id: 'indicator--2' }] })).toString('base64'),
      },
    }])).rejects.toThrowError(expect.objectContaining({
      extensions: expect.objectContaining({
        data: expect.objectContaining({
          batch_error_code: BatchAdmissionErrorCode.DeliveryIdentityConflict,
        }),
      }),
    }));
    await expect(reserveBatchDeliveryChildren(testContext, root.internal_id, [{
      ...child,
      branchSequence: 2,
      queueMessage: {
        ...child.queueMessage,
        ...buildChildBatchDeliveryEnvelope(root.internal_id, BatchDeliveryBranchKind.IntactReplay, 2, 0),
      },
    }])).rejects.toThrowError(expect.objectContaining({
      extensions: expect.objectContaining({
        data: expect.objectContaining({
          batch_error_code: BatchAdmissionErrorCode.DeliveryIdentityConflict,
        }),
      }),
    }));
    await expect(reserveBatchDeliveryChildren(testContext, root.internal_id, [
      child,
      {
        branchKind: BatchDeliveryBranchKind.IntactReplay,
        branchSequence: 2,
        branchOrdinal: 0,
        queueMessage: {
          ...queueMessage,
          content: Buffer.from(JSON.stringify({ type: 'bundle', id: 'bundle--1', objects: [{ id: 'indicator--3' }] })).toString('base64'),
          ...buildChildBatchDeliveryEnvelope(root.internal_id, BatchDeliveryBranchKind.IntactReplay, 2, 0),
        },
      },
    ])).rejects.toThrowError(expect.objectContaining({
      extensions: expect.objectContaining({
        data: expect.objectContaining({
          batch_error_code: BatchAdmissionErrorCode.DeliveryIdentityConflict,
        }),
      }),
    }));
    expect(updateBatchExpectation).not.toHaveBeenCalled();
  });

  it('fails closed when a planned child set changes queue metadata before child inserts complete', async () => {
    const rootEnvelope = buildRootBatchDeliveryEnvelope('batch-submission--1');
    const root = await reserveBatchDelivery(testContext, {
      deliveryId: rootEnvelope.delivery_id,
      submissionId: 'batch-submission--1',
      parentDeliveryId: null,
      deliveryKind: BatchDeliveryKind.Root,
      branchKind: BatchDeliveryBranchKind.Root,
      branchSequence: 0,
      branchOrdinal: 0,
      payloadFingerprint: buildBatchDeliveryPayloadFingerprint({ type: 'bundle', id: 'bundle--1' }),
      queueMessage: {
        ...queueMessage,
        content: Buffer.from(JSON.stringify({ type: 'bundle', id: 'bundle--1' })).toString('base64'),
        ...rootEnvelope,
      },
      requiredWorkerProtocol: BatchDeliveryProtocol.V2,
    });
    const child: BatchDeliveryChildReservationInput = {
      branchKind: BatchDeliveryBranchKind.IntactReplay,
      branchSequence: 1,
      branchOrdinal: 0,
      queueMessage: {
        ...queueMessage,
        content: Buffer.from(JSON.stringify({ type: 'bundle', id: 'bundle--1', objects: [{ id: 'indicator--1' }] })).toString('base64'),
        ...buildChildBatchDeliveryEnvelope(root.internal_id, BatchDeliveryBranchKind.IntactReplay, 1, 0),
      },
    };
    vi.mocked(elIndex).mockRejectedValueOnce(new Error('child storage unavailable'));

    await expect(reserveBatchDeliveryChildren(testContext, root.internal_id, [child]))
      .rejects.toThrowError('child storage unavailable');

    await expect(reserveBatchDeliveryChildren(testContext, root.internal_id, [{
      ...child,
      queueMessage: {
        ...child.queueMessage,
        no_split: false,
      },
    }])).rejects.toThrowError(expect.objectContaining({
      extensions: expect.objectContaining({
        data: expect.objectContaining({
          batch_error_code: BatchAdmissionErrorCode.DeliveryIdentityConflict,
        }),
      }),
    }));
  });

  it('advances handoff evidence monotonically and returns only missing published children', async () => {
    const rootEnvelope = buildRootBatchDeliveryEnvelope('batch-submission--1');
    const root = await reserveBatchDelivery(testContext, {
      deliveryId: rootEnvelope.delivery_id,
      submissionId: 'batch-submission--1',
      parentDeliveryId: null,
      deliveryKind: BatchDeliveryKind.Root,
      branchKind: BatchDeliveryBranchKind.Root,
      branchSequence: 0,
      branchOrdinal: 0,
      payloadFingerprint: buildBatchDeliveryPayloadFingerprint({ type: 'bundle', id: 'bundle--1' }),
      queueMessage: {
        ...queueMessage,
        content: Buffer.from(JSON.stringify({ type: 'bundle', id: 'bundle--1' })).toString('base64'),
        ...rootEnvelope,
      },
      requiredWorkerProtocol: BatchDeliveryProtocol.V2,
    });
    const reservation = await reserveBatchDeliveryChildren(testContext, root.internal_id, [
      {
        branchKind: BatchDeliveryBranchKind.OversizedChunk,
        branchSequence: 0,
        branchOrdinal: 0,
        queueMessage: {
          ...queueMessage,
          content: Buffer.from(JSON.stringify({ type: 'bundle', id: 'bundle--1', objects: [{ id: 'indicator--1' }] })).toString('base64'),
          ...buildChildBatchDeliveryEnvelope(root.internal_id, BatchDeliveryBranchKind.OversizedChunk, 0, 0),
        },
      },
      {
        branchKind: BatchDeliveryBranchKind.OversizedChunk,
        branchSequence: 0,
        branchOrdinal: 1,
        queueMessage: {
          ...queueMessage,
          content: Buffer.from(JSON.stringify({ type: 'bundle', id: 'bundle--1', objects: [{ id: 'indicator--2' }] })).toString('base64'),
          ...buildChildBatchDeliveryEnvelope(root.internal_id, BatchDeliveryBranchKind.OversizedChunk, 0, 1),
        },
      },
    ]);
    const [firstChild, secondChild] = reservation.children;

    const afterFirst = await markBatchDeliveryChildrenPublished(testContext, root.internal_id, [firstChild.internal_id]);
    const afterSecond = await markBatchDeliveryChildrenPublished(testContext, root.internal_id, [secondChild.internal_id]);
    const afterReplay = await markBatchDeliveryChildrenPublished(testContext, root.internal_id, [firstChild.internal_id]);
    const replay = await loadBatchDeliveryHandoff(testContext, root.internal_id);

    expect(afterFirst.parentDelivery.handoff_evidence).toBe(BatchDeliveryHandoffEvidence.ChildrenReserved);
    expect(afterFirst.pendingChildren.map((child) => child.internal_id)).toEqual([secondChild.internal_id]);
    expect(afterSecond.parentDelivery.handoff_evidence).toBe(BatchDeliveryHandoffEvidence.ChildrenPublished);
    expect(afterSecond.pendingChildren).toEqual([]);
    expect(afterReplay.parentDelivery.handoff_evidence).toBe(BatchDeliveryHandoffEvidence.ChildrenPublished);
    expect(replay.parentDelivery.handoff_evidence).toBe(BatchDeliveryHandoffEvidence.ChildrenPublished);
    expect(replay.pendingChildren).toEqual([]);
  });
});
