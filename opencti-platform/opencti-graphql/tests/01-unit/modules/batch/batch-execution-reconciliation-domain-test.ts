import { beforeEach, describe, expect, it, vi } from 'vitest';
import { elIndex, elLoadById, elUpdate } from '../../../../src/database/engine';
import { lockResources } from '../../../../src/lock/master-lock';
import {
  assertBatchExecutionReconciliationReservation,
  assertBatchExecutionReconciliationTransition,
  buildBatchExecutionReconciliationId,
  buildBatchExecutionReconciliationLockId,
  ensureBatchExecutionReconciliationForRequiresReconciliation,
  openBatchExecutionReconciliation,
  recordBatchExecutionReconciliationResolvedCompleted,
  recordBatchExecutionReconciliationResolvedFailedTerminal,
  reserveBatchExecutionReconciliation,
} from '../../../../src/modules/batch/batch-execution-reconciliation-domain';
import {
  buildBatchExecutionReceiptRequestMetadata,
  recordBatchExecutionReceiptCompletion,
  recordBatchExecutionReceiptRequiresReconciliation,
  recordBatchExecutionReceiptStarted,
  recordBatchExecutionReceiptTerminalFailure,
  reserveBatchExecutionReceipt,
} from '../../../../src/modules/batch/batch-execution-receipt-domain';
import {
  BatchAdmissionErrorCode,
  BatchDeliveryBranchKind,
  BatchDeliveryHandoffEvidence,
  BatchDeliveryKind,
  BatchDeliveryProtocol,
  BatchDeliveryState,
  BatchExecutionMode,
  BatchExecutionReceiptFailureProof,
  BatchExecutionReceiptState,
  BatchExecutionReconciliationEvidenceClass,
  BatchExecutionReconciliationOpenedReason,
  BatchExecutionReconciliationState,
  BatchWaitUntil,
  type BatchDelivery,
  type BatchExecutionReceipt,
  type BatchExecutionReceiptResultMetadata,
} from '../../../../src/modules/batch/batch-types';
import { testContext } from '../../../utils/testQuery';

vi.mock('../../../../src/database/engine', () => ({
  elIndex: vi.fn(),
  elLoadById: vi.fn(),
  elUpdate: vi.fn(),
}));

vi.mock('../../../../src/lock/master-lock', () => ({
  lockResources: vi.fn(),
}));

const delivery = {
  id: 'batch-delivery--reconciliation-test',
  internal_id: 'batch-delivery--reconciliation-test',
  standard_id: 'batch-delivery--reconciliation-test',
  entity_type: 'BatchDelivery',
  base_type: 'ENTITY',
  parent_types: ['Basic-Object', 'Internal-Object'],
  submission_id: 'batch-submission--reconciliation-test',
  parent_delivery_id: null,
  delivery_kind: BatchDeliveryKind.Root,
  branch_kind: BatchDeliveryBranchKind.Root,
  branch_sequence: 0,
  branch_ordinal: 0,
  payload_fingerprint: 'payload-fingerprint-reconciliation-test',
  queue_payload_version: 1,
  queue_payload: '{}',
  required_worker_protocol: BatchDeliveryProtocol.V2,
  state: BatchDeliveryState.Published,
  handoff_evidence: BatchDeliveryHandoffEvidence.None,
  child_set_fingerprint: null,
  child_count: 0,
  child_delivery_ids: [],
  created_at: '2026-08-08T00:00:00.000Z',
  updated_at: '2026-08-08T00:00:00.000Z',
  published_at: '2026-08-08T00:00:00.000Z',
  children_reserved_at: null,
  children_published_at: null,
  last_error: null,
} as BatchDelivery;

const requestMetadata = buildBatchExecutionReceiptRequestMetadata({
  delivery,
  executionMode: BatchExecutionMode.Bulk,
  waitUntil: BatchWaitUntil.Materialized,
  batchPlan: null,
  operations: [{
    query: 'mutation Record($value: String!) {\n  record(value: $value)\n}',
    variables: { value: 'one' },
    operationName: 'Record',
    objectId: 'indicator--1',
    executionGroup: null,
    executionPhase: null,
    files: null,
  }],
});

const reserveReceiptInput = {
  deliveryId: delivery.internal_id,
  submissionId: delivery.submission_id,
  deliveryPayloadFingerprint: delivery.payload_fingerprint,
  executionMode: BatchExecutionMode.Bulk,
  waitUntil: BatchWaitUntil.Materialized,
  requestMetadata,
};

const resultMetadata: BatchExecutionReceiptResultMetadata = {
  operationCount: 1,
  operationErrors: [],
  executionMode: BatchExecutionMode.Bulk,
  waitUntil: BatchWaitUntil.Materialized,
  sideEffectKinds: [],
  materialized: true,
};

describe('batch execution reconciliation domain', () => {
  let records: Map<string, any>;

  beforeEach(() => {
    vi.clearAllMocks();
    records = new Map();
    vi.mocked(lockResources).mockResolvedValue({ unlock: vi.fn() } as any);
    vi.mocked(elLoadById).mockImplementation(async (_context, _user, id) => records.get(id) ?? null);
    vi.mocked(elIndex).mockImplementation(async (_index, document) => {
      records.set(document.internal_id, document);
      return document;
    });
    vi.mocked(elUpdate).mockImplementation(async (_context, _index, id, update) => {
      const current = records.get(id);
      const next = {
        ...current,
        ...(update as any).doc,
      };
      records.set(id, next);
      return next;
    });
  });

  const createStartedReceipt = async (): Promise<BatchExecutionReceipt> => {
    const prepared = await reserveBatchExecutionReceipt(testContext, reserveReceiptInput);
    return recordBatchExecutionReceiptStarted(testContext, prepared);
  };

  it('builds one deterministic reconciliation identity and opens a STARTED receipt without mutating it', async () => {
    const started = await createStartedReceipt();
    const reconciliation = await openBatchExecutionReconciliation(testContext, started.delivery_id);
    const replay = await openBatchExecutionReconciliation(testContext, started.delivery_id);

    expect(buildBatchExecutionReconciliationId(started.delivery_id)).toBe(buildBatchExecutionReconciliationId(started.delivery_id));
    expect(buildBatchExecutionReconciliationLockId(started.delivery_id)).toBe(`batch-execution-reconciliation:${started.delivery_id}`);
    expect(reconciliation).toMatchObject({
      internal_id: buildBatchExecutionReconciliationId(started.delivery_id),
      receipt_id: started.internal_id,
      delivery_id: started.delivery_id,
      submission_id: started.submission_id,
      request_fingerprint: started.request_fingerprint,
      opened_from_receipt_state: BatchExecutionReceiptState.Started,
      opened_reason: BatchExecutionReconciliationOpenedReason.ExplicitStartedReceipt,
      state: BatchExecutionReconciliationState.Open,
    });
    expect(replay).toBe(reconciliation);
    expect(records.get(started.internal_id)).toMatchObject({
      state: BatchExecutionReceiptState.Started,
      reconciliation_required_at: null,
    });
  });

  it('opens an existing REQUIRES_RECONCILIATION receipt as OPEN without mutating the receipt', async () => {
    const started = await createStartedReceipt();
    const requiresReconciliation = await recordBatchExecutionReceiptRequiresReconciliation(
      testContext,
      started,
      new Error('post-start response lost'),
    );

    const reconciliation = await openBatchExecutionReconciliation(testContext, requiresReconciliation.delivery_id);

    expect(reconciliation).toMatchObject({
      receipt_id: requiresReconciliation.internal_id,
      opened_from_receipt_state: BatchExecutionReceiptState.RequiresReconciliation,
      opened_reason: BatchExecutionReconciliationOpenedReason.ExplicitRequiresReconciliationReceipt,
      state: BatchExecutionReconciliationState.Open,
    });
    expect(records.get(requiresReconciliation.internal_id)).toMatchObject({
      state: BatchExecutionReceiptState.RequiresReconciliation,
      last_error: 'post-start response lost',
    });
  });

  it('rejects changed receipt identity before any reconciliation transition', async () => {
    const started = await createStartedReceipt();
    const reconciliation = await reserveBatchExecutionReconciliation(testContext, {
      receipt: started,
      openedReason: BatchExecutionReconciliationOpenedReason.ExplicitStartedReceipt,
    });

    expect(() => assertBatchExecutionReconciliationReservation(reconciliation, {
      receipt: {
        ...started,
        request_fingerprint: 'different-request-fingerprint',
      },
      openedReason: BatchExecutionReconciliationOpenedReason.ExplicitStartedReceipt,
    })).toThrowError(expect.objectContaining({
      extensions: expect.objectContaining({
        data: expect.objectContaining({
          batch_error_code: BatchAdmissionErrorCode.ExecutionReconciliationConflict,
        }),
      }),
    }));
  });

  it('advances OPEN to AMBIGUOUS once and keeps resolved states immutable', async () => {
    const started = await createStartedReceipt();
    const open = await openBatchExecutionReconciliation(testContext, started.delivery_id);
    const requiresReconciliation = await recordBatchExecutionReceiptRequiresReconciliation(
      testContext,
      started,
      new Error('post-start response lost'),
    );
    const ambiguous = await ensureBatchExecutionReconciliationForRequiresReconciliation(
      testContext,
      requiresReconciliation,
      BatchExecutionReconciliationOpenedReason.PostStartError,
      new Error('post-start response lost'),
    );
    const completed = await recordBatchExecutionReceiptCompletion(testContext, requiresReconciliation, resultMetadata);
    const resolved = await recordBatchExecutionReconciliationResolvedCompleted(testContext, ambiguous, {
      evidenceClass: BatchExecutionReconciliationEvidenceClass.ExistingTerminalReceipt,
      receipt: completed,
    });

    expect(open.state).toBe(BatchExecutionReconciliationState.Open);
    expect(ambiguous).toMatchObject({
      internal_id: open.internal_id,
      opened_reason: BatchExecutionReconciliationOpenedReason.ExplicitStartedReceipt,
      state: BatchExecutionReconciliationState.Ambiguous,
      last_error: 'post-start response lost',
    });
    expect(resolved).toMatchObject({
      state: BatchExecutionReconciliationState.ResolvedCompleted,
      evidence_class: BatchExecutionReconciliationEvidenceClass.ExistingTerminalReceipt,
      resolved_receipt_state: BatchExecutionReceiptState.Completed,
    });
    expect(() => assertBatchExecutionReconciliationTransition(resolved, BatchExecutionReconciliationState.Ambiguous))
      .toThrowError(expect.objectContaining({
        extensions: expect.objectContaining({
          data: expect.objectContaining({
            batch_error_code: BatchAdmissionErrorCode.ExecutionReconciliationConflict,
          }),
        }),
      }));
    await expect(recordBatchExecutionReconciliationResolvedCompleted(testContext, resolved, {
      evidenceClass: BatchExecutionReconciliationEvidenceClass.ExistingTerminalReceipt,
      receipt: completed,
    })).resolves.toStrictEqual(resolved);
  });

  it('records no-effect terminal receipt evidence and rejects later terminal rewrites', async () => {
    const started = await createStartedReceipt();
    const open = await openBatchExecutionReconciliation(testContext, started.delivery_id);
    const failed = await recordBatchExecutionReceiptTerminalFailure(testContext, started, {
      stage: 'EXECUTION',
      code: 'NO_EFFECT',
      message: 'no effect escaped',
      proof: BatchExecutionReceiptFailureProof.NoEffectTerminal,
    });
    const resolved = await recordBatchExecutionReconciliationResolvedFailedTerminal(testContext, open, {
      evidenceClass: BatchExecutionReconciliationEvidenceClass.ExistingTerminalReceipt,
      receipt: failed,
    });

    expect(resolved).toMatchObject({
      state: BatchExecutionReconciliationState.ResolvedFailedTerminal,
      evidence_class: BatchExecutionReconciliationEvidenceClass.ExistingTerminalReceipt,
      resolved_receipt_state: BatchExecutionReceiptState.FailedTerminal,
    });
    expect(() => assertBatchExecutionReconciliationTransition(resolved, BatchExecutionReconciliationState.ResolvedCompleted))
      .toThrowError(expect.objectContaining({
        extensions: expect.objectContaining({
          data: expect.objectContaining({
            batch_error_code: BatchAdmissionErrorCode.ExecutionReconciliationConflict,
          }),
        }),
      }));
  });

  it('rejects inadmissible terminal evidence and incomplete future completion evidence', async () => {
    const started = await createStartedReceipt();
    const open = await openBatchExecutionReconciliation(testContext, started.delivery_id);
    const requiresReconciliation = await recordBatchExecutionReceiptRequiresReconciliation(
      testContext,
      started,
      new Error('post-start response lost'),
    );
    const ambiguous = await ensureBatchExecutionReconciliationForRequiresReconciliation(
      testContext,
      requiresReconciliation,
      BatchExecutionReconciliationOpenedReason.PostStartError,
      new Error('post-start response lost'),
    );

    for (const evidenceClass of [
      'WORK_PROGRESS',
      'FINAL_HTTP_RESPONSE_STATE',
      'RABBITMQ_ACK_STATE',
      'ELAPSED_TIME_ONLY',
      'NON_MATERIALIZED_COMMIT',
    ]) {
      await expect(recordBatchExecutionReconciliationResolvedCompleted(testContext, ambiguous, {
        evidenceClass,
        receipt: requiresReconciliation,
        materialized: true,
      })).rejects.toThrowError(expect.objectContaining({
        extensions: expect.objectContaining({
          data: expect.objectContaining({
            batch_error_code: BatchAdmissionErrorCode.ExecutionReconciliationConflict,
          }),
        }),
      }));
    }

    await expect(recordBatchExecutionReconciliationResolvedCompleted(testContext, ambiguous, {
      evidenceClass: BatchExecutionReconciliationEvidenceClass.MaterializationTerminal,
      receipt: requiresReconciliation,
      materialized: false,
      requestFingerprint: requiresReconciliation.request_fingerprint,
      resultMetadata,
    })).rejects.toThrowError(expect.objectContaining({
      extensions: expect.objectContaining({
        data: expect.objectContaining({
          batch_error_code: BatchAdmissionErrorCode.ExecutionReconciliationConflict,
        }),
      }),
    }));

    await expect(recordBatchExecutionReconciliationResolvedCompleted(testContext, ambiguous, {
      evidenceClass: BatchExecutionReconciliationEvidenceClass.MaterializationTerminal,
      receipt: requiresReconciliation,
      materialized: true,
      requestFingerprint: requiresReconciliation.request_fingerprint,
      resultMetadata: null,
    })).rejects.toThrowError('Batch execution reconciliation completion requires durable worker-visible result metadata');

    await expect(recordBatchExecutionReconciliationResolvedCompleted(testContext, ambiguous, {
      evidenceClass: BatchExecutionReconciliationEvidenceClass.MaterializationTerminal,
      receipt: requiresReconciliation,
      materialized: true,
      requestFingerprint: 'different-request-fingerprint',
      resultMetadata,
    })).rejects.toThrowError('Batch execution reconciliation completion metadata does not match the receipt request identity');

    expect(open.state).toBe(BatchExecutionReconciliationState.Open);
    expect(records.get(ambiguous.internal_id)).toMatchObject({
      state: BatchExecutionReconciliationState.Ambiguous,
      evidence_class: null,
      resolved_receipt_state: null,
    });
  });

  it('rejects stale terminal rewrites after a different terminal state was already persisted', async () => {
    const started = await createStartedReceipt();
    const open = await openBatchExecutionReconciliation(testContext, started.delivery_id);
    const completed = await recordBatchExecutionReceiptCompletion(testContext, started, resultMetadata);
    const failed = {
      ...started,
      state: BatchExecutionReceiptState.FailedTerminal,
      failure_proof: BatchExecutionReceiptFailureProof.NoEffectTerminal,
      failure_fingerprint: 'failure-fingerprint',
    };

    await recordBatchExecutionReconciliationResolvedCompleted(testContext, open, {
      evidenceClass: BatchExecutionReconciliationEvidenceClass.ExistingTerminalReceipt,
      receipt: completed,
    });

    await expect(recordBatchExecutionReconciliationResolvedFailedTerminal(testContext, open, {
      evidenceClass: BatchExecutionReconciliationEvidenceClass.ExistingTerminalReceipt,
      receipt: failed,
    })).rejects.toThrowError(expect.objectContaining({
      extensions: expect.objectContaining({
        data: expect.objectContaining({
          batch_error_code: BatchAdmissionErrorCode.ExecutionReconciliationConflict,
        }),
      }),
    }));
    expect(records.get(open.internal_id)).toMatchObject({
      state: BatchExecutionReconciliationState.ResolvedCompleted,
      resolved_receipt_state: BatchExecutionReceiptState.Completed,
    });
  });
});
