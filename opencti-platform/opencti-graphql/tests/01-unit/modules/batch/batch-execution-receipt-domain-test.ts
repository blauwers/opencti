import { beforeEach, describe, expect, it, vi } from 'vitest';
import {
  assertBatchExecutionReceiptReservation,
  buildBatchExecutionReceiptId,
  buildBatchExecutionReceiptRequestMetadata,
  readBatchExecutionReceiptResultMetadata,
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
  BatchWaitUntil,
  type BatchDelivery,
} from '../../../../src/modules/batch/batch-types';
import { elIndex, elLoadById, elUpdate } from '../../../../src/database/engine';
import { testContext } from '../../../utils/testQuery';

vi.mock('../../../../src/database/engine', () => ({
  elIndex: vi.fn(),
  elLoadById: vi.fn(),
  elUpdate: vi.fn(),
}));

const delivery = {
  id: 'batch-delivery--1',
  internal_id: 'batch-delivery--1',
  standard_id: 'batch-delivery--1',
  entity_type: 'BatchDelivery',
  base_type: 'ENTITY',
  parent_types: ['Basic-Object', 'Internal-Object'],
  submission_id: 'batch-submission--1',
  parent_delivery_id: null,
  delivery_kind: BatchDeliveryKind.Root,
  branch_kind: BatchDeliveryBranchKind.Root,
  branch_sequence: 0,
  branch_ordinal: 0,
  payload_fingerprint: 'payload-fingerprint-1',
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
  batchPlan: {
    version: 1,
    executionPhases: [{ phase: 0, objectIds: ['indicator--1'] }],
  },
  operations: [{
    query: 'mutation Record($value: String!) {\n  record(value: $value)\n}',
    variables: { value: 'one' },
    operationName: 'Record',
    objectId: 'indicator--1',
    executionGroup: 0,
    executionPhase: 0,
    files: null,
  }],
});

const reserveInput = {
  deliveryId: delivery.internal_id,
  submissionId: delivery.submission_id,
  deliveryPayloadFingerprint: delivery.payload_fingerprint,
  executionMode: BatchExecutionMode.Bulk,
  waitUntil: BatchWaitUntil.Materialized,
  requestMetadata,
};

describe('batch execution receipt domain', () => {
  let receipts: Map<string, any>;

  beforeEach(() => {
    vi.clearAllMocks();
    receipts = new Map();
    vi.mocked(elLoadById).mockImplementation(async (_context, _user, id) => receipts.get(id) ?? null);
    vi.mocked(elIndex).mockImplementation(async (_index, document) => {
      receipts.set(document.internal_id, document);
      return document;
    });
    vi.mocked(elUpdate).mockImplementation(async (_context, _index, id, update) => {
      const current = receipts.get(id);
      const next = {
        ...current,
        ...(update as any).doc,
      };
      receipts.set(id, next);
      return next;
    });
  });

  it('builds one deterministic receipt id and request fingerprint for equivalent normalized inputs', () => {
    const equivalentMetadata = buildBatchExecutionReceiptRequestMetadata({
      delivery,
      executionMode: BatchExecutionMode.Bulk,
      waitUntil: BatchWaitUntil.Materialized,
      batchPlan: {
        executionPhases: [{ objectIds: ['indicator--1'], phase: 0 }],
        version: 1,
      },
      operations: [{
        query: 'mutation Record($value: String!) {\n  record(value: $value)\n}',
        variables: { value: 'one' },
        operationName: 'Record',
        objectId: 'indicator--1',
        executionGroup: 0,
        executionPhase: 0,
        files: null,
      }],
    });

    expect(buildBatchExecutionReceiptId(delivery.internal_id)).toBe(buildBatchExecutionReceiptId(delivery.internal_id));
    expect(requestMetadata.requestFingerprint).toBe(equivalentMetadata.requestFingerprint);
    expect(requestMetadata.operationManifestFingerprint).toBe(equivalentMetadata.operationManifestFingerprint);
    expect(requestMetadata.batchPlanFingerprint).toBe(equivalentMetadata.batchPlanFingerprint);
  });

  it('reuses one PREPARED receipt identity for the same leaf request and rejects fingerprint conflicts', async () => {
    const first = await reserveBatchExecutionReceipt(testContext, reserveInput);
    const replay = await reserveBatchExecutionReceipt(testContext, reserveInput);

    expect(first).toMatchObject({
      internal_id: buildBatchExecutionReceiptId(delivery.internal_id),
      delivery_id: delivery.internal_id,
      submission_id: delivery.submission_id,
      state: BatchExecutionReceiptState.Prepared,
    });
    expect(replay).toBe(first);
    expect(elIndex).toHaveBeenCalledTimes(1);

    expect(() => assertBatchExecutionReceiptReservation(first, {
      ...reserveInput,
      requestMetadata: {
        ...requestMetadata,
        requestFingerprint: 'different-request-fingerprint',
      },
    })).toThrowError(expect.objectContaining({
      extensions: expect.objectContaining({
        data: expect.objectContaining({
          batch_error_code: BatchAdmissionErrorCode.ExecutionReceiptConflict,
        }),
      }),
    }));
  });

  it('keeps PREPARED retryable, then makes STARTED fail closed until terminal evidence exists', async () => {
    const prepared = await reserveBatchExecutionReceipt(testContext, reserveInput);
    const preparedReplay = await reserveBatchExecutionReceipt(testContext, reserveInput);
    const started = await recordBatchExecutionReceiptStarted(testContext, preparedReplay);

    expect(prepared.state).toBe(BatchExecutionReceiptState.Prepared);
    expect(preparedReplay.state).toBe(BatchExecutionReceiptState.Prepared);
    expect(started.state).toBe(BatchExecutionReceiptState.Started);

    const reconciliation = await recordBatchExecutionReceiptRequiresReconciliation(
      testContext,
      started,
      new Error('response lost after execution started'),
    );

    expect(reconciliation).toMatchObject({
      state: BatchExecutionReceiptState.RequiresReconciliation,
      last_error: 'response lost after execution started',
    });
    await expect(recordBatchExecutionReceiptStarted(testContext, reconciliation))
      .rejects.toThrowError(expect.objectContaining({
        extensions: expect.objectContaining({
          data: expect.objectContaining({
            batch_error_code: BatchAdmissionErrorCode.ExecutionReceiptConflict,
          }),
        }),
      }));
  });

  it('persists materialized terminal result metadata and reads it back for replay', async () => {
    const prepared = await reserveBatchExecutionReceipt(testContext, reserveInput);
    const started = await recordBatchExecutionReceiptStarted(testContext, prepared);
    const completed = await recordBatchExecutionReceiptCompletion(testContext, started, {
      operationCount: 1,
      operationErrors: [{
        code: 'MISSING_REFERENCE_ERROR',
        message: 'missing reference',
        objectId: 'indicator--1',
        operationIndex: 0,
        retryable: true,
      }],
      executionMode: BatchExecutionMode.Bulk,
      waitUntil: BatchWaitUntil.Materialized,
      sideEffectKinds: ['STREAM_PUBLICATION', 'STREAM_PUBLICATION'],
      materialized: true,
    });

    expect(completed).toMatchObject({
      state: BatchExecutionReceiptState.Completed,
      result_operation_count: 1,
      result_execution_mode: BatchExecutionMode.Bulk,
      result_wait_until: BatchWaitUntil.Materialized,
      result_materialized: true,
      completion_boundary: 'MATERIALIZED',
    });
    expect(readBatchExecutionReceiptResultMetadata(completed)).toEqual({
      operationCount: 1,
      operationErrors: [{
        code: 'MISSING_REFERENCE_ERROR',
        message: 'missing reference',
        objectId: 'indicator--1',
        operationIndex: 0,
        retryable: true,
      }],
      executionMode: BatchExecutionMode.Bulk,
      waitUntil: BatchWaitUntil.Materialized,
      sideEffectKinds: ['STREAM_PUBLICATION', 'STREAM_PUBLICATION'],
      materialized: true,
    });
    await expect(recordBatchExecutionReceiptCompletion(testContext, completed, {
      operationCount: 1,
      operationErrors: [],
      executionMode: BatchExecutionMode.Bulk,
      waitUntil: BatchWaitUntil.Materialized,
      sideEffectKinds: [],
      materialized: true,
    })).rejects.toThrowError(expect.objectContaining({
      extensions: expect.objectContaining({
        data: expect.objectContaining({
          batch_error_code: BatchAdmissionErrorCode.ExecutionReceiptConflict,
        }),
      }),
    }));
  });

  it('records explicit pre-start terminal failure evidence and never treats non-materialized output as completion', async () => {
    const prepared = await reserveBatchExecutionReceipt(testContext, reserveInput);
    const failed = await recordBatchExecutionReceiptTerminalFailure(testContext, prepared, {
      stage: 'BUILD_EXECUTION_PLAN',
      code: 'FUNCTIONAL_ERROR',
      message: 'invalid execution plan',
      proof: BatchExecutionReceiptFailureProof.PreStartValidation,
    });

    expect(failed).toMatchObject({
      state: BatchExecutionReceiptState.FailedTerminal,
      failure_stage: 'BUILD_EXECUTION_PLAN',
      failure_code: 'FUNCTIONAL_ERROR',
      failure_message: 'invalid execution plan',
      failure_retryable: false,
      failure_proof: BatchExecutionReceiptFailureProof.PreStartValidation,
    });

    const secondPrepared = await reserveBatchExecutionReceipt(testContext, {
      ...reserveInput,
      deliveryId: 'batch-delivery--2',
      submissionId: 'batch-submission--2',
      deliveryPayloadFingerprint: 'payload-fingerprint-2',
      requestMetadata: {
        ...requestMetadata,
        requestFingerprint: 'request-fingerprint-2',
      },
    });
    const secondStarted = await recordBatchExecutionReceiptStarted(testContext, secondPrepared);
    await expect(recordBatchExecutionReceiptCompletion(testContext, secondStarted, {
      operationCount: 1,
      operationErrors: [],
      executionMode: BatchExecutionMode.Bulk,
      waitUntil: BatchWaitUntil.Committed,
      sideEffectKinds: ['AUTO_ENRICHMENT'],
      materialized: false as true,
    })).rejects.toThrowError(expect.objectContaining({
      extensions: expect.objectContaining({
        data: expect.objectContaining({
          batch_error_code: BatchAdmissionErrorCode.ExecutionReceiptConflict,
        }),
      }),
    }));
  });
});
