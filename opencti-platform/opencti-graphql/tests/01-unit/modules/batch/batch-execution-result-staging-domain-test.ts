import { beforeEach, describe, expect, it, vi } from 'vitest';
import { elIndex, elLoadById } from '../../../../src/database/engine';
import { lockResources } from '../../../../src/lock/master-lock';
import {
  assertBatchExecutionResultStagingDraft,
  BATCH_EXECUTION_RESULT_STAGING_MAX_SERIALIZED_BYTES,
  buildBatchExecutionResultStagingDraft,
  buildBatchExecutionResultStagingId,
  buildBatchExecutionResultStagingLockId,
  loadBatchExecutionResultStaging,
  readBatchExecutionResultStagingPayload,
  reserveBatchExecutionResultStaging,
} from '../../../../src/modules/batch/batch-execution-result-staging-domain';
import { BatchSideEffectKind } from '../../../../src/modules/batch/batch-executor';
import {
  type BatchExecutionReceipt,
  type BatchExecutionResultStaging,
  BatchExecutionMode,
  BatchWaitUntil,
} from '../../../../src/modules/batch/batch-types';
import { hashSHA256 } from '../../../../src/utils/hash';
import { testContext } from '../../../utils/testQuery';

vi.mock('../../../../src/database/engine', () => ({
  elIndex: vi.fn(),
  elLoadById: vi.fn(),
}));

vi.mock('../../../../src/lock/master-lock', () => ({
  lockResources: vi.fn(),
}));

const receipt = {
  internal_id: 'batch-execution-receipt--staging-test',
  delivery_id: 'batch-delivery--staging-test',
  submission_id: 'batch-submission--staging-test',
  request_fingerprint: 'a'.repeat(64),
  request_contract_version: 1,
} as Pick<
  BatchExecutionReceipt,
  'internal_id' | 'delivery_id' | 'submission_id' | 'request_fingerprint' | 'request_contract_version'
>;

const result = {
  operationCount: 3,
  operationErrors: [{
    code: 'MISSING_REFERENCE_ERROR',
    message: 'missing reference',
    objectId: 'indicator--1',
    operationIndex: 1,
    retryable: true,
  }, {
    message: 'dependency failed',
    operationIndex: 2,
    retryable: false,
  }],
  executionMode: BatchExecutionMode.Bulk,
  waitUntil: BatchWaitUntil.Committed,
  sideEffectKinds: [
    BatchSideEffectKind.StreamPublication,
    BatchSideEffectKind.AutoEnrichment,
  ],
};

describe('batch execution result staging domain', () => {
  let stagingRows: Map<string, any>;

  beforeEach(() => {
    vi.clearAllMocks();
    stagingRows = new Map();
    vi.mocked(lockResources).mockResolvedValue({ unlock: vi.fn() } as any);
    vi.mocked(elLoadById).mockImplementation(async (_context, _user, id) => stagingRows.get(id) ?? undefined);
    vi.mocked(elIndex).mockImplementation(async (_index, document) => {
      stagingRows.set(document.internal_id, document as BatchExecutionResultStaging);
      return document;
    });
  });

  it('builds deterministic identity, fingerprint, byte count, and staged payload for equivalent sealed snapshots', () => {
    const first = buildBatchExecutionResultStagingDraft({ receipt, result });
    const second = buildBatchExecutionResultStagingDraft({
      receipt,
      result: {
        sideEffectKinds: [
          BatchSideEffectKind.StreamPublication,
          BatchSideEffectKind.AutoEnrichment,
        ],
        waitUntil: BatchWaitUntil.Committed,
        executionMode: BatchExecutionMode.Bulk,
        operationErrors: [{
          retryable: true,
          operationIndex: 1,
          objectId: 'indicator--1',
          message: 'missing reference',
          code: 'MISSING_REFERENCE_ERROR',
        }, {
          retryable: false,
          operationIndex: 2,
          message: 'dependency failed',
        }],
        operationCount: 3,
      },
    });

    expect(buildBatchExecutionResultStagingId(receipt.delivery_id))
      .toBe(hashSHA256(`batch-execution-result-staging:${receipt.delivery_id}`));
    expect(buildBatchExecutionResultStagingLockId(receipt.delivery_id))
      .toBe(`batch-execution-result-staging:${receipt.delivery_id}`);
    expect(first.staging_id).toBe(second.staging_id);
    expect(first.staging_fingerprint).toBe(second.staging_fingerprint);
    expect(first.serialized_bytes).toBe(second.serialized_bytes);
    expect(first.operation_errors).toEqual(second.operation_errors);
  });

  it('reserves one immutable row, reads back only staged payload data, and conflicts on changed immutable data', async () => {
    const draft = buildBatchExecutionResultStagingDraft({ receipt, result });
    const first = await reserveBatchExecutionResultStaging(testContext, draft);
    const replay = await reserveBatchExecutionResultStaging(testContext, draft);
    const readback = await loadBatchExecutionResultStaging(testContext, receipt.delivery_id);

    expect(first).toMatchObject({
      internal_id: draft.staging_id,
      staging_id: draft.staging_id,
      receipt_id: receipt.internal_id,
      delivery_id: receipt.delivery_id,
      submission_id: receipt.submission_id,
      result_version: 1,
    });
    expect(replay).toStrictEqual(first);
    expect(readback).toStrictEqual(first);
    expect(readBatchExecutionResultStagingPayload(first)).toEqual({
      operationCount: 3,
      operationErrors: result.operationErrors,
      executionMode: BatchExecutionMode.Bulk,
      waitUntil: BatchWaitUntil.Committed,
      sideEffectKinds: [
        BatchSideEffectKind.StreamPublication,
        BatchSideEffectKind.AutoEnrichment,
      ],
    });
    expect(readBatchExecutionResultStagingPayload(first)).not.toHaveProperty('materialized');
    expect(first).not.toHaveProperty('materialized');
    expect(lockResources).toHaveBeenCalledWith([buildBatchExecutionResultStagingLockId(receipt.delivery_id)]);
    expect(elIndex).toHaveBeenCalledTimes(1);

    await expect(reserveBatchExecutionResultStaging(testContext, buildBatchExecutionResultStagingDraft({
      receipt: {
        ...receipt,
        submission_id: 'batch-submission--changed',
      },
      result,
    }))).rejects.toThrowError('Batch execution result staging is already associated with different immutable data');

    await expect(reserveBatchExecutionResultStaging(testContext, buildBatchExecutionResultStagingDraft({
      receipt,
      result: {
        ...result,
        sideEffectKinds: [BatchSideEffectKind.StreamPublication],
      },
    }))).rejects.toThrowError('Batch execution result staging is already associated with different immutable data');
    expect(elIndex).toHaveBeenCalledTimes(1);
  });

  it('accepts only normal engine metadata during strict readback', async () => {
    const staging = await reserveBatchExecutionResultStaging(testContext, buildBatchExecutionResultStagingDraft({ receipt, result }));
    stagingRows.set(staging.internal_id, {
      ...staging,
      _id: staging.internal_id,
      _index: 'internal_objects',
      sort: [staging.internal_id],
    } as BatchExecutionResultStaging);

    await expect(loadBatchExecutionResultStaging(testContext, receipt.delivery_id))
      .resolves.toMatchObject({
        internal_id: staging.internal_id,
        staging_id: staging.staging_id,
        _id: staging.internal_id,
      });
  });

  it('fails readback closed when the durable row grows authority, history, or raw error fields outside the staging contract', async () => {
    const staging = await reserveBatchExecutionResultStaging(testContext, buildBatchExecutionResultStagingDraft({ receipt, result }));
    stagingRows.set(staging.internal_id, {
      ...staging,
      materialized: true,
      raw_graphql_error_tree: [{ message: 'raw' }],
      stack_trace: 'stack',
      work_state: 'processed',
      http_state: 200,
      rabbitmq_ack_state: 'acked',
      closure: 'opaque',
      event_history: [],
      retry_history: [],
    } as unknown as BatchExecutionResultStaging);

    await expect(loadBatchExecutionResultStaging(testContext, receipt.delivery_id))
      .rejects.toThrowError('Batch execution result staging payload is malformed');
  });

  it('fails closed for invalid enum values, malformed error objects, unsorted errors, and too many errors', () => {
    expect(() => buildBatchExecutionResultStagingDraft({
      receipt,
      result: {
        ...result,
        materialized: true,
      } as any,
    })).toThrowError('Batch execution result staging input payload is malformed');

    expect(() => buildBatchExecutionResultStagingDraft({
      receipt,
      result: {
        ...result,
        executionMode: 'UNKNOWN_MODE' as BatchExecutionMode,
      },
    })).toThrowError('Invalid batch execution result staging execution mode');

    expect(() => buildBatchExecutionResultStagingDraft({
      receipt,
      result: {
        ...result,
        waitUntil: 'UNKNOWN_WAIT' as BatchWaitUntil,
      },
    })).toThrowError('Invalid batch execution result staging wait_until value');

    expect(() => buildBatchExecutionResultStagingDraft({
      receipt,
      result: {
        ...result,
        sideEffectKinds: ['UNKNOWN_EFFECT'],
      },
    })).toThrowError('Batch execution result staging side effect kind is invalid');

    expect(() => buildBatchExecutionResultStagingDraft({
      receipt,
      result: {
        ...result,
        operationErrors: [{
          ...result.operationErrors[0],
          extensions: { code: 'RAW_GRAPHQL_TREE' },
        } as any],
      },
    })).toThrowError('Batch execution result staging operation error payload is malformed');

    expect(() => buildBatchExecutionResultStagingDraft({
      receipt,
      result: {
        ...result,
        operationErrors: [
          { message: 'later', operationIndex: 2, retryable: false },
          { message: 'earlier', operationIndex: 1, retryable: false },
        ],
      },
    })).toThrowError('Batch execution result staging operation errors are not ordered by operation index');

    expect(() => buildBatchExecutionResultStagingDraft({
      receipt,
      result: {
        ...result,
        operationCount: 1,
        operationErrors: [
          { message: 'one', operationIndex: 0, retryable: false },
          { message: 'two', operationIndex: 0, retryable: false },
        ],
      },
    })).toThrowError('Batch execution result staging operation error count exceeds the operation count');
  });

  it('fails closed for sparse staged arrays instead of fingerprinting holes', () => {
    const sparseOperationErrors = Array(2) as any[];
    sparseOperationErrors[1] = {
      message: 'missing first error slot',
      operationIndex: 1,
      retryable: false,
    };
    expect(() => buildBatchExecutionResultStagingDraft({
      receipt,
      result: {
        ...result,
        operationErrors: sparseOperationErrors,
      },
    })).toThrowError('Batch execution result staging operation errors must be a dense array');

    const sparseSideEffectKinds = Array(1) as string[];
    expect(() => buildBatchExecutionResultStagingDraft({
      receipt,
      result: {
        ...result,
        sideEffectKinds: sparseSideEffectKinds,
      },
    })).toThrowError('Batch execution result staging side effect kinds must be a dense array');
  });

  it('fails readback closed when persisted fingerprints or byte counts no longer verify', async () => {
    const staging = await reserveBatchExecutionResultStaging(testContext, buildBatchExecutionResultStagingDraft({ receipt, result }));

    stagingRows.set(staging.internal_id, {
      ...staging,
      staging_fingerprint: 'b'.repeat(64),
    } as BatchExecutionResultStaging);
    await expect(loadBatchExecutionResultStaging(testContext, receipt.delivery_id))
      .rejects.toThrowError('Batch execution result staging fingerprint changed after persistence');

    stagingRows.set(staging.internal_id, {
      ...staging,
      serialized_bytes: staging.serialized_bytes + 1,
    } as BatchExecutionResultStaging);
    await expect(loadBatchExecutionResultStaging(testContext, receipt.delivery_id))
      .rejects.toThrowError('Batch execution result staging serialized byte count changed after persistence');
  });

  it('enforces the 1 MiB canonical payload cap before reservation', () => {
    expect(() => buildBatchExecutionResultStagingDraft({
      receipt,
      result: {
        operationCount: 1,
        operationErrors: [{
          message: 'x'.repeat(BATCH_EXECUTION_RESULT_STAGING_MAX_SERIALIZED_BYTES),
          operationIndex: 0,
          retryable: false,
        }],
        executionMode: BatchExecutionMode.Bulk,
        waitUntil: BatchWaitUntil.Committed,
        sideEffectKinds: [BatchSideEffectKind.StreamPublication],
      },
    })).toThrowError('Batch execution result staging serialized byte cap exceeded');
    expect(elIndex).not.toHaveBeenCalled();
  });

  it('rejects non-canonical draft changes before durable reservation', () => {
    const draft = buildBatchExecutionResultStagingDraft({ receipt, result });

    expect(() => assertBatchExecutionResultStagingDraft({
      ...draft,
      serialized_bytes: draft.serialized_bytes + 1,
    })).toThrowError('Batch execution result staging serialized byte count changed after persistence');

    expect(() => assertBatchExecutionResultStagingDraft({
      ...draft,
      staging_fingerprint: 'c'.repeat(64),
    })).toThrowError('Batch execution result staging fingerprint changed after persistence');
  });
});
