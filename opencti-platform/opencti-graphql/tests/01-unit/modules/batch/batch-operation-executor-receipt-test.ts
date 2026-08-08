import { makeExecutableSchema } from '@graphql-tools/schema';
import { beforeEach, describe, expect, it, vi } from 'vitest';
import { elIndex, elLoadById, elUpdate } from '../../../../src/database/engine';
import { lockResources } from '../../../../src/lock/master-lock';
import { loadBatchDelivery, readBatchDeliveryQueueMessage } from '../../../../src/modules/batch/batch-delivery-domain';
import { startBatchBackendAttemptObservationRefreshLoop } from '../../../../src/modules/batch/batch-backend-attempt-observation-domain';
import { buildBatchExecutionReconciliationId, openBatchExecutionReconciliation } from '../../../../src/modules/batch/batch-execution-reconciliation-domain';
import {
  buildBatchExecutionReceiptId,
  buildBatchExecutionReceiptRequestMetadata,
  recordBatchExecutionReceiptStarted,
  reserveBatchExecutionReceipt,
} from '../../../../src/modules/batch/batch-execution-receipt-domain';
import {
  BatchSideEffectKind,
  registerBatchSideEffect,
} from '../../../../src/modules/batch/batch-executor';
import { executeBatchGraphqlOperations } from '../../../../src/modules/batch/batch-operation-executor';
import {
  BatchAdmissionErrorCode,
  BatchDeliveryBranchKind,
  BatchDeliveryHandoffEvidence,
  BatchDeliveryKind,
  BatchDeliveryProtocol,
  BatchDeliveryState,
  BatchExecutionMode,
  BatchExecutionReconciliationOpenedReason,
  BatchExecutionReconciliationState,
  BatchExecutionReceiptState,
  BatchWaitUntil,
  type BatchDelivery,
  type BatchDirectDeliveryContext,
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

vi.mock('../../../../src/modules/batch/batch-delivery-domain', () => ({
  loadBatchDelivery: vi.fn(),
  readBatchDeliveryQueueMessage: vi.fn(),
}));

vi.mock('../../../../src/modules/batch/batch-backend-attempt-observation-domain', () => ({
  startBatchBackendAttemptObservationRefreshLoop: vi.fn(),
}));

const query = 'mutation Record($value: String!) {\n  record(value: $value)\n}';
const delivery = {
  id: 'batch-delivery--receipt-test',
  internal_id: 'batch-delivery--receipt-test',
  standard_id: 'batch-delivery--receipt-test',
  entity_type: 'BatchDelivery',
  base_type: 'ENTITY',
  parent_types: ['Basic-Object', 'Internal-Object'],
  submission_id: 'batch-submission--receipt-test',
  parent_delivery_id: null,
  delivery_kind: BatchDeliveryKind.Root,
  branch_kind: BatchDeliveryBranchKind.Root,
  branch_sequence: 0,
  branch_ordinal: 0,
  payload_fingerprint: 'payload-fingerprint-receipt-test',
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
const directDeliveryContext: BatchDirectDeliveryContext = {
  submission_id: delivery.submission_id,
  delivery_id: delivery.internal_id,
  parent_delivery_id: null,
  delivery_kind: BatchDeliveryKind.Root,
  delivery_protocol_version: BatchDeliveryProtocol.V2,
  delivery_branch_kind: BatchDeliveryBranchKind.Root,
  delivery_branch_sequence: 0,
  delivery_branch_ordinal: 0,
};
const operation = {
  query,
  variables: JSON.stringify({ value: 'one' }),
  operationName: 'Record',
  objectId: 'indicator--1',
};

const buildSchema = (
  calls: string[],
  withSideEffect = false,
  failAfterWrite = false,
  openReconciliationDuringWrite = false,
) => makeExecutableSchema({
  typeDefs: `
    type Query {
      status: String!
    }

    type Mutation {
      record(value: String!): String!
    }
  `,
  resolvers: {
    Query: {
      status: () => 'ok',
    },
    Mutation: {
      record: async (_: unknown, { value }: { value: string }) => {
        calls.push(`write:${value}`);
        if (openReconciliationDuringWrite) {
          await openBatchExecutionReconciliation(testContext, delivery.internal_id);
        }
        if (withSideEffect) {
          await registerBatchSideEffect({
            kind: BatchSideEffectKind.StreamPublication,
            execute: async () => {
              calls.push(`side-effect:${value}`);
            },
          });
        }
        if (failAfterWrite) {
          throw new Error('resolver failed after write');
        }
        return value;
      },
    },
  },
});

const buildPreparedReceiptInput = () => ({
  deliveryId: delivery.internal_id,
  submissionId: delivery.submission_id,
  deliveryPayloadFingerprint: delivery.payload_fingerprint,
  executionMode: BatchExecutionMode.Bulk,
  waitUntil: BatchWaitUntil.Materialized,
  requestMetadata: buildBatchExecutionReceiptRequestMetadata({
    delivery,
    executionMode: BatchExecutionMode.Bulk,
    waitUntil: BatchWaitUntil.Materialized,
    batchPlan: null,
    operations: [{
      query,
      variables: { value: 'one' },
      operationName: 'Record',
      objectId: 'indicator--1',
      executionGroup: null,
      executionPhase: null,
      files: null,
    }],
  }),
});

describe('batch GraphQL execution receipt boundary', () => {
  let receipts: Map<string, any>;
  let stopAttemptObservationRefreshLoop: ReturnType<typeof vi.fn>;

  beforeEach(() => {
    vi.clearAllMocks();
    receipts = new Map();
    stopAttemptObservationRefreshLoop = vi.fn().mockResolvedValue(undefined);
    vi.mocked(lockResources).mockResolvedValue({ unlock: vi.fn() } as any);
    vi.mocked(loadBatchDelivery).mockResolvedValue(delivery);
    vi.mocked(readBatchDeliveryQueueMessage).mockReturnValue({
      submission_id: delivery.submission_id,
    } as any);
    vi.mocked(startBatchBackendAttemptObservationRefreshLoop).mockResolvedValue({
      stop: stopAttemptObservationRefreshLoop as unknown as () => Promise<void>,
    });
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

  it('reuses PREPARED receipts and executes exactly once after a retry', async () => {
    await reserveBatchExecutionReceipt(testContext, buildPreparedReceiptInput());
    const calls: string[] = [];

    const execution = await executeBatchGraphqlOperations(buildSchema(calls), testContext, [operation], {
      directDeliveryContext,
      waitUntil: BatchWaitUntil.Materialized,
    });

    expect(execution.materialized).toBe(true);
    expect(calls).toEqual(['write:one']);
    expect(Array.from(receipts.values())[0]).toMatchObject({
      state: BatchExecutionReceiptState.Completed,
      result_materialized: true,
    });
  });

  it('fails closed without executing when the durable receipt is already STARTED', async () => {
    const prepared = await reserveBatchExecutionReceipt(testContext, buildPreparedReceiptInput());
    await recordBatchExecutionReceiptStarted(testContext, prepared);
    const calls: string[] = [];

    await expect(executeBatchGraphqlOperations(buildSchema(calls), testContext, [operation], {
      directDeliveryContext,
      waitUntil: BatchWaitUntil.Materialized,
    })).rejects.toThrowError(expect.objectContaining({
      extensions: expect.objectContaining({
        data: expect.objectContaining({
          batch_error_code: BatchAdmissionErrorCode.ExecutionRequiresReconciliation,
        }),
      }),
    }));
    expect(calls).toEqual([]);
    expect(receipts.has(buildBatchExecutionReconciliationId(delivery.internal_id))).toBe(false);
    expect(startBatchBackendAttemptObservationRefreshLoop).not.toHaveBeenCalled();
  });

  it('starts observation only after STARTED is durable and stops it after materialized completion', async () => {
    const calls: string[] = [];
    vi.mocked(startBatchBackendAttemptObservationRefreshLoop).mockImplementation(async (receipt) => {
      expect(receipts.get(receipt.internal_id)).toMatchObject({
        state: BatchExecutionReceiptState.Started,
        started_at: expect.any(String),
      });
      calls.push('observation:start');
      return {
        stop: vi.fn(async () => {
          calls.push('observation:stop');
        }),
      };
    });

    await executeBatchGraphqlOperations(buildSchema(calls), testContext, [operation], {
      directDeliveryContext,
      waitUntil: BatchWaitUntil.Materialized,
    });

    expect(calls).toEqual(['observation:start', 'write:one', 'observation:stop']);
  });

  it('keeps direct execution semantics unchanged when observation startup fails', async () => {
    const calls: string[] = [];
    vi.mocked(startBatchBackendAttemptObservationRefreshLoop).mockRejectedValueOnce(new Error('redis unavailable'));

    const execution = await executeBatchGraphqlOperations(buildSchema(calls), testContext, [operation], {
      directDeliveryContext,
      waitUntil: BatchWaitUntil.Materialized,
    });

    expect(execution.materialized).toBe(true);
    expect(calls).toEqual(['write:one']);
    expect(receipts.get(buildBatchExecutionReceiptId(delivery.internal_id))).toMatchObject({
      state: BatchExecutionReceiptState.Completed,
      result_materialized: true,
    });
  });

  it('returns cached materialized metadata on duplicate delivery without rerunning the mutation', async () => {
    const calls: string[] = [];
    const schema = buildSchema(calls);

    const first = await executeBatchGraphqlOperations(schema, testContext, [operation], {
      directDeliveryContext,
      waitUntil: BatchWaitUntil.Materialized,
    });
    const second = await executeBatchGraphqlOperations(schema, testContext, [operation], {
      directDeliveryContext,
      waitUntil: BatchWaitUntil.Materialized,
    });

    expect(first.materialized).toBe(true);
    expect(second).toMatchObject({
      executionMode: BatchExecutionMode.Bulk,
      waitUntil: BatchWaitUntil.Materialized,
      materialized: true,
      operationErrors: [],
    });
    expect(second.results).toHaveLength(1);
    expect(calls).toEqual(['write:one']);
  });

  it('resolves a pre-opened STARTED reconciliation when the live attempt completes materially', async () => {
    const calls: string[] = [];

    const execution = await executeBatchGraphqlOperations(buildSchema(calls, false, false, true), testContext, [operation], {
      directDeliveryContext,
      waitUntil: BatchWaitUntil.Materialized,
    });

    expect(execution.materialized).toBe(true);
    expect(receipts.get(buildBatchExecutionReconciliationId(delivery.internal_id))).toMatchObject({
      state: BatchExecutionReconciliationState.ResolvedCompleted,
      resolved_receipt_state: BatchExecutionReceiptState.Completed,
      evidence_ref_id: buildBatchExecutionReceiptId(delivery.internal_id),
    });
    expect(calls).toEqual(['write:one']);
  });

  it('heals an OPEN reconciliation on duplicate read after terminal receipt persistence won the race', async () => {
    const calls: string[] = [];
    const reconciliationId = buildBatchExecutionReconciliationId(delivery.internal_id);
    let failTerminalUpdate = true;
    vi.mocked(elUpdate).mockImplementation(async (_context, _index, id, update) => {
      if (
        id === reconciliationId
        && (update as any).doc.state === BatchExecutionReconciliationState.ResolvedCompleted
        && failTerminalUpdate
      ) {
        failTerminalUpdate = false;
        throw new Error('terminal reconciliation update failed');
      }
      const current = receipts.get(id);
      const next = {
        ...current,
        ...(update as any).doc,
      };
      receipts.set(id, next);
      return next;
    });

    const first = await executeBatchGraphqlOperations(buildSchema(calls, false, false, true), testContext, [operation], {
      directDeliveryContext,
      waitUntil: BatchWaitUntil.Materialized,
    });
    const second = await executeBatchGraphqlOperations(buildSchema(calls), testContext, [operation], {
      directDeliveryContext,
      waitUntil: BatchWaitUntil.Materialized,
    });

    expect(first.materialized).toBe(true);
    expect(second.materialized).toBe(true);
    expect(receipts.get(reconciliationId)).toMatchObject({
      state: BatchExecutionReconciliationState.ResolvedCompleted,
      resolved_receipt_state: BatchExecutionReceiptState.Completed,
    });
    expect(calls).toEqual(['write:one']);
  });

  it('classifies materialized terminal receipt persistence failures as ambiguous', async () => {
    const calls: string[] = [];
    const receiptId = buildBatchExecutionReceiptId(delivery.internal_id);
    vi.mocked(elUpdate).mockImplementation(async (_context, _index, id, update) => {
      if (id === receiptId && (update as any).doc.state === BatchExecutionReceiptState.Completed) {
        throw new Error('terminal receipt update failed');
      }
      const current = receipts.get(id);
      const next = {
        ...current,
        ...(update as any).doc,
      };
      receipts.set(id, next);
      return next;
    });

    await expect(executeBatchGraphqlOperations(buildSchema(calls), testContext, [operation], {
      directDeliveryContext,
      waitUntil: BatchWaitUntil.Materialized,
    })).rejects.toThrow('terminal receipt update failed');

    expect(receipts.get(receiptId)).toMatchObject({
      state: BatchExecutionReceiptState.RequiresReconciliation,
    });
    expect(receipts.get(buildBatchExecutionReconciliationId(delivery.internal_id))).toMatchObject({
      state: BatchExecutionReconciliationState.Ambiguous,
      opened_reason: BatchExecutionReconciliationOpenedReason.PostStartError,
    });
  });

  it('never completes a COMMITTED non-materialized receipt and fails closed on replay', async () => {
    const calls: string[] = [];
    const schema = buildSchema(calls, true);

    const first = await executeBatchGraphqlOperations(schema, testContext, [operation], {
      directDeliveryContext,
      waitUntil: BatchWaitUntil.Committed,
    });

    expect(first.materialized).toBe(false);
    expect(Array.from(receipts.values())[0]).toMatchObject({
      state: BatchExecutionReceiptState.RequiresReconciliation,
      result_materialized: null,
    });
    expect(receipts.get(buildBatchExecutionReconciliationId(delivery.internal_id))).toMatchObject({
      state: BatchExecutionReconciliationState.Open,
      receipt_id: buildBatchExecutionReceiptId(delivery.internal_id),
      opened_from_receipt_state: BatchExecutionReceiptState.Started,
      opened_reason: BatchExecutionReconciliationOpenedReason.CommittedWithoutDurableMaterialization,
    });
    await expect(executeBatchGraphqlOperations(schema, testContext, [operation], {
      directDeliveryContext,
      waitUntil: BatchWaitUntil.Committed,
    })).rejects.toThrowError(expect.objectContaining({
      extensions: expect.objectContaining({
        data: expect.objectContaining({
          batch_error_code: BatchAdmissionErrorCode.ExecutionRequiresReconciliation,
        }),
      }),
    }));
    expect(calls.filter((call) => call === 'write:one')).toHaveLength(1);
  });

  it('keeps COMMITTED non-materialized persistence failures at OPEN instead of relabeling them ambiguous', async () => {
    const calls: string[] = [];
    const receiptId = buildBatchExecutionReceiptId(delivery.internal_id);
    vi.mocked(elUpdate).mockImplementation(async (_context, _index, id, update) => {
      if (id === receiptId && (update as any).doc.state === BatchExecutionReceiptState.RequiresReconciliation) {
        throw new Error('receipt reconciliation update failed');
      }
      const current = receipts.get(id);
      const next = {
        ...current,
        ...(update as any).doc,
      };
      receipts.set(id, next);
      return next;
    });

    await expect(executeBatchGraphqlOperations(buildSchema(calls, true), testContext, [operation], {
      directDeliveryContext,
      waitUntil: BatchWaitUntil.Committed,
    })).rejects.toThrow('receipt reconciliation update failed');

    expect(receipts.get(receiptId)).toMatchObject({
      state: BatchExecutionReceiptState.Started,
    });
    expect(receipts.get(buildBatchExecutionReconciliationId(delivery.internal_id))).toMatchObject({
      state: BatchExecutionReconciliationState.Open,
      opened_reason: BatchExecutionReconciliationOpenedReason.CommittedWithoutDurableMaterialization,
    });
  });

  it('opens reconciliation for an active post-start execution failure', async () => {
    const calls: string[] = [];

    await expect(executeBatchGraphqlOperations(buildSchema(calls, false, true), testContext, [operation], {
      directDeliveryContext,
      waitUntil: BatchWaitUntil.Materialized,
    })).rejects.toThrow('Batch GraphQL operation failed');

    expect(receipts.get(buildBatchExecutionReceiptId(delivery.internal_id))).toMatchObject({
      state: BatchExecutionReceiptState.RequiresReconciliation,
      last_error: 'Batch GraphQL operation failed',
    });
    expect(receipts.get(buildBatchExecutionReconciliationId(delivery.internal_id))).toMatchObject({
      state: BatchExecutionReconciliationState.Ambiguous,
      last_error: 'Batch GraphQL operation failed',
    });
    expect(calls).toEqual(['write:one']);
    expect(stopAttemptObservationRefreshLoop).toHaveBeenCalledTimes(1);
  });

  it('keeps an OPEN reconciliation row when ambiguity persistence fails after the receipt transition', async () => {
    const calls: string[] = [];
    const reconciliationId = buildBatchExecutionReconciliationId(delivery.internal_id);
    vi.mocked(elUpdate).mockImplementation(async (_context, _index, id, update) => {
      if (id === reconciliationId && (update as any).doc.state === BatchExecutionReconciliationState.Ambiguous) {
        throw new Error('reconciliation update failed');
      }
      const current = receipts.get(id);
      const next = {
        ...current,
        ...(update as any).doc,
      };
      receipts.set(id, next);
      return next;
    });

    await expect(executeBatchGraphqlOperations(buildSchema(calls, false, true), testContext, [operation], {
      directDeliveryContext,
      waitUntil: BatchWaitUntil.Materialized,
    })).rejects.toThrow('Batch GraphQL operation failed');

    expect(receipts.get(buildBatchExecutionReceiptId(delivery.internal_id))).toMatchObject({
      state: BatchExecutionReceiptState.RequiresReconciliation,
    });
    expect(receipts.get(reconciliationId)).toMatchObject({
      state: BatchExecutionReconciliationState.Open,
      receipt_id: buildBatchExecutionReceiptId(delivery.internal_id),
    });
  });

  it('rejects conflicting request reuse before execution and records pre-start terminal grouping failures', async () => {
    const calls: string[] = [];
    const schema = buildSchema(calls);
    await executeBatchGraphqlOperations(schema, testContext, [operation], {
      directDeliveryContext,
      waitUntil: BatchWaitUntil.Materialized,
    });

    await expect(executeBatchGraphqlOperations(schema, testContext, [{
      ...operation,
      variables: JSON.stringify({ value: 'two' }),
    }], {
      directDeliveryContext,
      waitUntil: BatchWaitUntil.Materialized,
    })).rejects.toThrowError(expect.objectContaining({
      extensions: expect.objectContaining({
        data: expect.objectContaining({
          batch_error_code: BatchAdmissionErrorCode.ExecutionReceiptConflict,
        }),
      }),
    }));
    expect(calls).toEqual(['write:one']);

    receipts.clear();
    await expect(executeBatchGraphqlOperations(schema, testContext, [operation], {
      directDeliveryContext,
      waitUntil: BatchWaitUntil.Materialized,
      bundlePlan: {
        version: 1,
        executionPhases: [],
      },
    })).rejects.toThrowError('Batch GraphQL operation object is missing from bundle plan');
    expect(Array.from(receipts.values())[0]).toMatchObject({
      state: BatchExecutionReceiptState.FailedTerminal,
      failure_stage: 'BUILD_EXECUTION_GROUPS',
    });
  });
});
