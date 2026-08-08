import { makeExecutableSchema } from '@graphql-tools/schema';
import { beforeEach, describe, expect, it, vi } from 'vitest';
import { elIndex, elLoadById, elUpdate } from '../../../../src/database/engine';
import { lockResources } from '../../../../src/lock/master-lock';
import { loadBatchDelivery, readBatchDeliveryQueueMessage } from '../../../../src/modules/batch/batch-delivery-domain';
import {
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

const buildSchema = (calls: string[], withSideEffect = false) => makeExecutableSchema({
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
        if (withSideEffect) {
          await registerBatchSideEffect({
            kind: BatchSideEffectKind.StreamPublication,
            execute: async () => {
              calls.push(`side-effect:${value}`);
            },
          });
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

  beforeEach(() => {
    vi.clearAllMocks();
    receipts = new Map();
    vi.mocked(lockResources).mockResolvedValue({ unlock: vi.fn() } as any);
    vi.mocked(loadBatchDelivery).mockResolvedValue(delivery);
    vi.mocked(readBatchDeliveryQueueMessage).mockReturnValue({
      submission_id: delivery.submission_id,
    } as any);
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
