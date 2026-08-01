import { makeExecutableSchema } from '@graphql-tools/schema';
import { describe, expect, it } from 'vitest';
import {
  BatchSideEffectKind,
  isBatchWriteBoundaryOpen,
  registerBatchCommitter,
  registerBatchSideEffect,
} from '../../../../src/modules/batch/batch-executor';
import { executeBatchGraphqlOperations } from '../../../../src/modules/batch/batch-operation-executor';
import { BatchExecutionMode, BatchWaitUntil } from '../../../../src/modules/batch/batch-types';

const buildSchema = (calls: string[]) => makeExecutableSchema({
  typeDefs: `
    type Query {
      status: String!
    }

    type Mutation {
      record(value: String!): String!
      fail: String!
      batchMutationsExecute: String!
    }
  `,
  resolvers: {
    Query: {
      status: () => 'ok',
    },
    Mutation: {
      record: async (_: unknown, { value }: { value: string }) => {
        calls.push(`write:${value}:${isBatchWriteBoundaryOpen()}`);
        registerBatchCommitter({
          key: 'graphql-operations',
          execute: async () => {
            calls.push(`commit:${isBatchWriteBoundaryOpen()}`);
          },
        });
        await registerBatchSideEffect({
          kind: BatchSideEffectKind.CompatibilityProjection,
          execute: async () => {
            calls.push(`side-effect:${value}:${isBatchWriteBoundaryOpen()}`);
          },
        });
        return value;
      },
      fail: () => {
        throw new Error('resolver failed');
      },
      batchMutationsExecute: () => 'nested',
    },
  },
});

describe('batch GraphQL operation executor', () => {
  it('executes mutation documents under one write boundary and preserves batch metadata', async () => {
    const calls: string[] = [];
    const schema = buildSchema(calls);

    const execution = await executeBatchGraphqlOperations(schema, {} as any, [
      {
        query: 'mutation Record($value: String!) { record(value: $value) }',
        variables: JSON.stringify({ value: 'first' }),
      },
      {
        query: 'mutation Record($value: String!) { record(value: $value) }',
        variables: JSON.stringify({ value: 'second' }),
      },
    ], {
      executionMode: BatchExecutionMode.Bulk,
      waitUntil: BatchWaitUntil.Materialized,
    });

    expect(calls).toEqual([
      'write:first:true',
      'write:second:true',
      'commit:false',
      'side-effect:first:false',
      'side-effect:second:false',
    ]);
    expect(execution.executionMode).toBe(BatchExecutionMode.Bulk);
    expect(execution.waitUntil).toBe(BatchWaitUntil.Materialized);
    expect(execution.results).toEqual([
      { record: 'first' },
      { record: 'second' },
    ]);
    expect(execution.sideEffectKinds).toEqual([
      BatchSideEffectKind.CompatibilityProjection,
      BatchSideEffectKind.CompatibilityProjection,
    ]);
  });

  it('rejects empty, non-mutation, multi-field, nested, and invalid-variable operations', async () => {
    const schema = buildSchema([]);

    await expect(executeBatchGraphqlOperations(schema, {} as any, []))
      .rejects.toThrow('Batch GraphQL operations cannot be empty');
    await expect(executeBatchGraphqlOperations(schema, {} as any, [
      { query: 'query Status { status }' },
    ])).rejects.toThrow('Batch GraphQL operations must contain exactly one mutation operation');
    await expect(executeBatchGraphqlOperations(schema, {} as any, [
      { query: 'mutation Record { record(value: "first") record(value: "second") }' },
    ])).rejects.toThrow('Batch GraphQL operations must contain exactly one top-level mutation field');
    await expect(executeBatchGraphqlOperations(schema, {} as any, [
      { query: 'mutation Nested { batchMutationsExecute }' },
    ])).rejects.toThrow('Nested batch GraphQL operation execution is not supported');
    await expect(executeBatchGraphqlOperations(schema, {} as any, [
      {
        query: 'mutation Record($value: String!) { record(value: $value) }',
        variables: '[]',
      },
    ])).rejects.toThrow('Invalid batch GraphQL operation variables');
  });

  it('fails fast when one operation returns GraphQL errors', async () => {
    const calls: string[] = [];
    const schema = buildSchema(calls);

    await expect(executeBatchGraphqlOperations(schema, {} as any, [
      { query: 'mutation Fail { fail }' },
      { query: 'mutation Record { record(value: "later") }' },
    ])).rejects.toThrow('Batch GraphQL operation failed');

    expect(calls).toEqual([]);
  });
});
