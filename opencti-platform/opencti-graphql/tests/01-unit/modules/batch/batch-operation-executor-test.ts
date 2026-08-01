import { makeExecutableSchema } from '@graphql-tools/schema';
import GraphQLUpload from 'graphql-upload/GraphQLUpload.mjs';
import { describe, expect, it } from 'vitest';
import {
  BatchSideEffectKind,
  isBatchWriteBoundaryOpen,
  registerBatchCommitter,
  registerBatchSideEffect,
} from '../../../../src/modules/batch/batch-executor';
import { buildBatchGraphqlResultToken, executeBatchGraphqlOperations } from '../../../../src/modules/batch/batch-operation-executor';
import { BatchExecutionMode, BatchWaitUntil } from '../../../../src/modules/batch/batch-types';

const buildSchema = (calls: string[]) => makeExecutableSchema({
  typeDefs: `
    type Query {
      status: String!
    }

    scalar Upload

    type Mutation {
      record(value: String!): String!
      echo(value: String!): String!
      upload(file: Upload!): String!
      fail: String!
      batchMutationsExecute: String!
    }
  `,
  resolvers: {
    Upload: GraphQLUpload,
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
      echo: (_: unknown, { value }: { value: string }) => {
        calls.push(`echo:${value}`);
        return value;
      },
      upload: async (_: unknown, { file }: { file: Promise<{ createReadStream: () => NodeJS.ReadableStream }> }) => {
        const upload = await file;
        const chunks: Buffer[] = [];
        for await (const chunk of upload.createReadStream()) {
          chunks.push(Buffer.from(chunk));
        }
        const value = Buffer.concat(chunks).toString('utf8');
        calls.push(`upload:${value}`);
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

  it('prepares the full operation plan before starting resolver writes', async () => {
    const calls: string[] = [];
    const schema = buildSchema(calls);

    await expect(executeBatchGraphqlOperations(schema, {} as any, [
      { query: 'mutation Record { record(value: "first") }' },
      { query: 'query Status { status }' },
    ])).rejects.toThrow('Batch GraphQL operations must contain exactly one mutation operation');

    expect(calls).toEqual([]);
  });

  it('rejects invalid upload paths and forward result tokens before starting resolver writes', async () => {
    const calls: string[] = [];
    const schema = buildSchema(calls);

    await expect(executeBatchGraphqlOperations(schema, {} as any, [
      { query: 'mutation Record { record(value: "first") }' },
      {
        query: 'mutation Upload($file: Upload!) { upload(file: $file) }',
        variables: JSON.stringify({ file: null }),
        files: [{
          path: 'missing',
          name: 'sample.txt',
          mimeType: 'text/plain',
          data: Buffer.from('payload').toString('base64'),
        }],
      },
    ])).rejects.toThrow('Invalid batch GraphQL file path');

    await expect(executeBatchGraphqlOperations(schema, {} as any, [
      {
        query: 'mutation Echo($value: String!) { echo(value: $value) }',
        variables: JSON.stringify({ value: buildBatchGraphqlResultToken(1, ['record']) }),
      },
      { query: 'mutation Record { record(value: "later") }' },
    ])).rejects.toThrow('Batch GraphQL result token must reference a prior operation');

    expect(calls).toEqual([]);
  });

  it('substitutes prior result tokens and hydrates file inputs before execution', async () => {
    const calls: string[] = [];
    const schema = buildSchema(calls);

    const execution = await executeBatchGraphqlOperations(schema, {} as any, [
      { query: 'mutation Record { record(value: "source") }' },
      {
        query: 'mutation Echo($value: String!) { echo(value: $value) }',
        variables: JSON.stringify({ value: buildBatchGraphqlResultToken(0, ['record']) }),
      },
      {
        query: 'mutation Upload($file: Upload!) { upload(file: $file) }',
        variables: JSON.stringify({ file: null }),
        files: [{
          path: 'file',
          name: 'sample.txt',
          mimeType: 'text/plain',
          data: Buffer.from('payload').toString('base64'),
        }],
      },
    ]);

    expect(calls).toEqual([
      'write:source:true',
      'echo:source',
      'upload:payload',
      'commit:false',
      'side-effect:source:false',
    ]);
    expect(execution.results).toEqual([
      { record: 'source' },
      { echo: 'source' },
      { upload: 'payload' },
    ]);
  });
});
