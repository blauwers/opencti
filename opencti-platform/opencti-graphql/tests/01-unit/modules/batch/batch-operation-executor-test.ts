import { makeExecutableSchema } from '@graphql-tools/schema';
import GraphQLUpload from 'graphql-upload/GraphQLUpload.mjs';
import { describe, expect, it } from 'vitest';
import { FUNCTIONAL_ERROR, FunctionalError, MissingReferenceError } from '../../../../src/config/errors';
import {
  BatchSideEffectKind,
  isBatchWriteBoundaryOpen,
  registerBatchCommitter,
  registerBatchSideEffect,
} from '../../../../src/modules/batch/batch-executor';
import {
  buildBatchGraphqlResultToken,
  executeBatchGraphqlOperations,
  getBatchGraphqlExecutionAdmissionStats,
  getBatchGraphqlExecutionAdmissionWeight,
} from '../../../../src/modules/batch/batch-operation-executor';
import { BatchExecutionMode, BatchWaitUntil } from '../../../../src/modules/batch/batch-types';

const buildSchema = (calls: string[], beforeRecord?: (value: string) => Promise<void>) => makeExecutableSchema({
  typeDefs: `
    type Query {
      status: String!
    }

    scalar Upload

    type RecordPayload {
      id: String!
      expensive: String!
    }

    type RecordEditPayload {
      apply(value: String!): String!
    }

    type Mutation {
      record(value: String!): String!
      recordAdd(value: String!): String!
      createRecord(value: String!): RecordPayload!
      recordEdit(id: String!): RecordEditPayload!
      echo(value: String!): String!
      upload(file: Upload!): String!
      fail: String!
      functionalFailure: String!
      missingReference: String!
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
        await beforeRecord?.(value);
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
      recordAdd: async (_: unknown, { value }: { value: string }) => {
        calls.push(`add:${value}:${isBatchWriteBoundaryOpen()}`);
        await beforeRecord?.(value);
        return value;
      },
      createRecord: (_: unknown, { value }: { value: string }) => {
        calls.push(`create:${value}`);
        return {
          id: value,
          expensive: `expensive:${value}`,
        };
      },
      recordEdit: (_: unknown, { id }: { id: string }) => {
        calls.push(`edit:${id}`);
        return { id };
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
      functionalFailure: () => {
        throw FunctionalError('invalid record');
      },
      missingReference: () => {
        throw MissingReferenceError({ unresolvedIds: ['identity--missing'] });
      },
      batchMutationsExecute: () => 'nested',
    },
    RecordPayload: {
      expensive: ({ id, expensive }: { id: string; expensive: string }) => {
        calls.push(`expensive:${id}`);
        return expensive;
      },
    },
    RecordEditPayload: {
      apply: ({ id }: { id: string }, { value }: { value: string }) => {
        calls.push(`apply:${id}:${value}`);
        return value;
      },
    },
  },
});

describe('batch GraphQL operation executor', () => {
  it('weights admission by both operation count and encoded payload bytes', () => {
    expect(getBatchGraphqlExecutionAdmissionWeight(Array.from({ length: 1089 }, () => ({ query: 'mutation { record(value: "x") }' })), 4)).toBe(1);
    expect(getBatchGraphqlExecutionAdmissionWeight(Array.from({ length: 2001 }, () => ({ query: 'mutation { record(value: "x") }' })), 4)).toBe(2);
    expect(getBatchGraphqlExecutionAdmissionWeight([{
      query: 'mutation { record(value: "x") }',
      variables: 'x'.repeat(11 * 1024 * 1024),
    }], 4)).toBe(3);
    expect(getBatchGraphqlExecutionAdmissionWeight([{
      query: 'mutation { record(value: "x") }',
      variables: 'x'.repeat(30 * 1024 * 1024),
    }], 4)).toBe(4);
    expect(getBatchGraphqlExecutionAdmissionStats([{
      query: 'a',
      variables: 'bc',
      operationName: 'd',
      objectId: 'e',
      files: [{
        path: 'f',
        name: 'g',
        mimeType: 'h',
        data: 'ij',
      }],
    }], 4)).toEqual({
      encodedBytes: 10,
      operationCount: 1,
      weight: 1,
    });
  });

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
    ])).rejects.toMatchObject({
      extensions: {
        data: {
          operation_errors: [{
            message: 'resolver failed',
            path: ['fail'],
          }],
        },
      },
    });

    expect(calls).toEqual([]);
  });

  it('returns retryable missing-reference failures without aborting bulk commits', async () => {
    const calls: string[] = [];
    const schema = buildSchema(calls);

    const execution = await executeBatchGraphqlOperations(schema, {} as any, [
      {
        query: 'mutation Record($value: String!) { record(value: $value) }',
        variables: JSON.stringify({ value: 'first' }),
        objectId: 'indicator--1',
      },
      {
        query: 'mutation MissingReference { missingReference }',
        objectId: 'relationship--1',
      },
      {
        query: 'mutation Record($value: String!) { record(value: $value) }',
        variables: JSON.stringify({ value: 'later' }),
        objectId: 'indicator--2',
      },
    ]);

    expect(calls).toEqual([
      'write:first:true',
      'write:later:true',
      'commit:false',
      'side-effect:first:false',
      'side-effect:later:false',
    ]);
    expect(execution.results).toEqual([
      { record: 'first' },
      null,
      { record: 'later' },
    ]);
    expect(execution.operationErrors).toEqual([{
      code: 'MISSING_REFERENCE_ERROR',
      message: 'Batch GraphQL operation failed',
      objectId: 'relationship--1',
      operationIndex: 1,
      retryable: true,
    }]);
  });

  it('returns nonretryable functional failures without aborting bulk commits', async () => {
    const calls: string[] = [];
    const schema = buildSchema(calls);

    const execution = await executeBatchGraphqlOperations(schema, {} as any, [
      {
        query: 'mutation Record($value: String!) { record(value: $value) }',
        variables: JSON.stringify({ value: 'first' }),
        objectId: 'indicator--1',
      },
      {
        query: 'mutation FunctionalFailure { functionalFailure }',
        objectId: 'external-reference--1',
      },
      {
        query: 'mutation Record($value: String!) { record(value: $value) }',
        variables: JSON.stringify({ value: 'later' }),
        objectId: 'indicator--2',
      },
    ]);

    expect(calls).toEqual([
      'write:first:true',
      'write:later:true',
      'commit:false',
      'side-effect:first:false',
      'side-effect:later:false',
    ]);
    expect(execution.results).toEqual([
      { record: 'first' },
      null,
      { record: 'later' },
    ]);
    expect(execution.operationErrors).toEqual([{
      code: FUNCTIONAL_ERROR,
      message: 'Batch GraphQL operation failed',
      objectId: 'external-reference--1',
      operationIndex: 1,
      retryable: false,
    }]);
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

  it('prunes unused object mutation fields for metadata-only batch execution', async () => {
    const calls: string[] = [];
    const schema = buildSchema(calls);

    const execution = await executeBatchGraphqlOperations(schema, {} as any, [
      {
        query: 'mutation CreateRecord($value: String!) { createRecord(value: $value) { id expensive } }',
        variables: JSON.stringify({ value: 'source' }),
      },
    ], {
      pruneUnusedResultFields: true,
    });

    expect(calls).toEqual(['create:source']);
    expect(execution.results).toEqual([
      { createRecord: { __typename: 'RecordPayload' } },
    ]);
  });

  it('does not prune nested edit mutation resolvers', async () => {
    const calls: string[] = [];
    const schema = buildSchema(calls);

    const execution = await executeBatchGraphqlOperations(schema, {} as any, [
      {
        query: 'mutation RecordEdit($id: String!, $value: String!) { recordEdit(id: $id) { apply(value: $value) } }',
        variables: JSON.stringify({ id: 'target', value: 'source' }),
      },
    ], {
      pruneUnusedResultFields: true,
    });

    expect(calls).toEqual([
      'edit:target',
      'apply:target:source',
    ]);
    expect(execution.results).toEqual([
      { recordEdit: { apply: 'source' } },
    ]);
  });

  it('retains only result-token fields when pruning object mutation responses', async () => {
    const calls: string[] = [];
    const schema = buildSchema(calls);

    const execution = await executeBatchGraphqlOperations(schema, {} as any, [
      {
        query: 'mutation CreateRecord($value: String!) { createRecord(value: $value) { id expensive } }',
        variables: JSON.stringify({ value: 'source' }),
      },
      {
        query: 'mutation Echo($value: String!) { echo(value: $value) }',
        variables: JSON.stringify({ value: buildBatchGraphqlResultToken(0, ['createRecord', 'id']) }),
      },
    ], {
      pruneUnusedResultFields: true,
    });

    expect(calls).toEqual([
      'create:source',
      'echo:source',
    ]);
    expect(execution.results).toEqual([
      { createRecord: { id: 'source' } },
      { echo: 'source' },
    ]);
  });

  it('coalesces repeated add operations during pruned batch execution and preserves duplicate result bindings', async () => {
    const calls: string[] = [];
    const schema = buildSchema(calls);

    const execution = await executeBatchGraphqlOperations(schema, {} as any, [
      {
        query: 'mutation RecordAdd($value: String!) { recordAdd(value: $value) }',
        variables: JSON.stringify({ value: 'shared' }),
        objectId: 'external-reference--one',
        executionGroup: 0,
        executionPhase: 0,
      },
      {
        query: 'mutation RecordAdd($value: String!) { recordAdd(value: $value) }',
        variables: JSON.stringify({ value: 'shared' }),
        objectId: 'external-reference--two',
        executionGroup: 1,
        executionPhase: 1,
      },
      {
        query: 'mutation Echo($value: String!) { echo(value: $value) }',
        variables: JSON.stringify({ value: buildBatchGraphqlResultToken(1, ['recordAdd']) }),
        objectId: 'relationship--one',
        executionGroup: 2,
        executionPhase: 2,
      },
    ], {
      pruneUnusedResultFields: true,
    });

    expect(calls).toEqual([
      'add:shared:true',
      'echo:shared',
    ]);
    expect(execution.results).toEqual([
      { recordAdd: 'shared' },
      { recordAdd: 'shared' },
      { echo: 'shared' },
    ]);
  });

  it('coalesces same-phase add operations while preserving coordinator participation', async () => {
    const calls: string[] = [];
    const schema = buildSchema(calls);

    await executeBatchGraphqlOperations(schema, {} as any, [
      {
        query: 'mutation RecordAdd($value: String!) { recordAdd(value: $value) }',
        variables: JSON.stringify({ value: 'shared' }),
        objectId: 'external-reference--one',
        executionGroup: 0,
        executionPhase: 0,
      },
      {
        query: 'mutation RecordAdd($value: String!) { recordAdd(value: $value) }',
        variables: JSON.stringify({ value: 'shared' }),
        objectId: 'external-reference--two',
        executionGroup: 1,
        executionPhase: 0,
      },
    ], {
      pruneUnusedResultFields: true,
    });

    expect(calls).toEqual([
      'add:shared:true',
    ]);
  });

  it('does not retain completed add results for non-pruned batch execution', async () => {
    const calls: string[] = [];
    const schema = buildSchema(calls);

    await executeBatchGraphqlOperations(schema, {} as any, [
      {
        query: 'mutation RecordAdd($value: String!) { recordAdd(value: $value) }',
        variables: JSON.stringify({ value: 'shared' }),
        objectId: 'external-reference--one',
        executionGroup: 0,
        executionPhase: 0,
      },
      {
        query: 'mutation RecordAdd($value: String!) { recordAdd(value: $value) }',
        variables: JSON.stringify({ value: 'shared' }),
        objectId: 'external-reference--two',
        executionGroup: 1,
        executionPhase: 1,
      },
    ]);

    expect(calls).toEqual([
      'add:shared:true',
      'add:shared:true',
    ]);
  });

  it('executes independent declared groups in the same phase concurrently', async () => {
    const calls: string[] = [];
    let releaseFirst: (() => void) | undefined;
    let markSecondStarted: (() => void) | undefined;
    const firstGate = new Promise<void>((resolve) => {
      releaseFirst = resolve;
    });
    const secondStarted = new Promise<void>((resolve) => {
      markSecondStarted = resolve;
    });
    const schema = buildSchema(calls, async (value) => {
      if (value === 'first') {
        await firstGate;
      }
      if (value === 'second') {
        markSecondStarted?.();
      }
    });

    const executionPromise = executeBatchGraphqlOperations(schema, {} as any, [
      {
        query: 'mutation Record($value: String!) { record(value: $value) }',
        variables: JSON.stringify({ value: 'first' }),
        executionGroup: 0,
        executionPhase: 1,
      },
      {
        query: 'mutation Record($value: String!) { record(value: $value) }',
        variables: JSON.stringify({ value: 'second' }),
        executionGroup: 1,
        executionPhase: 1,
      },
    ]);

    await secondStarted;
    expect(calls).toEqual([
      'write:first:true',
      'write:second:true',
    ]);

    releaseFirst?.();
    const execution = await executionPromise;
    expect(execution.results).toEqual([
      { record: 'first' },
      { record: 'second' },
    ]);
  });

  it('coordinates create-containing groups before relation-only groups in the same phase', async () => {
    const calls: string[] = [];
    let releaseFirst: (() => void) | undefined;
    let markSecondStarted: (() => void) | undefined;
    let relationOnlyStarted = false;
    const firstGate = new Promise<void>((resolve) => {
      releaseFirst = resolve;
    });
    const secondStarted = new Promise<void>((resolve) => {
      markSecondStarted = resolve;
    });
    const schema = buildSchema(calls, async (value) => {
      if (value === 'first') {
        await firstGate;
      }
      if (value === 'second') {
        markSecondStarted?.();
      }
      if (value === 'relation-only') {
        relationOnlyStarted = true;
      }
    });

    const executionPromise = executeBatchGraphqlOperations(schema, {} as any, [
      {
        query: 'mutation RecordAdd($value: String!) { recordAdd(value: $value) }',
        variables: JSON.stringify({ value: 'first' }),
        objectId: 'identity--first',
        executionGroup: 0,
        executionPhase: 1,
      },
      {
        query: 'mutation Record($value: String!) { record(value: $value) }',
        variables: JSON.stringify({ value: 'mixed' }),
        objectId: 'identity--first',
        executionGroup: 0,
        executionPhase: 1,
      },
      {
        query: 'mutation RecordAdd($value: String!) { recordAdd(value: $value) }',
        variables: JSON.stringify({ value: 'second' }),
        objectId: 'identity--second',
        executionGroup: 1,
        executionPhase: 1,
      },
      {
        query: 'mutation Record($value: String!) { record(value: $value) }',
        variables: JSON.stringify({ value: 'relation-only' }),
        objectId: 'relationship--only',
        executionGroup: 2,
        executionPhase: 1,
      },
    ]);

    await secondStarted;
    await new Promise((resolve) => setTimeout(resolve, 20));
    expect(relationOnlyStarted).toBe(false);

    releaseFirst?.();
    const execution = await executionPromise;
    expect(calls.indexOf('write:relation-only:true')).toBeGreaterThan(calls.indexOf('write:mixed:true'));
    expect(execution.results).toEqual([
      { recordAdd: 'first' },
      { record: 'mixed' },
      { recordAdd: 'second' },
      { record: 'relation-only' },
    ]);
  });

  it('rebuilds operation groups from backend bundle object phases', async () => {
    const calls: string[] = [];
    let releaseSource: (() => void) | undefined;
    let markIndependentStarted: (() => void) | undefined;
    const sourceGate = new Promise<void>((resolve) => {
      releaseSource = resolve;
    });
    const independentStarted = new Promise<void>((resolve) => {
      markIndependentStarted = resolve;
    });
    const schema = buildSchema(calls, async (value) => {
      if (value === 'source') {
        await sourceGate;
      }
      if (value === 'independent') {
        markIndependentStarted?.();
      }
    });

    const executionPromise = executeBatchGraphqlOperations(schema, {} as any, [
      {
        query: 'mutation Record($value: String!) { record(value: $value) }',
        variables: JSON.stringify({ value: 'source' }),
        objectId: 'identity--1',
        executionGroup: 99,
        executionPhase: 9,
      },
      {
        query: 'mutation Record($value: String!) { record(value: $value) }',
        variables: JSON.stringify({ value: 'independent' }),
        objectId: 'malware--1',
        executionGroup: 99,
        executionPhase: 9,
      },
      {
        query: 'mutation Record($value: String!) { record(value: $value) }',
        variables: JSON.stringify({ value: 'dependent' }),
        objectId: 'indicator--1',
        executionGroup: 99,
        executionPhase: 0,
      },
    ], {
      bundlePlan: {
        version: 1,
        executionPhases: [
          { phase: 0, objectIds: ['identity--1', 'malware--1'] },
          { phase: 1, objectIds: ['indicator--1'] },
        ],
      },
    });

    await independentStarted;
    expect(calls).toEqual([
      'write:source:true',
      'write:independent:true',
    ]);

    releaseSource?.();
    const execution = await executionPromise;
    expect(calls).toContain('write:dependent:true');
    expect(execution.results).toEqual([
      { record: 'source' },
      { record: 'independent' },
      { record: 'dependent' },
    ]);
  });

  it('delays generated bundle operations that reference another admitted object', async () => {
    const calls: string[] = [];
    let releaseSource: (() => void) | undefined;
    let markIndependentStarted: (() => void) | undefined;
    const sourceGate = new Promise<void>((resolve) => {
      releaseSource = resolve;
    });
    const independentStarted = new Promise<void>((resolve) => {
      markIndependentStarted = resolve;
    });
    const schema = buildSchema(calls, async (value) => {
      if (value === 'source') {
        await sourceGate;
      }
      if (value === 'independent') {
        markIndependentStarted?.();
      }
    });

    const executionPromise = executeBatchGraphqlOperations(schema, {} as any, [
      {
        query: 'mutation Record($value: String!) { record(value: $value) }',
        variables: JSON.stringify({ value: 'source' }),
        objectId: 'identity--1',
      },
      {
        query: 'mutation Record($value: String!) { record(value: $value) }',
        variables: JSON.stringify({ value: 'relationship--1' }),
        objectId: 'relationship--1',
      },
      {
        query: 'mutation Record($value: String!) { record(value: $value) }',
        variables: JSON.stringify({ value: 'identity--1' }),
        objectId: 'relationship--1',
      },
      {
        query: 'mutation Record($value: String!) { record(value: $value) }',
        variables: JSON.stringify({ value: 'independent' }),
        objectId: 'malware--1',
      },
    ], {
      bundlePlan: {
        version: 1,
        executionPhases: [{ phase: 0, objectIds: ['identity--1', 'relationship--1', 'malware--1'] }],
      },
    });

    await independentStarted;
    expect(calls).toEqual([
      'write:source:true',
      'write:relationship--1:true',
      'write:independent:true',
    ]);

    releaseSource?.();
    const execution = await executionPromise;
    expect(calls).toContain('write:identity--1:true');
    expect(execution.results).toEqual([
      { record: 'source' },
      { record: 'relationship--1' },
      { record: 'identity--1' },
      { record: 'independent' },
    ]);
  });

  it('delays consumers until the referenced object final generated group completes', async () => {
    const calls: string[] = [];
    let releaseSourceFinal: (() => void) | undefined;
    let markSourceFinalStarted: (() => void) | undefined;
    let consumerStarted = false;
    const sourceFinalGate = new Promise<void>((resolve) => {
      releaseSourceFinal = resolve;
    });
    const sourceFinalStarted = new Promise<void>((resolve) => {
      markSourceFinalStarted = resolve;
    });
    const schema = buildSchema(calls, async (value) => {
      if (value === 'source-final') {
        markSourceFinalStarted?.();
        await sourceFinalGate;
      }
      if (value === 'identity--source') {
        consumerStarted = true;
      }
    });

    const executionPromise = executeBatchGraphqlOperations(schema, {} as any, [
      {
        query: 'mutation Record($value: String!) { record(value: $value) }',
        variables: JSON.stringify({ value: 'identity--dependency' }),
        objectId: 'identity--dependency',
      },
      {
        query: 'mutation Record($value: String!) { record(value: $value) }',
        variables: JSON.stringify({ value: 'identity--dependency' }),
        objectId: 'identity--source',
      },
      {
        query: 'mutation Record($value: String!) { record(value: $value) }',
        variables: JSON.stringify({ value: 'source-final' }),
        objectId: 'identity--source',
      },
      {
        query: 'mutation Record($value: String!) { record(value: $value) }',
        variables: JSON.stringify({ value: 'identity--source' }),
        objectId: 'relationship--1',
      },
    ], {
      bundlePlan: {
        version: 1,
        executionPhases: [
          { phase: 0, objectIds: ['identity--dependency', 'identity--source'] },
          { phase: 1, objectIds: ['relationship--1'] },
        ],
      },
    });

    await sourceFinalStarted;
    await new Promise((resolve) => setTimeout(resolve, 20));
    expect(consumerStarted).toBe(false);

    releaseSourceFinal?.();
    const execution = await executionPromise;
    expect(calls.indexOf('write:identity--source:true')).toBeGreaterThan(calls.indexOf('write:source-final:true'));
    expect(execution.results).toEqual([
      { record: 'identity--dependency' },
      { record: 'identity--dependency' },
      { record: 'source-final' },
      { record: 'identity--source' },
    ]);
  });

  it('rejects bundle-planned operations without an admitted object id', async () => {
    const schema = buildSchema([]);

    await expect(executeBatchGraphqlOperations(schema, {} as any, [
      { query: 'mutation Record { record(value: "missing") }' },
    ], {
      bundlePlan: {
        version: 1,
        executionPhases: [{ phase: 0, objectIds: ['identity--1'] }],
      },
    })).rejects.toThrow('Batch GraphQL bundle plan requires object ids for every operation');
  });

  it('moves result-token consumers to a later phase even when declared beside the producer', async () => {
    const calls: string[] = [];
    let releaseSource: (() => void) | undefined;
    let markSourceStarted: (() => void) | undefined;
    const sourceGate = new Promise<void>((resolve) => {
      releaseSource = resolve;
    });
    const sourceStarted = new Promise<void>((resolve) => {
      markSourceStarted = resolve;
    });
    const schema = buildSchema(calls, async (value) => {
      if (value === 'source') {
        markSourceStarted?.();
        await sourceGate;
      }
    });

    const executionPromise = executeBatchGraphqlOperations(schema, {} as any, [
      {
        query: 'mutation Record($value: String!) { record(value: $value) }',
        variables: JSON.stringify({ value: 'source' }),
        executionGroup: 0,
        executionPhase: 1,
      },
      {
        query: 'mutation Echo($value: String!) { echo(value: $value) }',
        variables: JSON.stringify({ value: buildBatchGraphqlResultToken(0, ['record']) }),
        executionGroup: 1,
        executionPhase: 1,
      },
    ]);

    await sourceStarted;
    expect(calls).toEqual(['write:source:true']);

    releaseSource?.();
    const execution = await executionPromise;
    expect(calls).toContain('echo:source');
    expect(execution.results).toEqual([
      { record: 'source' },
      { echo: 'source' },
    ]);
  });

  it('does not commit writes when one concurrent group fails', async () => {
    const calls: string[] = [];
    const schema = buildSchema(calls);

    await expect(executeBatchGraphqlOperations(schema, {} as any, [
      {
        query: 'mutation Record($value: String!) { record(value: $value) }',
        variables: JSON.stringify({ value: 'first' }),
        executionGroup: 0,
        executionPhase: 1,
      },
      {
        query: 'mutation Fail { fail }',
        executionGroup: 1,
        executionPhase: 1,
      },
    ])).rejects.toThrow('Batch GraphQL operation failed');

    expect(calls).toContain('write:first:true');
    expect(calls).not.toContain('commit:false');
  });
});
