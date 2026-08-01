import { describe, expect, it, vi } from 'vitest';
import {
  BatchMutationKind,
  executeBatchMutations,
  executeSingleBatchMutation,
} from '../../../../src/modules/batch/batch-executor';
import { BatchExecutionMode, BatchWaitUntil } from '../../../../src/modules/batch/batch-types';

describe('batch executor', () => {
  it('executes mutations in order with compatibility materialization defaults', async () => {
    const calls: string[] = [];

    const execution = await executeBatchMutations([
      {
        kind: BatchMutationKind.CreateEntity,
        execute: async () => {
          calls.push('entity');
          return 'entity-result';
        },
      },
      {
        kind: BatchMutationKind.CreateRelation,
        execute: async () => {
          calls.push('relation');
          return 'relation-result';
        },
      },
    ]);

    expect(calls).toEqual(['entity', 'relation']);
    expect(execution).toEqual({
      executionMode: BatchExecutionMode.Compatibility,
      waitUntil: BatchWaitUntil.Materialized,
      results: ['entity-result', 'relation-result'],
    });
  });

  it('preserves explicit execution metadata for future batch callers', async () => {
    const execution = await executeBatchMutations([
      {
        kind: BatchMutationKind.UpdateAttribute,
        execute: async () => 'updated',
      },
    ], {
      executionMode: BatchExecutionMode.Bulk,
      waitUntil: BatchWaitUntil.Committed,
    });

    expect(execution.executionMode).toBe(BatchExecutionMode.Bulk);
    expect(execution.waitUntil).toBe(BatchWaitUntil.Committed);
  });

  it('returns the singular result while preserving fail-fast behavior', async () => {
    const laterMutation = vi.fn();

    await expect(executeBatchMutations([
      {
        kind: BatchMutationKind.CreateEntity,
        execute: async () => {
          throw new Error('failed mutation');
        },
      },
      {
        kind: BatchMutationKind.CreateRelation,
        execute: laterMutation,
      },
    ])).rejects.toThrow('failed mutation');

    expect(laterMutation).not.toHaveBeenCalled();
    await expect(executeSingleBatchMutation({
      kind: BatchMutationKind.CreateEntity,
      execute: async () => 'single-result',
    })).resolves.toBe('single-result');
  });
});
