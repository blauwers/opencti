import { describe, expect, it, vi } from 'vitest';
import {
  BatchMutationKind,
  BatchSideEffectKind,
  executeBatchMutations,
  executeSingleBatchMutation,
  registerBatchSideEffect,
  waitForPendingBatchMaterializations,
} from '../../../../src/modules/batch/batch-executor';
import { BatchExecutionMode, BatchWaitUntil } from '../../../../src/modules/batch/batch-types';

describe('batch executor', () => {
  it('executes mutations in order with compatibility materialization defaults', async () => {
    const calls: string[] = [];

    const execution = await executeBatchMutations([
      {
        kind: BatchMutationKind.CreateEntity,
        executeWrite: async () => {
          calls.push('entity');
          return 'entity-result';
        },
      },
      {
        kind: BatchMutationKind.CreateRelation,
        executeWrite: async () => {
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
      sideEffectKinds: [],
      materialized: true,
    });
  });

  it('preserves explicit execution metadata for future batch callers', async () => {
    const execution = await executeBatchMutations([
      {
        kind: BatchMutationKind.UpdateAttribute,
        executeWrite: async () => 'updated',
      },
    ], {
      executionMode: BatchExecutionMode.Bulk,
      waitUntil: BatchWaitUntil.Committed,
    });

    expect(execution.executionMode).toBe(BatchExecutionMode.Bulk);
    expect(execution.waitUntil).toBe(BatchWaitUntil.Committed);
    expect(execution.materialized).toBe(true);
  });

  it('returns the singular result while preserving fail-fast behavior', async () => {
    const laterMutation = vi.fn();

    await expect(executeBatchMutations([
      {
        kind: BatchMutationKind.CreateEntity,
        executeWrite: async () => {
          throw new Error('failed mutation');
        },
      },
      {
        kind: BatchMutationKind.CreateRelation,
        executeWrite: laterMutation,
      },
    ])).rejects.toThrow('failed mutation');

    expect(laterMutation).not.toHaveBeenCalled();
    await expect(executeSingleBatchMutation({
      kind: BatchMutationKind.CreateEntity,
      executeWrite: async () => 'single-result',
    })).resolves.toBe('single-result');
  });

  it('materializes side effects only after all writes complete, including nested writes', async () => {
    const calls: string[] = [];

    const execution = await executeBatchMutations([
      {
        kind: BatchMutationKind.CreateEntity,
        executeWrite: async () => {
          calls.push('outer-write');
          await executeSingleBatchMutation({
            kind: BatchMutationKind.UpdateAttribute,
            executeWrite: async () => {
              calls.push('nested-write');
              return 'nested-result';
            },
            sideEffects: () => [{
              kind: BatchSideEffectKind.AutoEnrichment,
              execute: async () => {
                calls.push('nested-side-effect');
              },
            }],
          });
          return 'outer-result';
        },
        sideEffects: () => [{
          kind: BatchSideEffectKind.AutoEnrichment,
          execute: async () => {
            calls.push('outer-side-effect');
          },
        }],
      },
      {
        kind: BatchMutationKind.CreateRelation,
        executeWrite: async () => {
          calls.push('second-write');
          return 'second-result';
        },
      },
    ]);

    expect(calls).toEqual([
      'outer-write',
      'nested-write',
      'second-write',
      'nested-side-effect',
      'outer-side-effect',
    ]);
    expect(execution.sideEffectKinds).toEqual([
      BatchSideEffectKind.AutoEnrichment,
      BatchSideEffectKind.AutoEnrichment,
    ]);
  });

  it('defers side effects registered inside raw write helpers', async () => {
    const calls: string[] = [];

    await registerBatchSideEffect({
      kind: BatchSideEffectKind.CompatibilityProjection,
      execute: async () => {
        calls.push('outside-effect');
      },
    });

    const execution = await executeSingleBatchMutation({
      kind: BatchMutationKind.CreateEntity,
      executeWrite: async () => {
        calls.push('write');
        await registerBatchSideEffect({
          kind: BatchSideEffectKind.CompatibilityProjection,
          execute: async () => {
            calls.push('deferred-effect');
          },
        });
        calls.push('write-complete');
        return 'result';
      },
    });

    expect(execution).toBe('result');
    expect(calls).toEqual(['outside-effect', 'write', 'write-complete', 'deferred-effect']);
  });

  it('returns committed results before deferred materialization finishes', async () => {
    const calls: string[] = [];
    let releaseMaterialization: (() => void) | undefined;
    const materializationGate = new Promise<void>((resolve) => {
      releaseMaterialization = resolve;
    });

    const execution = await executeBatchMutations([
      {
        kind: BatchMutationKind.CreateEntity,
        executeWrite: async () => {
          calls.push('write');
          return 'result';
        },
        sideEffects: () => [{
          kind: BatchSideEffectKind.AutoEnrichment,
          execute: async () => {
            calls.push('side-effect-started');
            await materializationGate;
            calls.push('side-effect-complete');
          },
        }],
      },
    ], { waitUntil: BatchWaitUntil.Committed });

    expect(execution.materialized).toBe(false);
    expect(calls).toEqual(['write', 'side-effect-started']);

    releaseMaterialization?.();
    await waitForPendingBatchMaterializations();

    expect(calls).toEqual(['write', 'side-effect-started', 'side-effect-complete']);
  });
});
