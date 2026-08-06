import { describe, expect, it, vi } from 'vitest';
import {
  BatchMutationKind,
  BatchSideEffectKind,
  executeBatchMutations,
  executeSingleBatchMutation,
  getBatchExecutionMetadata,
  isBatchWriteBoundaryOpen,
  registerBatchCommitter,
  registerBatchFinalizer,
  registerBatchSideEffect,
  setBatchExecutionMetadata,
  waitForPendingBatchMaterializations,
} from '../../../../src/modules/batch/batch-executor';
import { BatchExecutionMode, BatchWaitUntil } from '../../../../src/modules/batch/batch-types';

describe('batch executor', () => {
  it('executes mutations in order with bulk materialization defaults', async () => {
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
      executionMode: BatchExecutionMode.Bulk,
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

  it('commits buffered writes once before materialization', async () => {
    const calls: string[] = [];

    await executeBatchMutations([
      {
        kind: BatchMutationKind.CreateEntity,
        executeWrite: async () => {
          calls.push('first-write');
          setBatchExecutionMetadata('buffered-values', ['first']);
          registerBatchCommitter({
            key: 'buffered-write',
            execute: async () => {
              calls.push(`commit:${getBatchExecutionMetadata<string[]>('buffered-values')?.join(',')}`);
            },
          });
          return 'first-result';
        },
      },
      {
        kind: BatchMutationKind.CreateRelation,
        executeWrite: async () => {
          calls.push('second-write');
          getBatchExecutionMetadata<string[]>('buffered-values')?.push('second');
          registerBatchCommitter({
            key: 'buffered-write',
            execute: async () => {
              calls.push(`commit:${getBatchExecutionMetadata<string[]>('buffered-values')?.join(',')}`);
            },
          });
          return 'second-result';
        },
        sideEffects: () => [{
          kind: BatchSideEffectKind.CompatibilityProjection,
          execute: async () => {
            calls.push('side-effect');
          },
        }],
      },
    ]);

    expect(calls).toEqual([
      'first-write',
      'second-write',
      'commit:first,second',
      'side-effect',
    ]);
  });

  it('closes the write boundary before committers and side effects run', async () => {
    const boundaryStates: boolean[] = [];

    await executeSingleBatchMutation({
      kind: BatchMutationKind.CreateEntity,
      executeWrite: async () => {
        boundaryStates.push(isBatchWriteBoundaryOpen());
        registerBatchCommitter({
          key: 'boundary-state',
          execute: async () => {
            boundaryStates.push(isBatchWriteBoundaryOpen());
          },
        });
        return 'result';
      },
      sideEffects: () => [{
        kind: BatchSideEffectKind.CompatibilityProjection,
        execute: async () => {
          boundaryStates.push(isBatchWriteBoundaryOpen());
        },
      }],
    });

    expect(boundaryStates).toEqual([true, false, false]);
  });

  it('runs finalizers after committers and before side effects', async () => {
    const calls: string[] = [];

    await executeSingleBatchMutation({
      kind: BatchMutationKind.CreateEntity,
      executeWrite: async () => {
        calls.push('write');
        registerBatchCommitter({
          key: 'commit',
          execute: async () => {
            calls.push('commit');
          },
        });
        registerBatchFinalizer({
          key: 'finalizer',
          execute: async () => {
            calls.push('finalizer');
          },
        });
        return 'result';
      },
      sideEffects: () => [{
        kind: BatchSideEffectKind.CompatibilityProjection,
        execute: async () => {
          calls.push('side-effect');
        },
      }],
    });

    expect(calls).toEqual(['write', 'commit', 'finalizer', 'side-effect']);
  });

  it('materializes ordered side effects before bounded auto enrichment and keeps nested effects inline', async () => {
    const calls: string[] = [];

    const execution = await executeBatchMutations([
      {
        kind: BatchMutationKind.CreateEntity,
        executeWrite: async () => 'first-result',
        sideEffects: () => [{
          kind: BatchSideEffectKind.AutoEnrichment,
          execute: async () => {
            calls.push('first-side-effect:start');
            await registerBatchSideEffect({
              kind: BatchSideEffectKind.WorkLifecycle,
              execute: async () => {
                calls.push('nested-side-effect');
              },
            });
            calls.push('first-side-effect:end');
          },
        }],
      },
      {
        kind: BatchMutationKind.CreateRelation,
        executeWrite: async () => 'second-result',
        sideEffects: () => [{
          kind: BatchSideEffectKind.StreamPublication,
          execute: async () => {
            calls.push('second-side-effect');
          },
        }],
      },
    ]);

    expect(calls).toEqual([
      'second-side-effect',
      'first-side-effect:start',
      'nested-side-effect',
      'first-side-effect:end',
    ]);
    expect(execution.sideEffectKinds).toEqual([
      BatchSideEffectKind.AutoEnrichment,
      BatchSideEffectKind.StreamPublication,
      BatchSideEffectKind.WorkLifecycle,
    ]);
  });

  it('materializes nested mutation side effects inline during active materialization', async () => {
    const calls: string[] = [];

    const execution = await executeBatchMutations([
      {
        kind: BatchMutationKind.CreateEntity,
        executeWrite: async () => 'first-result',
        sideEffects: () => [{
          kind: BatchSideEffectKind.AutoEnrichment,
          execute: async () => {
            calls.push('first-side-effect:start');
            await executeSingleBatchMutation({
              kind: BatchMutationKind.UpdateAttribute,
              executeWrite: async () => {
                calls.push('nested-write');
                return 'nested-result';
              },
              sideEffects: () => [{
                kind: BatchSideEffectKind.ConnectorDispatch,
                execute: async () => {
                  calls.push('nested-side-effect');
                },
              }],
            });
            calls.push('first-side-effect:end');
          },
        }],
      },
      {
        kind: BatchMutationKind.CreateRelation,
        executeWrite: async () => 'second-result',
        sideEffects: () => [{
          kind: BatchSideEffectKind.StreamPublication,
          execute: async () => {
            calls.push('second-side-effect');
          },
        }],
      },
    ]);

    expect(calls).toEqual([
      'second-side-effect',
      'first-side-effect:start',
      'nested-write',
      'nested-side-effect',
      'first-side-effect:end',
    ]);
    expect(execution.sideEffectKinds).toEqual([
      BatchSideEffectKind.AutoEnrichment,
      BatchSideEffectKind.StreamPublication,
      BatchSideEffectKind.ConnectorDispatch,
    ]);
  });

  it('materializes top-level auto enrichments concurrently inside the bounded lane', async () => {
    const calls: string[] = [];
    let releaseFirst: (() => void) | undefined;
    let releaseSecond: (() => void) | undefined;
    let markBothStarted: (() => void) | undefined;
    const firstGate = new Promise<void>((resolve) => {
      releaseFirst = resolve;
    });
    const secondGate = new Promise<void>((resolve) => {
      releaseSecond = resolve;
    });
    const bothStarted = new Promise<void>((resolve) => {
      markBothStarted = resolve;
    });
    const recordStart = (value: string) => {
      calls.push(value);
      if (calls.length === 2) {
        markBothStarted?.();
      }
    };

    const executionPromise = executeBatchMutations([
      {
        kind: BatchMutationKind.CreateEntity,
        executeWrite: async () => 'first-result',
        sideEffects: () => [{
          kind: BatchSideEffectKind.AutoEnrichment,
          execute: async () => {
            recordStart('first-side-effect:start');
            await firstGate;
            calls.push('first-side-effect:end');
          },
        }],
      },
      {
        kind: BatchMutationKind.CreateRelation,
        executeWrite: async () => 'second-result',
        sideEffects: () => [{
          kind: BatchSideEffectKind.AutoEnrichment,
          execute: async () => {
            recordStart('second-side-effect:start');
            await secondGate;
            calls.push('second-side-effect:end');
          },
        }],
      },
    ]);

    await bothStarted;
    expect(calls).toEqual(['first-side-effect:start', 'second-side-effect:start']);

    releaseFirst?.();
    releaseSecond?.();
    await executionPromise;

    expect(calls.slice(0, 2)).toEqual([
      'first-side-effect:start',
      'second-side-effect:start',
    ]);
    expect(calls.slice(2).sort()).toEqual([
      'first-side-effect:end',
      'second-side-effect:end',
    ].sort());
  });

  it('keeps side effects registered by committers and finalizers queued until materialization starts', async () => {
    const calls: string[] = [];

    const execution = await executeSingleBatchMutation({
      kind: BatchMutationKind.CreateEntity,
      executeWrite: async () => {
        calls.push('write');
        registerBatchCommitter({
          key: 'committer-side-effect',
          execute: async () => {
            calls.push('commit');
            await registerBatchSideEffect({
              kind: BatchSideEffectKind.WorkLifecycle,
              execute: async () => {
                calls.push('commit-side-effect');
              },
            });
          },
        });
        registerBatchFinalizer({
          key: 'finalizer-side-effect',
          execute: async () => {
            calls.push('finalizer');
            await registerBatchSideEffect({
              kind: BatchSideEffectKind.ConnectorDispatch,
              execute: async () => {
                calls.push('finalizer-side-effect');
              },
            });
          },
        });
        return 'result';
      },
    });

    expect(execution).toBe('result');
    expect(calls).toEqual([
      'write',
      'commit',
      'finalizer',
      'commit-side-effect',
      'finalizer-side-effect',
    ]);
  });

  it('runs finalizers when a buffered write fails before commit', async () => {
    const finalizer = vi.fn().mockResolvedValue(undefined);

    await expect(executeSingleBatchMutation({
      kind: BatchMutationKind.CreateEntity,
      executeWrite: async () => {
        registerBatchFinalizer({
          key: 'finalizer',
          execute: finalizer,
        });
        throw new Error('failed write');
      },
    })).rejects.toThrow('failed write');

    expect(finalizer).toHaveBeenCalledTimes(1);
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
