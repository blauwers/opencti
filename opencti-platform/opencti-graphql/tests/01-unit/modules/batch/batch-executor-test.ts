import { describe, expect, it, vi } from 'vitest';
import {
  BATCH_SIDE_EFFECT_SEAL_DESCRIPTORS,
  BatchMutationKind,
  BatchSideEffectKind,
  BatchSideEffectSealClass,
  evaluateBatchSideEffectSeal,
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

  it('evaluates an exact ordered closed seal snapshot after finalizers and before materialization starts', async () => {
    const calls: string[] = [];
    let sealSnapshot: ReturnType<typeof evaluateBatchSideEffectSeal> = undefined;

    await executeSingleBatchMutation({
      kind: BatchMutationKind.CreateEntity,
      executeWrite: async () => {
        calls.push('write');
        registerBatchFinalizer({
          key: 'finalizer-side-effect',
          execute: async () => {
            calls.push('finalizer');
            await registerBatchSideEffect({
              kind: BatchSideEffectKind.WorkLifecycle,
              sealDescriptor: BATCH_SIDE_EFFECT_SEAL_DESCRIPTORS.workLifecycleRedisInitialize,
              execute: async () => {
                calls.push('work-side-effect');
              },
            });
          },
        });
        return 'result';
      },
      sideEffects: () => [{
        kind: BatchSideEffectKind.StreamPublication,
        sealDescriptor: BATCH_SIDE_EFFECT_SEAL_DESCRIPTORS.streamPublicationRaw,
        execute: async () => {
          calls.push('stream-side-effect');
        },
      }],
    }, {
      onSideEffectSealEvaluated: (snapshot) => {
        calls.push('seal-evaluated');
        sealSnapshot = snapshot;
      },
      onMaterializationStarted: () => {
        calls.push('materialization-started');
      },
    });

    expect(sealSnapshot).toEqual([
      BATCH_SIDE_EFFECT_SEAL_DESCRIPTORS.streamPublicationRaw,
      BATCH_SIDE_EFFECT_SEAL_DESCRIPTORS.workLifecycleRedisInitialize,
    ]);
    expect(Object.isFrozen(sealSnapshot)).toBe(true);
    expect(Object.isFrozen(sealSnapshot?.[0])).toBe(true);
    expect(calls).toEqual([
      'write',
      'finalizer',
      'seal-evaluated',
      'materialization-started',
      'stream-side-effect',
      'work-side-effect',
    ]);
  });

  it('rejects missing, expanding, unclassified, and mismatched descriptors before sealing', async () => {
    const execute = async () => undefined;
    const unclassifiedDescriptor = {
      contract_id: 'test.unclassified',
      contract_version: 1,
      kind: BatchSideEffectKind.StreamPublication,
      seal_class: BatchSideEffectSealClass.Unclassified,
    } as const;
    const mismatchedDescriptor = {
      contract_id: 'test.mismatched',
      contract_version: 1,
      kind: BatchSideEffectKind.WorkLifecycle,
      seal_class: BatchSideEffectSealClass.Closed,
    } as const;

    expect(evaluateBatchSideEffectSeal([{
      kind: BatchSideEffectKind.StreamPublication,
      execute,
    }])).toBeUndefined();
    expect(evaluateBatchSideEffectSeal([{
      kind: BatchSideEffectKind.AutoEnrichment,
      sealDescriptor: BATCH_SIDE_EFFECT_SEAL_DESCRIPTORS.autoEnrichmentUpdateEntity,
      execute,
    }])).toBeUndefined();
    expect(evaluateBatchSideEffectSeal([{
      kind: BatchSideEffectKind.StreamPublication,
      sealDescriptor: unclassifiedDescriptor,
      execute,
    }])).toBeUndefined();
    expect(evaluateBatchSideEffectSeal([{
      kind: BatchSideEffectKind.StreamPublication,
      sealDescriptor: mismatchedDescriptor,
      execute,
    }])).toBeUndefined();
  });

  it('rejects post-seal registration before nested execution and keeps the sealed snapshot unchanged', async () => {
    const calls: string[] = [];
    let sealSnapshot: ReturnType<typeof evaluateBatchSideEffectSeal> = undefined;

    await expect(executeSingleBatchMutation({
      kind: BatchMutationKind.CreateEntity,
      executeWrite: async () => 'result',
      sideEffects: () => [{
        kind: BatchSideEffectKind.StreamPublication,
        sealDescriptor: BATCH_SIDE_EFFECT_SEAL_DESCRIPTORS.streamPublicationRaw,
        execute: async () => {
          calls.push('outer-side-effect');
          await registerBatchSideEffect({
            kind: BatchSideEffectKind.WorkLifecycle,
            sealDescriptor: BATCH_SIDE_EFFECT_SEAL_DESCRIPTORS.workLifecycleRedisInitialize,
            execute: async () => {
              calls.push('nested-side-effect');
            },
          });
        },
      }],
    }, {
      onSideEffectSealEvaluated: (snapshot) => {
        sealSnapshot = snapshot;
      },
    })).rejects.toThrow('Cannot register batch side effect WORK_LIFECYCLE after pre-materialization seal');

    expect(calls).toEqual(['outer-side-effect']);
    expect(sealSnapshot).toEqual([BATCH_SIDE_EFFECT_SEAL_DESCRIPTORS.streamPublicationRaw]);
    expect(() => (sealSnapshot as unknown as Array<unknown>).push('mutated')).toThrow();
  });

  it('keeps expanding auto enrichment unsealed so it can append Work and connector effects', async () => {
    const calls: string[] = [];
    let sealSnapshot: ReturnType<typeof evaluateBatchSideEffectSeal> = undefined;

    const execution = await executeSingleBatchMutation({
      kind: BatchMutationKind.CreateEntity,
      executeWrite: async () => 'result',
      sideEffects: () => [{
        kind: BatchSideEffectKind.AutoEnrichment,
        sealDescriptor: BATCH_SIDE_EFFECT_SEAL_DESCRIPTORS.autoEnrichmentUpdateEntity,
        execute: async () => {
          calls.push('auto-enrichment');
          await registerBatchSideEffect({
            kind: BatchSideEffectKind.WorkLifecycle,
            sealDescriptor: BATCH_SIDE_EFFECT_SEAL_DESCRIPTORS.workLifecycleRedisInitialize,
            execute: async () => {
              calls.push('work-lifecycle');
            },
          });
          await registerBatchSideEffect({
            kind: BatchSideEffectKind.ConnectorDispatch,
            sealDescriptor: BATCH_SIDE_EFFECT_SEAL_DESCRIPTORS.connectorDispatchConnectorSend,
            execute: async () => {
              calls.push('connector-dispatch');
            },
          });
        },
      }],
    }, {
      onSideEffectSealEvaluated: (snapshot) => {
        sealSnapshot = snapshot;
      },
    });

    expect(sealSnapshot).toBeUndefined();
    expect(calls).toEqual(['auto-enrichment', 'work-lifecycle', 'connector-dispatch']);
    expect(execution).toBe('result');
  });

  it('coalesces auto enrichment side effects that share one explicit batch descriptor', async () => {
    const calls: string[] = [];
    const batchDescriptor = {
      key: 'auto-enrichment.dispatch.v1',
      execute: async (sideEffects: readonly { batchPayload?: unknown }[]) => {
        calls.push(`batch:${sideEffects.map((sideEffect) => sideEffect.batchPayload).join(',')}`);
      },
    };

    const execution = await executeBatchMutations([
      {
        kind: BatchMutationKind.CreateEntity,
        executeWrite: async () => 'first-result',
        sideEffects: () => [{
          kind: BatchSideEffectKind.AutoEnrichment,
          batchDescriptor,
          batchPayload: 'first',
          execute: async () => {
            calls.push('legacy:first');
          },
        }],
      },
      {
        kind: BatchMutationKind.CreateEntity,
        executeWrite: async () => 'second-result',
        sideEffects: () => [{
          kind: BatchSideEffectKind.AutoEnrichment,
          batchDescriptor,
          batchPayload: 'second',
          execute: async () => {
            calls.push('legacy:second');
          },
        }],
      },
      {
        kind: BatchMutationKind.CreateEntity,
        executeWrite: async () => 'third-result',
        sideEffects: () => [{
          kind: BatchSideEffectKind.AutoEnrichment,
          execute: async () => {
            calls.push('legacy:third');
          },
        }],
      },
    ]);

    expect(calls).toEqual(['batch:first,second', 'legacy:third']);
    expect(execution.sideEffectKinds).toEqual([
      BatchSideEffectKind.AutoEnrichment,
      BatchSideEffectKind.AutoEnrichment,
      BatchSideEffectKind.AutoEnrichment,
    ]);
  });

  it('keeps compatibility projections unsealed until they have a narrow descriptor', async () => {
    let sealSnapshot: ReturnType<typeof evaluateBatchSideEffectSeal> = undefined;

    await executeSingleBatchMutation({
      kind: BatchMutationKind.CreateEntity,
      executeWrite: async () => 'result',
      sideEffects: () => [{
        kind: BatchSideEffectKind.CompatibilityProjection,
        execute: async () => undefined,
      }],
    }, {
      onSideEffectSealEvaluated: (snapshot) => {
        sealSnapshot = snapshot;
      },
    });

    expect(sealSnapshot).toBeUndefined();
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

  it('notifies callers when side-effect materialization starts', async () => {
    const calls: string[] = [];

    await executeBatchMutations([
      {
        kind: BatchMutationKind.CreateEntity,
        executeWrite: async () => {
          calls.push('write');
          return 'result';
        },
        sideEffects: () => [{
          kind: BatchSideEffectKind.AutoEnrichment,
          execute: async () => {
            calls.push('side-effect');
          },
        }],
      },
    ], {
      onMaterializationStarted: () => {
        calls.push('materialization-started');
      },
    });

    expect(calls).toEqual(['write', 'materialization-started', 'side-effect']);
  });

  it('does not reuse a completed batch scope in detached work', async () => {
    let releaseDetached: (() => void) | undefined;
    const detachedGate = new Promise<void>((resolve) => {
      releaseDetached = resolve;
    });
    let resolveDetached: ((value: { committed: boolean; writeBoundaryOpen: boolean }) => void) | undefined;
    const detachedResult = new Promise<{ committed: boolean; writeBoundaryOpen: boolean }>((resolve) => {
      resolveDetached = resolve;
    });

    await executeSingleBatchMutation({
      kind: BatchMutationKind.CreateEntity,
      executeWrite: async () => {
        void detachedGate.then(async () => {
          let committed = false;
          const writeBoundaryOpen = await executeSingleBatchMutation({
            kind: BatchMutationKind.UpdateAttribute,
            executeWrite: async () => {
              registerBatchCommitter({
                key: 'detached-commit',
                execute: async () => {
                  committed = true;
                },
              });
              return isBatchWriteBoundaryOpen();
            },
          });
          resolveDetached?.({ committed, writeBoundaryOpen });
        });
        return 'outer-result';
      },
    });

    releaseDetached?.();
    await expect(detachedResult).resolves.toEqual({
      committed: true,
      writeBoundaryOpen: true,
    });
  });

  it('keeps deferred materialization in an active batch scope', async () => {
    const calls: string[] = [];
    let releaseMaterialization: (() => void) | undefined;
    const materializationGate = new Promise<void>((resolve) => {
      releaseMaterialization = resolve;
    });

    const execution = await executeBatchMutations([
      {
        kind: BatchMutationKind.CreateEntity,
        executeWrite: async () => 'result',
        sideEffects: () => [{
          kind: BatchSideEffectKind.AutoEnrichment,
          execute: async () => {
            calls.push(`side-effect:${isBatchWriteBoundaryOpen()}`);
            await materializationGate;
            await executeSingleBatchMutation({
              kind: BatchMutationKind.UpdateAttribute,
              executeWrite: async () => {
                calls.push(`nested-write:${isBatchWriteBoundaryOpen()}`);
                return 'nested-result';
              },
              sideEffects: () => [{
                kind: BatchSideEffectKind.WorkLifecycle,
                execute: async () => {
                  calls.push('nested-side-effect');
                },
              }],
            });
          },
        }],
      },
    ], { waitUntil: BatchWaitUntil.Committed });

    expect(execution.materialized).toBe(false);
    expect(calls).toEqual(['side-effect:false']);

    releaseMaterialization?.();
    await waitForPendingBatchMaterializations();

    expect(calls).toEqual([
      'side-effect:false',
      'nested-write:false',
      'nested-side-effect',
    ]);
  });
});
