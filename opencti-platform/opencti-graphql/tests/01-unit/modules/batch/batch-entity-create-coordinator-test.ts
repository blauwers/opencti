import { beforeEach, describe, expect, it, vi } from 'vitest';
import conf from '../../../../src/config/conf';
import { internalFindByIds } from '../../../../src/database/middleware-loader';
import { lockResources } from '../../../../src/lock/master-lock';
import { executeBatchCoalescedEntityCreate } from '../../../../src/modules/batch/batch-entity-create-cache';
import { getBatchRetainedLockIds } from '../../../../src/modules/batch/batch-lock-retention';
import {
  BatchEntityCreateCoordinator,
  getBatchEntityCreateCoordinatorHeldParticipantIds,
  resolveBatchEntityCreateLookup,
  resolveBatchParticipantLock,
  runWithBatchEntityCreateCoordinator,
} from '../../../../src/modules/batch/batch-entity-create-coordinator';
import { BatchMutationKind, executeBatchMutations } from '../../../../src/modules/batch/batch-executor';
import { registerBatchCreatedEntity } from '../../../../src/modules/batch/batch-relation-lookup';

vi.mock('../../../../src/database/middleware-loader', () => ({
  internalFindByIds: vi.fn(),
}));

vi.mock('../../../../src/lock/master-lock', () => ({
  lockResources: vi.fn(),
}));

const buildLock = () => ({
  signal: new AbortController().signal,
  unlock: vi.fn().mockResolvedValue(undefined),
});

describe('batch entity create coordinator', () => {
  const context = {} as any;

  beforeEach(() => {
    vi.clearAllMocks();
    vi.mocked(lockResources).mockResolvedValue(buildLock() as any);
  });

  it('resolves disjoint direct ids in one locked phase lookup', async () => {
    vi.mocked(internalFindByIds).mockResolvedValue([
      {
        internal_id: 'indicator--internal-one',
        standard_id: 'indicator--one',
        entity_type: 'Indicator',
      },
      {
        internal_id: 'indicator--internal-two',
        standard_id: 'indicator--two',
        entity_type: 'Indicator',
      },
    ] as any);

    const coordinator = new BatchEntityCreateCoordinator(context, [0, 1]);
    const heldParticipantIdsByGroup: string[][] = [];
    const [first, second] = await Promise.all([
      runWithBatchEntityCreateCoordinator(coordinator, 0, async () => {
        const resolution = await resolveBatchEntityCreateLookup({
          finderIds: ['indicator--one'],
          participantIds: ['indicator--one'],
          type: 'Indicator',
        }) as any;
        heldParticipantIdsByGroup[0] = getBatchEntityCreateCoordinatorHeldParticipantIds().sort();
        return resolution;
      }),
      runWithBatchEntityCreateCoordinator(coordinator, 1, async () => {
        const resolution = await resolveBatchEntityCreateLookup({
          finderIds: ['indicator--two'],
          participantIds: ['indicator--two'],
          type: 'Indicator',
        }) as any;
        heldParticipantIdsByGroup[1] = getBatchEntityCreateCoordinatorHeldParticipantIds().sort();
        return resolution;
      }),
    ]);
    await coordinator.close();

    expect(lockResources).toHaveBeenCalledTimes(2);
    expect(lockResources).toHaveBeenNthCalledWith(1, ['indicator--one', 'indicator--two'], { draftId: undefined });
    expect(lockResources).toHaveBeenNthCalledWith(2, ['indicator--internal-one', 'indicator--internal-two'], { draftId: undefined });
    expect(internalFindByIds).toHaveBeenCalledTimes(1);
    expect(internalFindByIds).toHaveBeenCalledWith(context, expect.anything(), ['indicator--one', 'indicator--two'], { type: 'Indicator' });
    expect(first.existingByIds.map((element: any) => element.internal_id)).toEqual(['indicator--internal-one']);
    expect(second.existingByIds.map((element: any) => element.internal_id)).toEqual(['indicator--internal-two']);
    expect(heldParticipantIdsByGroup).toEqual([
      ['indicator--internal-one', 'indicator--one'],
      ['indicator--internal-two', 'indicator--two'],
    ]);
  });

  it('lets cross-group coalesced creates join the coordinator lookup wave', async () => {
    vi.mocked(internalFindByIds).mockResolvedValue([] as any);

    const coordinator = new BatchEntityCreateCoordinator(context, [0, 1]);
    const input = { external_id: 'T1174', source_name: 'mitre-attack' };
    const execute = vi.fn(async () => {
      await resolveBatchEntityCreateLookup({
        finderIds: ['external-reference--one'],
        participantIds: ['external-reference--one'],
        type: 'External-Reference',
      });
      return { element: { internal_id: 'external-reference--one' } };
    });

    try {
      const execution = await executeBatchMutations([{
        kind: BatchMutationKind.CreateEntity,
        executeWrite: async () => Promise.all([
          runWithBatchEntityCreateCoordinator(coordinator, 0, async () => executeBatchCoalescedEntityCreate('External-Reference', input, {}, execute)),
          runWithBatchEntityCreateCoordinator(coordinator, 1, async () => executeBatchCoalescedEntityCreate('External-Reference', input, {}, execute)),
        ]),
      }]);

      expect(execution.results).toEqual([[
        { element: { internal_id: 'external-reference--one' } },
        { element: { internal_id: 'external-reference--one' } },
      ]]);
      expect(execute).toHaveBeenCalledTimes(1);
      expect(internalFindByIds).toHaveBeenCalledTimes(1);
    } finally {
      await coordinator.close();
    }
  });

  it('parks cross-group coalesced waiters while the producer enters a later lookup wave', async () => {
    vi.mocked(internalFindByIds).mockResolvedValue([] as any);

    const coordinator = new BatchEntityCreateCoordinator(context, [0, 1]);
    const input = { external_id: 'T1174', source_name: 'mitre-attack' };
    let waiterStartedResolve: (() => void) | undefined;
    const waiterStarted = new Promise<void>((resolve) => {
      waiterStartedResolve = resolve;
    });
    const execute = vi.fn(async () => {
      await resolveBatchEntityCreateLookup({
        finderIds: ['external-reference--one'],
        participantIds: ['external-reference--one'],
        type: 'External-Reference',
      });
      await waiterStarted;
      await resolveBatchEntityCreateLookup({
        finderIds: ['kill-chain-phase--one'],
        participantIds: ['kill-chain-phase--one'],
        type: 'Kill-Chain-Phase',
      });
      return { element: { internal_id: 'external-reference--one' } };
    });

    try {
      const executionPromise = executeBatchMutations([{
        kind: BatchMutationKind.CreateEntity,
        executeWrite: async () => Promise.all([
          runWithBatchEntityCreateCoordinator(coordinator, 0, async () => executeBatchCoalescedEntityCreate('External-Reference', input, {}, execute)),
          runWithBatchEntityCreateCoordinator(coordinator, 1, async () => {
            waiterStartedResolve?.();
            return executeBatchCoalescedEntityCreate('External-Reference', input, {}, execute);
          }),
        ]),
      }]);
      const execution = await Promise.race([
        executionPromise,
        new Promise<never>((_, reject) => {
          setTimeout(() => reject(new Error('cross-group coalesced waiter deadlocked')), 1000);
        }),
      ]);

      expect(execution.results).toEqual([[
        { element: { internal_id: 'external-reference--one' } },
        { element: { internal_id: 'external-reference--one' } },
      ]]);
      expect(execute).toHaveBeenCalledTimes(1);
      expect(internalFindByIds).toHaveBeenCalledTimes(2);
    } finally {
      await coordinator.close();
    }
  });

  it('keeps post-lookup group execution bounded after resolving one disjoint wave', async () => {
    vi.mocked(internalFindByIds).mockResolvedValue([] as any);

    const coordinator = new BatchEntityCreateCoordinator(context, [0, 1], 1);
    let releaseFirst: (() => void) | undefined;
    const firstGate = new Promise<void>((resolve) => {
      releaseFirst = resolve;
    });
    let firstResolved = false;
    let secondResolved = false;
    const firstPromise = runWithBatchEntityCreateCoordinator(coordinator, 0, async () => {
      const resolution = await resolveBatchEntityCreateLookup({
        finderIds: ['indicator--one'],
        participantIds: ['indicator--one'],
        type: 'Indicator',
      }) as any;
      firstResolved = true;
      await firstGate;
      return resolution;
    });
    const secondPromise = runWithBatchEntityCreateCoordinator(coordinator, 1, async () => {
      const resolution = await resolveBatchEntityCreateLookup({
        finderIds: ['indicator--two'],
        participantIds: ['indicator--two'],
        type: 'Indicator',
      }) as any;
      secondResolved = true;
      return resolution;
    });

    for (let tick = 0; tick < 10 && !firstResolved; tick += 1) {
      await new Promise((resolve) => setTimeout(resolve, 0));
    }
    expect(firstResolved).toBe(true);
    expect(secondResolved).toBe(false);
    expect(internalFindByIds).toHaveBeenCalledTimes(1);

    releaseFirst?.();
    await Promise.all([firstPromise, secondPromise]);
    await coordinator.close();

    expect(secondResolved).toBe(true);
    expect(internalFindByIds).toHaveBeenCalledTimes(1);
  });

  it('serializes overlapping participant ids into later lookup waves', async () => {
    vi.mocked(internalFindByIds)
      .mockResolvedValueOnce([] as any)
      .mockResolvedValueOnce([{
        internal_id: 'indicator--internal-one',
        standard_id: 'indicator--one',
        entity_type: 'Indicator',
      }] as any);

    const coordinator = new BatchEntityCreateCoordinator(context, [0, 1]);
    let releaseFirst: (() => void) | undefined;
    const firstGate = new Promise<void>((resolve) => {
      releaseFirst = resolve;
    });
    let firstResolved = false;
    let secondResolved = false;
    const firstPromise = runWithBatchEntityCreateCoordinator(coordinator, 0, async () => {
      const resolution = await resolveBatchEntityCreateLookup({
        finderIds: ['indicator--one'],
        participantIds: ['indicator--one'],
        type: 'Indicator',
      }) as any;
      firstResolved = true;
      await firstGate;
      return resolution;
    });
    const secondPromise = runWithBatchEntityCreateCoordinator(coordinator, 1, async () => {
      const resolution = await resolveBatchEntityCreateLookup({
        finderIds: ['indicator--one'],
        participantIds: ['indicator--one'],
        type: 'Indicator',
      }) as any;
      secondResolved = true;
      return resolution;
    });

    for (let tick = 0; tick < 10 && !firstResolved; tick += 1) {
      await new Promise((resolve) => setTimeout(resolve, 0));
    }
    expect(firstResolved).toBe(true);
    expect(secondResolved).toBe(false);
    expect(internalFindByIds).toHaveBeenCalledTimes(1);

    releaseFirst?.();
    const [, second] = await Promise.all([firstPromise, secondPromise]);
    await coordinator.close();

    expect(internalFindByIds).toHaveBeenCalledTimes(2);
    expect(second.existingByIds.map((element: any) => element.internal_id)).toEqual(['indicator--internal-one']);
  });

  it('serializes disjoint inputs that resolve to the same existing entity', async () => {
    vi.mocked(internalFindByIds).mockResolvedValue([{
      internal_id: 'indicator--internal-one',
      standard_id: 'indicator--one',
      x_opencti_stix_ids: ['indicator--two'],
      entity_type: 'Indicator',
    }] as any);

    const coordinator = new BatchEntityCreateCoordinator(context, [0, 1]);
    let releaseFirst: (() => void) | undefined;
    const firstGate = new Promise<void>((resolve) => {
      releaseFirst = resolve;
    });
    let firstResolved = false;
    let secondResolved = false;
    const firstPromise = runWithBatchEntityCreateCoordinator(coordinator, 0, async () => {
      const resolution = await resolveBatchEntityCreateLookup({
        finderIds: ['indicator--one'],
        participantIds: ['indicator--one'],
        type: 'Indicator',
      }) as any;
      firstResolved = true;
      await firstGate;
      return resolution;
    });
    const secondPromise = runWithBatchEntityCreateCoordinator(coordinator, 1, async () => {
      const resolution = await resolveBatchEntityCreateLookup({
        finderIds: ['indicator--two'],
        participantIds: ['indicator--two'],
        type: 'Indicator',
      }) as any;
      secondResolved = true;
      return resolution;
    });

    for (let tick = 0; tick < 10 && !firstResolved; tick += 1) {
      await new Promise((resolve) => setTimeout(resolve, 0));
    }
    expect(firstResolved).toBe(true);
    expect(secondResolved).toBe(false);
    expect(lockResources).toHaveBeenCalledTimes(2);
    expect(lockResources).toHaveBeenNthCalledWith(1, ['indicator--one', 'indicator--two'], { draftId: undefined });
    expect(lockResources).toHaveBeenNthCalledWith(2, ['indicator--internal-one'], { draftId: undefined });
    expect(internalFindByIds).toHaveBeenCalledTimes(1);

    releaseFirst?.();
    await Promise.all([firstPromise, secondPromise]);
    await coordinator.close();

    expect(secondResolved).toBe(true);
  });

  it('releases prior logical reservations before a group waits on its next lookup', async () => {
    vi.mocked(internalFindByIds).mockResolvedValue([] as any);

    const coordinator = new BatchEntityCreateCoordinator(context, [0, 1]);
    let releaseSecondWave: (() => void) | undefined;
    const secondWaveGate = new Promise<void>((resolve) => {
      releaseSecondWave = resolve;
    });
    let firstWaveResolved = 0;
    let firstSecondLookupResolved = false;
    let secondSecondLookupResolved = false;
    const firstPromise = runWithBatchEntityCreateCoordinator(coordinator, 0, async () => {
      await resolveBatchEntityCreateLookup({
        finderIds: ['indicator--one'],
        participantIds: ['indicator--one'],
        type: 'Indicator',
      });
      firstWaveResolved += 1;
      await secondWaveGate;
      await resolveBatchEntityCreateLookup({
        finderIds: ['indicator--two'],
        participantIds: ['indicator--two'],
        type: 'Indicator',
      });
      firstSecondLookupResolved = true;
    });
    const secondPromise = runWithBatchEntityCreateCoordinator(coordinator, 1, async () => {
      await resolveBatchEntityCreateLookup({
        finderIds: ['indicator--two'],
        participantIds: ['indicator--two'],
        type: 'Indicator',
      });
      firstWaveResolved += 1;
      await secondWaveGate;
      await resolveBatchEntityCreateLookup({
        finderIds: ['indicator--two'],
        participantIds: ['indicator--two'],
        type: 'Indicator',
      });
      secondSecondLookupResolved = true;
    });

    for (let tick = 0; tick < 10 && firstWaveResolved < 2; tick += 1) {
      await new Promise((resolve) => setTimeout(resolve, 0));
    }
    expect(firstWaveResolved).toBe(2);

    releaseSecondWave?.();
    await Promise.all([firstPromise, secondPromise]);
    await coordinator.close();

    expect(firstSecondLookupResolved).toBe(true);
    expect(secondSecondLookupResolved).toBe(true);
    expect(internalFindByIds).toHaveBeenCalledTimes(3);
  });

  it('retains earlier group-owned ids for nested writes after later lookups', async () => {
    vi.mocked(internalFindByIds).mockResolvedValue([] as any);

    const coordinator = new BatchEntityCreateCoordinator(context, [0]);
    let heldAfterFirstLookup: string[] = [];
    let heldAfterSecondLookup: string[] = [];
    await runWithBatchEntityCreateCoordinator(coordinator, 0, async () => {
      await resolveBatchEntityCreateLookup({
        finderIds: ['external-reference--one'],
        participantIds: ['external-reference--one'],
        type: 'External-Reference',
      });
      heldAfterFirstLookup = getBatchEntityCreateCoordinatorHeldParticipantIds().sort();
      await resolveBatchEntityCreateLookup({
        finderIds: ['course-of-action--one'],
        participantIds: ['course-of-action--one'],
        type: 'Course-Of-Action',
      });
      heldAfterSecondLookup = getBatchEntityCreateCoordinatorHeldParticipantIds().sort();
    });
    await coordinator.close();

    expect(heldAfterFirstLookup).toEqual(['external-reference--one']);
    expect(heldAfterSecondLookup).toEqual(['course-of-action--one', 'external-reference--one']);
  });

  it('keeps group-owned locks until the enclosing batch commit finishes', async () => {
    vi.mocked(internalFindByIds).mockResolvedValue([] as any);
    const acquiredLock = buildLock();
    vi.mocked(lockResources).mockResolvedValue(acquiredLock as any);

    await executeBatchMutations([{
      kind: BatchMutationKind.CreateEntity,
      executeWrite: async () => {
        const coordinator = new BatchEntityCreateCoordinator(context, [0]);
        await runWithBatchEntityCreateCoordinator(coordinator, 0, async () => {
          await resolveBatchEntityCreateLookup({
            finderIds: ['identity--one'],
            participantIds: ['identity--one'],
            type: 'Identity',
          });
          registerBatchCreatedEntity({
            internal_id: 'identity--internal-one',
            standard_id: 'identity--one',
            entity_type: 'Identity',
          } as any);
        });
        await coordinator.close();

        expect(acquiredLock.unlock).not.toHaveBeenCalled();
        expect(getBatchRetainedLockIds()).toEqual(['identity--one']);
        expect(lockResources).toHaveBeenCalledWith(['identity--one'], {
          draftId: undefined,
          retryCount: Number(conf.get('app:concurrency:batch_retry_count')),
        });
        return null;
      },
    }]);

    expect(acquiredLock.unlock).toHaveBeenCalledTimes(1);
  });

  it('releases group-owned locks for persisted entities when the phase closes', async () => {
    vi.mocked(internalFindByIds).mockResolvedValue([{
      internal_id: 'identity--internal-one',
      standard_id: 'identity--one',
      entity_type: 'Identity',
    }] as any);
    const acquiredLocks: ReturnType<typeof buildLock>[] = [];
    vi.mocked(lockResources).mockImplementation(async () => {
      const lock = buildLock();
      acquiredLocks.push(lock);
      return lock as any;
    });

    await executeBatchMutations([{
      kind: BatchMutationKind.CreateEntity,
      executeWrite: async () => {
        const coordinator = new BatchEntityCreateCoordinator(context, [0]);
        await runWithBatchEntityCreateCoordinator(coordinator, 0, async () => {
          await resolveBatchEntityCreateLookup({
            finderIds: ['identity--one'],
            participantIds: ['identity--one'],
            type: 'Identity',
          });
        });
        await coordinator.close();

        expect(getBatchRetainedLockIds()).toEqual([]);
        expect(acquiredLocks).toHaveLength(2);
        acquiredLocks.forEach((lock) => expect(lock.unlock).toHaveBeenCalledTimes(1));
        return null;
      },
    }]);

    acquiredLocks.forEach((lock) => expect(lock.unlock).toHaveBeenCalledTimes(1));
  });

  it('reuses outer-batch retained participant ids in later coordinator phases', async () => {
    vi.mocked(internalFindByIds).mockResolvedValue([] as any);
    const acquiredLock = buildLock();
    vi.mocked(lockResources).mockResolvedValue(acquiredLock as any);

    await executeBatchMutations([{
      kind: BatchMutationKind.CreateEntity,
      executeWrite: async () => {
        const firstCoordinator = new BatchEntityCreateCoordinator(context, [0]);
        await runWithBatchEntityCreateCoordinator(firstCoordinator, 0, async () => {
          await resolveBatchEntityCreateLookup({
            finderIds: ['identity--one'],
            participantIds: ['identity--one'],
            type: 'Identity',
          });
          registerBatchCreatedEntity({
            internal_id: 'identity--internal-one',
            standard_id: 'identity--one',
            entity_type: 'Identity',
          } as any);
        });
        await firstCoordinator.close();

        const secondCoordinator = new BatchEntityCreateCoordinator(context, [1]);
        await runWithBatchEntityCreateCoordinator(secondCoordinator, 1, async () => {
          await resolveBatchEntityCreateLookup({
            finderIds: ['identity--one'],
            participantIds: ['identity--one'],
            type: 'Identity',
          });
          expect(getBatchEntityCreateCoordinatorHeldParticipantIds()).toEqual(['identity--one']);
        });
        await secondCoordinator.close();

        expect(lockResources).toHaveBeenCalledTimes(1);
        expect(acquiredLock.unlock).not.toHaveBeenCalled();
        return null;
      },
    }]);

    expect(acquiredLock.unlock).toHaveBeenCalledTimes(1);
  });

  it('coordinates lock-only relation participants without issuing an entity lookup', async () => {
    vi.mocked(internalFindByIds).mockResolvedValue([] as any);
    const acquiredLocks: ReturnType<typeof buildLock>[] = [];
    vi.mocked(lockResources).mockImplementation(async () => {
      const lock = buildLock();
      acquiredLocks.push(lock);
      return lock as any;
    });

    const coordinator = new BatchEntityCreateCoordinator(context, [0, 1]);
    let releaseFirst: (() => void) | undefined;
    const firstGate = new Promise<void>((resolve) => {
      releaseFirst = resolve;
    });
    let firstResolved = false;
    let secondResolved = false;
    const firstPromise = runWithBatchEntityCreateCoordinator(coordinator, 0, async () => {
      const lock = await resolveBatchParticipantLock({
        participantIds: ['relationship--one'],
      }) as any;
      firstResolved = true;
      await firstGate;
      await lock.unlock();
    });
    const secondPromise = runWithBatchEntityCreateCoordinator(coordinator, 1, async () => {
      const lock = await resolveBatchParticipantLock({
        participantIds: ['relationship--one'],
      }) as any;
      secondResolved = true;
      await lock.unlock();
    });

    for (let tick = 0; tick < 10 && !firstResolved; tick += 1) {
      await new Promise((resolve) => setTimeout(resolve, 0));
    }
    expect(firstResolved).toBe(true);
    expect(secondResolved).toBe(false);
    expect(internalFindByIds).not.toHaveBeenCalled();

    releaseFirst?.();
    await Promise.all([firstPromise, secondPromise]);
    await coordinator.close();

    expect(secondResolved).toBe(true);
    expect(lockResources).toHaveBeenCalledTimes(2);
    expect(lockResources).toHaveBeenNthCalledWith(1, ['relationship--one'], { draftId: undefined });
    expect(lockResources).toHaveBeenNthCalledWith(2, ['relationship--one'], { draftId: undefined });
    expect(acquiredLocks[0].unlock).toHaveBeenCalledTimes(1);
  });

  it('serializes later lock-only lookups that discover the same persisted relation id', async () => {
    vi.mocked(internalFindByIds).mockResolvedValue([] as any);

    const coordinator = new BatchEntityCreateCoordinator(context, [0, 1]);
    let releaseFirstShared: (() => void) | undefined;
    const firstSharedGate = new Promise<void>((resolve) => {
      releaseFirstShared = resolve;
    });
    let releaseFirstGroup: (() => void) | undefined;
    const firstGroupGate = new Promise<void>((resolve) => {
      releaseFirstGroup = resolve;
    });
    let firstResolvedSharedId = false;
    let secondResolvedSharedId = false;
    const firstPromise = runWithBatchEntityCreateCoordinator(coordinator, 0, async () => {
      const plannedLock = await resolveBatchParticipantLock({
        participantIds: ['relationship--planned-one'],
      }) as any;
      const sharedLock = await resolveBatchParticipantLock({
        participantIds: ['relationship--internal-shared'],
      }) as any;
      firstResolvedSharedId = true;
      await firstSharedGate;
      await sharedLock.unlock();
      await firstGroupGate;
      await plannedLock.unlock();
    });
    const secondPromise = runWithBatchEntityCreateCoordinator(coordinator, 1, async () => {
      const plannedLock = await resolveBatchParticipantLock({
        participantIds: ['relationship--planned-two'],
      }) as any;
      const sharedLock = await resolveBatchParticipantLock({
        participantIds: ['relationship--internal-shared'],
      }) as any;
      secondResolvedSharedId = true;
      await sharedLock.unlock();
      await plannedLock.unlock();
    });

    for (let tick = 0; tick < 10 && !firstResolvedSharedId; tick += 1) {
      await new Promise((resolve) => setTimeout(resolve, 0));
    }
    expect(firstResolvedSharedId).toBe(true);
    expect(secondResolvedSharedId).toBe(false);

    releaseFirstShared?.();
    for (let tick = 0; tick < 10 && !secondResolvedSharedId; tick += 1) {
      await new Promise((resolve) => setTimeout(resolve, 0));
    }
    expect(secondResolvedSharedId).toBe(true);

    releaseFirstGroup?.();
    await Promise.all([firstPromise, secondPromise]);
    await coordinator.close();

    expect(lockResources).toHaveBeenCalledTimes(4);
    expect(lockResources).toHaveBeenNthCalledWith(1, ['relationship--planned-one'], { draftId: undefined });
    expect(lockResources).toHaveBeenNthCalledWith(2, ['relationship--planned-two'], { draftId: undefined });
    expect(lockResources).toHaveBeenNthCalledWith(3, ['relationship--internal-shared'], { draftId: undefined });
    expect(lockResources).toHaveBeenNthCalledWith(4, ['relationship--internal-shared'], { draftId: undefined });
  });

  it('does not require a phase-wide barrier for later lock-only lookups', async () => {
    vi.mocked(internalFindByIds).mockResolvedValue([] as any);

    const coordinator = new BatchEntityCreateCoordinator(context, [0, 1]);
    let releaseSecond: (() => void) | undefined;
    const secondGate = new Promise<void>((resolve) => {
      releaseSecond = resolve;
    });
    let firstResolvedAdditionalId = false;
    let secondResolvedInitialId = false;
    const firstPromise = runWithBatchEntityCreateCoordinator(coordinator, 0, async () => {
      const plannedLock = await resolveBatchParticipantLock({
        participantIds: ['relationship--planned-one'],
      }) as any;
      const additionalLock = await resolveBatchParticipantLock({
        participantIds: ['relationship--internal-one'],
      }) as any;
      firstResolvedAdditionalId = true;
      await additionalLock.unlock();
      await plannedLock.unlock();
    });
    const secondPromise = runWithBatchEntityCreateCoordinator(coordinator, 1, async () => {
      const lock = await resolveBatchParticipantLock({
        participantIds: ['relationship--planned-two'],
      }) as any;
      secondResolvedInitialId = true;
      await secondGate;
      await lock.unlock();
    });

    for (let tick = 0; tick < 10 && (!firstResolvedAdditionalId || !secondResolvedInitialId); tick += 1) {
      await new Promise((resolve) => setTimeout(resolve, 0));
    }
    expect(secondResolvedInitialId).toBe(true);
    expect(firstResolvedAdditionalId).toBe(true);

    releaseSecond?.();
    await Promise.all([firstPromise, secondPromise]);
    await coordinator.close();

    expect(lockResources).toHaveBeenCalledTimes(3);
    expect(lockResources).toHaveBeenNthCalledWith(1, ['relationship--planned-one'], { draftId: undefined });
    expect(lockResources).toHaveBeenNthCalledWith(2, ['relationship--planned-two'], { draftId: undefined });
    expect(lockResources).toHaveBeenNthCalledWith(3, ['relationship--internal-one'], { draftId: undefined });
  });

  it('parks crossed follow-on participant waits so one active group can finish', async () => {
    vi.mocked(internalFindByIds).mockResolvedValue([] as any);

    const coordinator = new BatchEntityCreateCoordinator(context, [0, 1]);
    let releaseFollowOn: (() => void) | undefined;
    const followOnGate = new Promise<void>((resolve) => {
      releaseFollowOn = resolve;
    });
    let initialLookupCount = 0;
    let followOnLockCount = 0;
    const runGroup = async (ownedId: string, followOnId: string) => {
      await resolveBatchEntityCreateLookup({
        finderIds: [ownedId],
        participantIds: [ownedId],
        type: 'Indicator',
      });
      initialLookupCount += 1;
      await followOnGate;
      const followOnLock = await resolveBatchParticipantLock({
        participantIds: [followOnId],
      }) as any;
      followOnLockCount += 1;
      await followOnLock.unlock();
    };

    const executionPromise = Promise.all([
      runWithBatchEntityCreateCoordinator(coordinator, 0, async () => runGroup('indicator--one', 'indicator--two')),
      runWithBatchEntityCreateCoordinator(coordinator, 1, async () => runGroup('indicator--two', 'indicator--one')),
    ]);

    for (let tick = 0; tick < 10 && initialLookupCount < 2; tick += 1) {
      await new Promise((resolve) => setTimeout(resolve, 0));
    }
    expect(initialLookupCount).toBe(2);

    releaseFollowOn?.();
    await Promise.race([
      executionPromise,
      new Promise<never>((_, reject) => {
        setTimeout(() => reject(new Error('crossed follow-on participant wait deadlocked')), 1000);
      }),
    ]);
    await coordinator.close();

    expect(followOnLockCount).toBe(2);
  });

  it('releases lock-only reservations between operations to avoid crossed follow-on waits', async () => {
    vi.mocked(internalFindByIds).mockResolvedValue([] as any);

    const coordinator = new BatchEntityCreateCoordinator(context, [0, 1]);
    let releaseSecondOperation: (() => void) | undefined;
    const secondOperationGate = new Promise<void>((resolve) => {
      releaseSecondOperation = resolve;
    });
    let firstOperationCount = 0;
    let secondOperationCount = 0;
    const firstPromise = runWithBatchEntityCreateCoordinator(coordinator, 0, async () => {
      const firstLock = await resolveBatchParticipantLock({
        participantIds: ['relationship--internal-one'],
      }) as any;
      firstOperationCount += 1;
      await firstLock.unlock();
      await secondOperationGate;
      const secondLock = await resolveBatchParticipantLock({
        participantIds: ['relationship--internal-two'],
      }) as any;
      secondOperationCount += 1;
      await secondLock.unlock();
    });
    const secondPromise = runWithBatchEntityCreateCoordinator(coordinator, 1, async () => {
      const firstLock = await resolveBatchParticipantLock({
        participantIds: ['relationship--internal-two'],
      }) as any;
      firstOperationCount += 1;
      await firstLock.unlock();
      await secondOperationGate;
      const secondLock = await resolveBatchParticipantLock({
        participantIds: ['relationship--internal-one'],
      }) as any;
      secondOperationCount += 1;
      await secondLock.unlock();
    });

    for (let tick = 0; tick < 10 && firstOperationCount < 2; tick += 1) {
      await new Promise((resolve) => setTimeout(resolve, 0));
    }
    expect(firstOperationCount).toBe(2);

    releaseSecondOperation?.();
    await Promise.all([firstPromise, secondPromise]);
    await coordinator.close();

    expect(secondOperationCount).toBe(2);
    expect(lockResources).toHaveBeenCalledTimes(4);
    expect(lockResources).toHaveBeenNthCalledWith(1, ['relationship--internal-one'], { draftId: undefined });
    expect(lockResources).toHaveBeenNthCalledWith(2, ['relationship--internal-two'], { draftId: undefined });
    expect(lockResources).toHaveBeenNthCalledWith(3, ['relationship--internal-two'], { draftId: undefined });
    expect(lockResources).toHaveBeenNthCalledWith(4, ['relationship--internal-one'], { draftId: undefined });
  });

});
