import { beforeEach, describe, expect, it, vi } from 'vitest';
import { internalFindByIds } from '../../../../src/database/middleware-loader';
import { lockResources } from '../../../../src/lock/master-lock';
import {
  BatchEntityCreateCoordinator,
  getBatchEntityCreateCoordinatorHeldParticipantIds,
  resolveBatchEntityCreateLookup,
  resolveBatchParticipantLock,
  runWithBatchEntityCreateCoordinator,
} from '../../../../src/modules/batch/batch-entity-create-coordinator';

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

  it('coordinates lock-only relation participants without issuing an entity lookup', async () => {
    vi.mocked(internalFindByIds).mockResolvedValue([] as any);

    const coordinator = new BatchEntityCreateCoordinator(context, [0, 1]);
    let releaseFirst: (() => void) | undefined;
    const firstGate = new Promise<void>((resolve) => {
      releaseFirst = resolve;
    });
    let firstResolved = false;
    let secondResolved = false;
    const firstPromise = runWithBatchEntityCreateCoordinator(coordinator, 0, async () => {
      await resolveBatchParticipantLock({
        participantIds: ['relationship--one'],
      });
      firstResolved = true;
      await firstGate;
    });
    const secondPromise = runWithBatchEntityCreateCoordinator(coordinator, 1, async () => {
      await resolveBatchParticipantLock({
        participantIds: ['relationship--one'],
      });
      secondResolved = true;
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
    expect(lockResources).toHaveBeenCalledTimes(1);
    expect(lockResources).toHaveBeenCalledWith(['relationship--one'], { draftId: undefined });
  });
});
