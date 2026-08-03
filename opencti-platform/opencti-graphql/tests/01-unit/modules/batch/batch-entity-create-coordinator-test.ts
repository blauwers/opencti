import { beforeEach, describe, expect, it, vi } from 'vitest';
import { internalFindByIds } from '../../../../src/database/middleware-loader';
import { lockResources } from '../../../../src/lock/master-lock';
import {
  BatchEntityCreateCoordinator,
  resolveBatchEntityCreateLookup,
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
    const [first, second] = await Promise.all([
      runWithBatchEntityCreateCoordinator(coordinator, 0, async () => resolveBatchEntityCreateLookup({
        finderIds: ['indicator--one'],
        participantIds: ['indicator--one'],
        type: 'Indicator',
      }) as any),
      runWithBatchEntityCreateCoordinator(coordinator, 1, async () => resolveBatchEntityCreateLookup({
        finderIds: ['indicator--two'],
        participantIds: ['indicator--two'],
        type: 'Indicator',
      }) as any),
    ]);
    await coordinator.close();

    expect(lockResources).toHaveBeenCalledTimes(1);
    expect(lockResources).toHaveBeenCalledWith(['indicator--one', 'indicator--two'], { draftId: undefined });
    expect(internalFindByIds).toHaveBeenCalledTimes(1);
    expect(internalFindByIds).toHaveBeenCalledWith(context, expect.anything(), ['indicator--one', 'indicator--two'], { type: 'Indicator' });
    expect(first.existingByIds.map((element: any) => element.internal_id)).toEqual(['indicator--internal-one']);
    expect(second.existingByIds.map((element: any) => element.internal_id)).toEqual(['indicator--internal-two']);
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
});
