import { beforeEach, describe, expect, it, vi } from 'vitest';
import { internalFindByIds } from '../../../../src/database/middleware-loader';
import { createExistingEntityIdsBatchLoader, createInputResolveRefsBatchLoader, createStoreLoadByIdWithRefsBatchLoader } from '../../../../src/modules/batch/batch-reference-loader';
import { BatchMutationKind, executeBatchMutations } from '../../../../src/modules/batch/batch-executor';

vi.mock('../../../../src/database/middleware-loader', () => ({
  internalFindByIds: vi.fn(),
}));

describe('batch reference loader', () => {
  const context = {} as any;
  const user = {} as any;

  beforeEach(() => {
    vi.clearAllMocks();
  });

  it('coalesces same-tick shared reference reads into one lookup', async () => {
    vi.mocked(internalFindByIds).mockResolvedValue([
      {
        internal_id: 'identity--internal',
        standard_id: 'identity--shared',
        entity_type: 'Identity',
      },
      {
        internal_id: 'marking-definition--internal',
        standard_id: 'marking-definition--shared',
        entity_type: 'Marking-Definition',
      },
    ] as any);

    const loader = createInputResolveRefsBatchLoader(context, user);
    const [identity, marking] = await Promise.all([
      loader.load('identity--shared'),
      loader.load('marking-definition--shared'),
    ]);

    expect(internalFindByIds).toHaveBeenCalledTimes(1);
    expect(internalFindByIds).toHaveBeenCalledWith(context, user, [
      'identity--shared',
      'marking-definition--shared',
    ]);
    expect(identity).toEqual([expect.objectContaining({ internal_id: 'identity--internal' })]);
    expect(marking).toEqual([expect.objectContaining({ internal_id: 'marking-definition--internal' })]);
  });

  it('preserves duplicate matches for collision handling', async () => {
    vi.mocked(internalFindByIds).mockResolvedValue([
      {
        internal_id: 'identity--one',
        standard_id: 'identity--shared',
        entity_type: 'Identity',
      },
      {
        internal_id: 'identity--two',
        standard_id: 'identity--shared',
        entity_type: 'Identity',
      },
    ] as any);

    const loader = createInputResolveRefsBatchLoader(context, user);
    const matches = await loader.load('identity--shared');

    expect(matches.map((element) => element.internal_id)).toEqual(['identity--one', 'identity--two']);
  });

  it('reuses shared reference reads across ticks until an affected id is invalidated', async () => {
    vi.mocked(internalFindByIds)
      .mockResolvedValueOnce([{
        internal_id: 'identity--one',
        standard_id: 'identity--shared',
        entity_type: 'Identity',
      }] as any)
      .mockResolvedValueOnce([{
        internal_id: 'identity--two',
        standard_id: 'identity--shared',
        entity_type: 'Identity',
      }] as any);

    const loader = createInputResolveRefsBatchLoader(context, user);
    const first = await loader.load('identity--shared');
    const second = await loader.load('identity--shared');
    loader.invalidate(['identity--one']);
    const third = await loader.load('identity--shared');

    expect(internalFindByIds).toHaveBeenCalledTimes(2);
    expect(first[0].internal_id).toBe('identity--one');
    expect(second[0].internal_id).toBe('identity--one');
    expect(third[0].internal_id).toBe('identity--two');
  });

  it('does not reuse shared reference reads across top-level batch executions', async () => {
    vi.mocked(internalFindByIds)
      .mockResolvedValueOnce([] as any)
      .mockResolvedValueOnce([{
        internal_id: 'identity--one',
        standard_id: 'identity--shared',
        entity_type: 'Identity',
      }] as any);

    const loader = createInputResolveRefsBatchLoader(context, user);
    const first = await executeBatchMutations([{
      kind: BatchMutationKind.CreateEntity,
      executeWrite: () => loader.load('identity--shared'),
    }]);
    const second = await executeBatchMutations([{
      kind: BatchMutationKind.CreateEntity,
      executeWrite: () => loader.load('identity--shared'),
    }]);

    expect(internalFindByIds).toHaveBeenCalledTimes(2);
    expect(first.results[0]).toEqual([]);
    expect(second.results[0].map((element) => element.internal_id)).toEqual(['identity--one']);
  });

  it('coalesces same-type entity existence probes and maps results back to each input', async () => {
    vi.mocked(internalFindByIds).mockResolvedValue([
      {
        internal_id: 'indicator--one',
        standard_id: 'indicator--standard-one',
        entity_type: 'Indicator',
      },
      {
        internal_id: 'indicator--two',
        standard_id: 'indicator--standard-two',
        entity_type: 'Indicator',
      },
    ] as any);

    const loader = createExistingEntityIdsBatchLoader(context, user);
    const [first, second] = await Promise.all([
      loader.load({ ids: ['indicator--standard-one'], type: 'Indicator' }),
      loader.load({ ids: ['indicator--standard-two'], type: 'Indicator' }),
    ]);

    expect(internalFindByIds).toHaveBeenCalledTimes(1);
    expect(internalFindByIds).toHaveBeenCalledWith(context, user, [
      'indicator--standard-one',
      'indicator--standard-two',
    ], { type: 'Indicator' });
    expect(first.map((element) => element.internal_id)).toEqual(['indicator--one']);
    expect(second.map((element) => element.internal_id)).toEqual(['indicator--two']);
  });

  it('keeps different entity types in separate existence probes', async () => {
    vi.mocked(internalFindByIds)
      .mockResolvedValueOnce([{
        internal_id: 'indicator--one',
        standard_id: 'indicator--standard-one',
        entity_type: 'Indicator',
      }] as any)
      .mockResolvedValueOnce([{
        internal_id: 'identity--one',
        standard_id: 'identity--standard-one',
        entity_type: 'Identity',
      }] as any);

    const loader = createExistingEntityIdsBatchLoader(context, user);
    await Promise.all([
      loader.load({ ids: ['indicator--standard-one'], type: 'Indicator' }),
      loader.load({ ids: ['identity--standard-one'], type: 'Identity' }),
    ]);

    expect(internalFindByIds).toHaveBeenCalledTimes(2);
    expect(internalFindByIds).toHaveBeenCalledWith(context, user, ['indicator--standard-one'], { type: 'Indicator' });
    expect(internalFindByIds).toHaveBeenCalledWith(context, user, ['identity--standard-one'], { type: 'Identity' });
  });

  it('reuses entity existence probes across ticks until an affected id is invalidated', async () => {
    vi.mocked(internalFindByIds)
      .mockResolvedValueOnce([] as any)
      .mockResolvedValueOnce([{
        internal_id: 'indicator--one',
        standard_id: 'indicator--standard-one',
        entity_type: 'Indicator',
      }] as any);

    const loader = createExistingEntityIdsBatchLoader(context, user);
    const first = await loader.load({ ids: ['indicator--standard-one'], type: 'Indicator' });
    const second = await loader.load({ ids: ['indicator--standard-one'], type: 'Indicator' });
    loader.invalidate(['indicator--standard-one']);
    const third = await loader.load({ ids: ['indicator--standard-one'], type: 'Indicator' });

    expect(internalFindByIds).toHaveBeenCalledTimes(2);
    expect(first).toEqual([]);
    expect(second).toEqual([]);
    expect(third.map((element) => element.internal_id)).toEqual(['indicator--one']);
  });

  it('coalesces same-user hydrated reads with matching options', async () => {
    const loadByIdsWithRefs = vi.fn().mockResolvedValue([
      {
        internal_id: 'identity--internal',
        standard_id: 'identity--shared',
        entity_type: 'Identity',
      },
      {
        internal_id: 'marking-definition--internal',
        standard_id: 'marking-definition--shared',
        entity_type: 'Marking-Definition',
      },
    ] as any);

    const loader = createStoreLoadByIdWithRefsBatchLoader(context, loadByIdsWithRefs);
    const [identity, marking] = await Promise.all([
      loader.load({ id: 'identity--shared', opts: { onlyMarking: false }, user }),
      loader.load({ id: 'marking-definition--shared', opts: { onlyMarking: false }, user }),
    ]);

    expect(loadByIdsWithRefs).toHaveBeenCalledTimes(1);
    expect(loadByIdsWithRefs).toHaveBeenCalledWith(context, user, [
      'identity--shared',
      'marking-definition--shared',
    ], { onlyMarking: false });
    expect(identity).toEqual(expect.objectContaining({ internal_id: 'identity--internal' }));
    expect(marking).toEqual(expect.objectContaining({ internal_id: 'marking-definition--internal' }));
  });

  it('keeps hydrated reads with different users or options in separate probes', async () => {
    const otherUser = {} as any;
    const loadByIdsWithRefs = vi.fn()
      .mockResolvedValueOnce([{
        internal_id: 'identity--one',
        standard_id: 'identity--shared',
        entity_type: 'Identity',
      }] as any)
      .mockResolvedValueOnce([{
        internal_id: 'indicator--one',
        standard_id: 'indicator--shared',
        entity_type: 'Indicator',
      }] as any)
      .mockResolvedValueOnce([{
        internal_id: 'identity--two',
        standard_id: 'identity--shared-two',
        entity_type: 'Identity',
      }] as any);

    const loader = createStoreLoadByIdWithRefsBatchLoader(context, loadByIdsWithRefs);
    await Promise.all([
      loader.load({ id: 'identity--shared', opts: { type: 'Identity' }, user }),
      loader.load({ id: 'indicator--shared', opts: { type: 'Indicator' }, user }),
      loader.load({ id: 'identity--shared-two', opts: { type: 'Identity' }, user: otherUser }),
    ]);

    expect(loadByIdsWithRefs).toHaveBeenCalledTimes(3);
    expect(loadByIdsWithRefs).toHaveBeenCalledWith(context, user, ['identity--shared'], { type: 'Identity' });
    expect(loadByIdsWithRefs).toHaveBeenCalledWith(context, user, ['indicator--shared'], { type: 'Indicator' });
    expect(loadByIdsWithRefs).toHaveBeenCalledWith(context, otherUser, ['identity--shared-two'], { type: 'Identity' });
  });

  it('reuses hydrated reads across ticks until an affected id is invalidated', async () => {
    const loadByIdsWithRefs = vi.fn()
      .mockResolvedValueOnce([{
        internal_id: 'identity--one',
        standard_id: 'identity--shared',
        entity_type: 'Identity',
      }] as any)
      .mockResolvedValueOnce([{
        internal_id: 'identity--two',
        standard_id: 'identity--shared',
        entity_type: 'Identity',
      }] as any);

    const loader = createStoreLoadByIdWithRefsBatchLoader(context, loadByIdsWithRefs);
    const first = await loader.load({ id: 'identity--shared', user });
    const second = await loader.load({ id: 'identity--shared', user });
    loader.invalidate(['identity--one']);
    const third = await loader.load({ id: 'identity--shared', user });

    expect(loadByIdsWithRefs).toHaveBeenCalledTimes(2);
    expect(first?.internal_id).toBe('identity--one');
    expect(second?.internal_id).toBe('identity--one');
    expect(third?.internal_id).toBe('identity--two');
  });
});
