import { beforeEach, describe, expect, it, vi } from 'vitest';

const mocks = vi.hoisted(() => ({
  fullRelationsList: vi.fn(),
  elFindByIds: vi.fn(),
}));

vi.mock('../../../src/database/middleware-loader', () => ({
  fullRelationsList: mocks.fullRelationsList,
}));

vi.mock('../../../src/database/engine', () => ({
  elFindByIds: mocks.elFindByIds,
}));

const { resolveNeighborsForFeed } = await import('../../../src/http/httpRollingFeed');

const feed = {
  feed_attributes: [{
    mappings: [{
      relationship_type: 'indicates',
      target_entity_type: 'Malware',
    }],
  }],
} as any;

describe('resolveNeighborsForFeed', () => {
  beforeEach(() => {
    vi.clearAllMocks();
    mocks.elFindByIds.mockImplementation(async (_context, _user, ids: string[]) => Object.fromEntries(
      ids.map((id) => [id, { internal_id: id, name: id }]),
    ));
  });

  it('folds paged relation results without changing from-before-to neighbor order', async () => {
    mocks.fullRelationsList
      .mockImplementationOnce(async (_context, _user, _type, args) => {
        await args.callback([
          { fromId: 'source-1', toId: 'target-1' },
          { fromId: 'source-1', toId: 'target-2' },
        ]);
        await args.callback([{ fromId: 'source-1', toId: 'target-1' }]);
        return [];
      })
      .mockImplementationOnce(async (_context, _user, _type, args) => {
        await args.callback([
          { fromId: 'target-3', toId: 'source-1' },
          { fromId: 'target-1', toId: 'source-1' },
        ]);
        return [];
      });

    const neighbors = await resolveNeighborsForFeed(
      {} as any,
      {} as any,
      [{ internal_id: 'source-1' }],
      feed,
    );

    expect(mocks.fullRelationsList).toHaveBeenCalledTimes(2);
    expect(mocks.fullRelationsList.mock.calls.every(([, , , args]) => typeof args.callback === 'function')).toBe(true);
    expect(mocks.elFindByIds).toHaveBeenCalledWith(
      {} as any,
      {} as any,
      ['target-1', 'target-2', 'target-3'],
      { type: 'Malware', toMap: true },
    );
    expect(neighbors.get('source-1')?.get('indicates:Malware')?.map((neighbor) => neighbor.internal_id)).toEqual([
      'target-1',
      'target-2',
      'target-3',
    ]);
  });
});
