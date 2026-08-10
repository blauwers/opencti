import { beforeEach, describe, expect, it, vi } from 'vitest';
import { BULK_TIMEOUT, elBulk, elFindByIds, elIndex, elLoadById } from '../../../src/database/engine';
import { redisInitializeWork, redisInitializeWorks } from '../../../src/database/redis';
import { createWorks } from '../../../src/domain/work';
import { INDEX_HISTORY, READ_INDEX_HISTORY } from '../../../src/database/utils';
import { ENTITY_TYPE_WORK } from '../../../src/schema/internalObject';

vi.mock('../../../src/database/engine', async (importOriginal) => {
  const actual = await importOriginal<typeof import('../../../src/database/engine')>();
  return {
    ...actual,
    elBulk: vi.fn(),
    elFindByIds: vi.fn(),
    elIndex: vi.fn(),
    elLoadById: vi.fn(),
  };
});

vi.mock('../../../src/database/redis', async (importOriginal) => {
  const actual = await importOriginal<typeof import('../../../src/database/redis')>();
  return {
    ...actual,
    redisInitializeWork: vi.fn(),
    redisInitializeWorks: vi.fn(),
  };
});

describe('createWorks', () => {
  const context = {} as never;
  const user = { id: 'user--1' } as never;
  const connector = {
    connector_name: 'Hygiene',
    connector_type: 'INTERNAL_ENRICHMENT',
    internal_id: 'connector--1',
  };

  beforeEach(() => {
    vi.clearAllMocks();
    vi.mocked(elBulk).mockResolvedValue({ errors: false } as never);
    vi.mocked(elFindByIds).mockImplementation(async (_context, _user, ids) => {
      return (ids as string[]).map((id) => ({
        _index: INDEX_HISTORY,
        entity_type: ENTITY_TYPE_WORK,
        internal_id: id,
      })) as never;
    });
    vi.mocked(elLoadById).mockImplementation(async (_context, _user, id) => ({
      _index: INDEX_HISTORY,
      entity_type: ENTITY_TYPE_WORK,
      internal_id: id,
    }) as never);
    vi.mocked(redisInitializeWork).mockResolvedValue(undefined as never);
    vi.mocked(redisInitializeWorks).mockResolvedValue(undefined as never);
  });

  it('bulk indexes and initializes multiple work documents once', async () => {
    const works = await createWorks(context, user, [
      {
        connector,
        friendlyName: 'First work',
        sourceId: 'indicator--1',
        args: { preallocatedWork: { id: 'work--1', timestamp: '2026-08-10T00:00:00.000Z' } },
      },
      {
        connector,
        friendlyName: 'Second work',
        sourceId: 'indicator--2',
        args: {
          isMultiPartWork: true,
          preallocatedWork: { id: 'work--2', timestamp: '2026-08-10T00:00:01.000Z' },
        },
      },
    ]);

    expect(elIndex).not.toHaveBeenCalled();
    expect(elBulk).toHaveBeenCalledTimes(1);
    expect(elBulk).toHaveBeenCalledWith(context, expect.objectContaining({
      refresh: true,
      timeout: BULK_TIMEOUT,
    }));
    const body = vi.mocked(elBulk).mock.calls[0][1].body;
    expect(body[0]).toEqual({ index: { _index: INDEX_HISTORY, _id: 'work--1' } });
    expect(body[1]).toEqual(expect.objectContaining({
      connector_id: 'connector--1',
      entity_type: ENTITY_TYPE_WORK,
      event_source_id: 'indicator--1',
      internal_id: 'work--1',
      name: 'First work',
    }));
    expect(body[2]).toEqual({ index: { _index: INDEX_HISTORY, _id: 'work--2' } });
    expect(body[3]).toEqual(expect.objectContaining({
      connector_id: 'connector--1',
      entity_type: ENTITY_TYPE_WORK,
      event_source_id: 'indicator--2',
      internal_id: 'work--2',
      is_multipart: true,
      name: 'Second work',
    }));
    expect(elFindByIds).toHaveBeenCalledWith(context, user, ['work--1', 'work--2'], {
      indices: READ_INDEX_HISTORY,
      type: ENTITY_TYPE_WORK,
      withoutRels: false,
    });
    expect(redisInitializeWorks).toHaveBeenCalledWith([
      { workId: 'work--1', isMultiPartWork: false },
      { workId: 'work--2', isMultiPartWork: true },
    ]);
    expect(works.map((work: { id?: string }) => work?.id)).toEqual(['work--1', 'work--2']);
  });

  it('keeps a single work on the singular indexing and initialization path', async () => {
    const works = await createWorks(context, user, [{
      connector,
      friendlyName: 'Only work',
      sourceId: 'indicator--1',
      args: { preallocatedWork: { id: 'work--1', timestamp: '2026-08-10T00:00:00.000Z' } },
    }]);

    expect(elBulk).not.toHaveBeenCalled();
    expect(elIndex).toHaveBeenCalledWith(INDEX_HISTORY, expect.objectContaining({
      internal_id: 'work--1',
      name: 'Only work',
    }), { context });
    expect(redisInitializeWork).toHaveBeenCalledWith('work--1', false);
    expect(redisInitializeWorks).not.toHaveBeenCalled();
    expect(works.map((work: { id?: string }) => work?.id)).toEqual(['work--1']);
  });
});
