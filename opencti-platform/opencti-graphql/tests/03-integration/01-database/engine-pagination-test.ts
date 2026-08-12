import { afterEach, describe, expect, it, vi } from 'vitest';
import { engine, elList, elPaginate } from '../../../src/database/engine';
import { READ_INDEX_INTERNAL_OBJECTS } from '../../../src/database/utils';
import { ENTITY_TYPE_SETTINGS, ENTITY_TYPE_USER } from '../../../src/schema/internalObject';
import { ADMIN_USER, testContext } from '../../utils/testQuery';

const runSettingsPagination = async (options: Record<string, unknown> = {}) => {
  const searchSpy = vi.spyOn(engine as any, 'search');
  const result = await elPaginate(testContext, ADMIN_USER, READ_INDEX_INTERNAL_OBJECTS, {
    types: [ENTITY_TYPE_SETTINGS],
    first: 1,
    ...options,
  });
  const [query] = searchSpy.mock.calls.at(-1) as any[];
  return { query, result };
};

const runUserList = async (options: Record<string, unknown> = {}) => {
  const searchSpy = vi.spyOn(engine as any, 'search');
  const result = await elList(testContext, ADMIN_USER, READ_INDEX_INTERNAL_OBJECTS, {
    types: [ENTITY_TYPE_USER],
    first: 2,
    ...options,
  });
  const queries = searchSpy.mock.calls.map(([query]) => query as any);
  return { queries, result };
};

describe('engine pagination hit counting', () => {
  afterEach(() => {
    vi.restoreAllMocks();
  });

  it('suppresses exact totals for direct raw-array pagination', async () => {
    const { query, result } = await runSettingsPagination({ connectionFormat: false });

    expect(query.track_total_hits).toBe(false);
    expect(Array.isArray(result)).toBe(true);
    expect((result as any[]).length).toBeGreaterThan(0);
  });

  it('keeps exact totals for connection pagination', async () => {
    const { query, result } = await runSettingsPagination();

    expect(query.track_total_hits).toBe(true);
    expect((result as any).pageInfo.globalCount).toBeGreaterThan(0);
  });

  it('keeps exact totals when raw pagination requests result metadata', async () => {
    const { query, result } = await runSettingsPagination({
      connectionFormat: false,
      withResultMeta: true,
    });

    expect(query.track_total_hits).toBe(true);
    expect(Array.isArray((result as any).elements)).toBe(true);
    expect((result as any).total).toBeGreaterThan(0);
  });

  it('suppresses exact totals for plain full-list reads', async () => {
    const { queries, result } = await runUserList();

    expect(queries.length).toBeGreaterThan(1);
    expect(queries.every((query) => query.track_total_hits === false)).toBe(true);
    expect(Array.isArray(result)).toBe(true);
    expect((result as any[]).length).toBeGreaterThan(0);
  });

  it('suppresses exact totals for callback-based full-list reads by default', async () => {
    const callback = vi.fn();
    const { queries } = await runUserList({ callback });

    expect(queries.length).toBeGreaterThan(1);
    expect(queries.every((query) => query.track_total_hits === false)).toBe(true);
    expect(callback).toHaveBeenCalled();
    expect(callback.mock.calls[0][1]).toBe(0);
  });

  it('keeps exact totals for callback-based full-list reads when explicitly requested', async () => {
    const callback = vi.fn();
    const { queries } = await runUserList({ callback, includeTotalCount: true });

    expect(queries.length).toBeGreaterThan(1);
    expect(queries.every((query) => query.track_total_hits === true)).toBe(true);
    expect(callback).toHaveBeenCalled();
    expect(callback.mock.calls[0][1]).toBeGreaterThan(0);
  });
});
