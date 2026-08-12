import { afterEach, describe, expect, it, vi } from 'vitest';
import { engine, elPaginate } from '../../../src/database/engine';
import { READ_INDEX_INTERNAL_OBJECTS } from '../../../src/database/utils';
import { ENTITY_TYPE_SETTINGS } from '../../../src/schema/internalObject';
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
});
