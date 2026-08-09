import { beforeEach, describe, expect, it, vi } from 'vitest';
import type { FilterGroup } from '../../../src/generated/graphql';
import { getEntitiesMapFromCache } from '../../../src/database/cache';
import { isUserCanAccessStixElement } from '../../../src/utils/access';
import { buildResolutionMapForFilterGroup, resolveFilterGroup } from '../../../src/utils/filtering/filtering-resolution';
import { testFilterGroup } from '../../../src/utils/filtering/boolean-logic-engine';
import { prepareStixMatchFilterGroup } from '../../../src/utils/filtering/filtering-stix/stix-filtering';

vi.mock('../../../src/database/cache', () => ({
  getEntitiesMapFromCache: vi.fn(),
}));

vi.mock('../../../src/utils/access', () => ({
  isUserCanAccessStixElement: vi.fn(),
  SYSTEM_USER: { id: 'system-user' },
}));

vi.mock('../../../src/utils/filtering/filtering-resolution', () => ({
  buildResolutionMapForFilterGroup: vi.fn(),
  resolveFilterGroup: vi.fn(),
}));

vi.mock('../../../src/utils/filtering/boolean-logic-engine', () => ({
  testFilterGroup: vi.fn(),
}));

vi.mock('../../../src/utils/filtering/filtering-stix/stix-testers', () => ({
  FILTER_KEY_TESTERS_MAP: { entity_type: vi.fn() },
}));

const context = {} as any;
const user = { id: 'user' } as any;
const filterGroup = {
  mode: 'and',
  filters: [{
    key: ['entity_type'],
    mode: 'or',
    operator: 'eq',
    values: ['Malware'],
  }],
  filterGroups: [],
} as FilterGroup;

describe('prepareStixMatchFilterGroup', () => {
  beforeEach(() => {
    vi.clearAllMocks();
    vi.mocked(getEntitiesMapFromCache).mockResolvedValue(new Map());
    vi.mocked(buildResolutionMapForFilterGroup).mockResolvedValue(new Map());
    vi.mocked(resolveFilterGroup).mockResolvedValue(filterGroup);
    vi.mocked(isUserCanAccessStixElement).mockResolvedValue(true);
    vi.mocked(testFilterGroup).mockReturnValue(true);
  });

  it('reuses prepared filter resolution across accessible stix objects', async () => {
    const matcher = await prepareStixMatchFilterGroup(context, user, filterGroup);

    await expect(matcher({ id: 'malware--1' })).resolves.toBe(true);
    await expect(matcher({ id: 'malware--2' })).resolves.toBe(true);

    expect(getEntitiesMapFromCache).toHaveBeenCalledTimes(1);
    expect(buildResolutionMapForFilterGroup).toHaveBeenCalledTimes(1);
    expect(resolveFilterGroup).toHaveBeenCalledTimes(1);
    expect(isUserCanAccessStixElement).toHaveBeenCalledTimes(2);
    expect(testFilterGroup).toHaveBeenCalledTimes(2);
  });

  it('defers filter resolution until the first accessible stix object', async () => {
    vi.mocked(isUserCanAccessStixElement)
      .mockResolvedValueOnce(false)
      .mockResolvedValueOnce(true);
    const matcher = await prepareStixMatchFilterGroup(context, user, filterGroup);

    await expect(matcher({ id: 'malware--restricted' })).resolves.toBe(false);
    expect(resolveFilterGroup).not.toHaveBeenCalled();

    await expect(matcher({ id: 'malware--visible' })).resolves.toBe(true);
    expect(resolveFilterGroup).toHaveBeenCalledTimes(1);
  });
});
