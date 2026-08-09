import { beforeEach, describe, expect, it, vi } from 'vitest';
import { createEntityAutoEnrichment } from '../../../src/domain/enrichment';
import { createWork } from '../../../src/domain/work';
import { pushToConnector } from '../../../src/database/rabbitmq';
import { getEntitiesListFromCache } from '../../../src/database/cache';
import { isUserCanAccessStoreElement } from '../../../src/utils/access';
import { getDraftContext } from '../../../src/utils/draftContext';
import { resolveUserByIdFromCache } from '../../../src/domain/user';
import type { AuthContext, AuthUser } from '../../../src/types/user';

vi.mock('../../../src/domain/work', () => ({
  createWork: vi.fn(),
}));

vi.mock('../../../src/database/rabbitmq', () => ({
  pushToConnector: vi.fn(),
}));

vi.mock('../../../src/database/cache', () => ({
  getEntitiesListFromCache: vi.fn(),
}));

vi.mock('../../../src/schema/stixCoreObject', () => ({
  isStixObject: vi.fn().mockReturnValue(true),
}));

vi.mock('../../../src/utils/access', () => ({
  SYSTEM_USER: {},
  isUserCanAccessStoreElement: vi.fn(),
}));

vi.mock('../../../src/utils/draftContext', () => ({
  getDraftContext: vi.fn(),
}));

vi.mock('../../../src/domain/user', () => ({
  resolveUserByIdFromCache: vi.fn(),
}));

const testContext = {} as AuthContext;
const testUser = { id: 'user--test' } as AuthUser;
const element = {
  entity_type: 'Indicator',
  internal_id: 'indicator-internal-id',
  standard_id: 'indicator--test',
};
const connectors = [
  {
    id: 'connector-1',
    internal_id: 'connector-1',
    name: 'Hygiene',
    active: true,
    auto: true,
    connector_type: 'INTERNAL_ENRICHMENT',
    connector_scope: ['Indicator'],
    connector_user_id: 'connector-user-1',
    enrichment_resolution: 'stix_bundle',
  },
  {
    id: 'connector-2',
    internal_id: 'connector-2',
    name: 'Scoring',
    active: true,
    auto: true,
    connector_type: 'INTERNAL_ENRICHMENT',
    connector_scope: ['Indicator'],
    connector_user_id: 'connector-user-2',
    enrichment_resolution: 'stix_bundle',
  },
];

describe('createEntityAutoEnrichment', () => {
  beforeEach(() => {
    vi.clearAllMocks();
    vi.mocked(getDraftContext).mockReturnValue('');
    vi.mocked(getEntitiesListFromCache).mockResolvedValue(connectors as never);
    vi.mocked(resolveUserByIdFromCache).mockResolvedValue(testUser as never);
    vi.mocked(isUserCanAccessStoreElement).mockResolvedValue(true);
    vi.mocked(createWork)
      .mockResolvedValueOnce({ id: 'work-1' } as never)
      .mockResolvedValueOnce({ id: 'work-2' } as never);
    vi.mocked(pushToConnector).mockResolvedValue(undefined as never);
  });

  it('reuses one exact STIX payload snapshot across multiple target connectors', async () => {
    const stixLoaders = {
      loadById: vi.fn()
        .mockResolvedValueOnce('{"id":"indicator--test","snapshot":1}')
        .mockResolvedValueOnce('{"id":"indicator--test","snapshot":2}'),
      bundleById: vi.fn()
        .mockResolvedValueOnce('{"id":"bundle--first"}')
        .mockResolvedValueOnce('{"id":"bundle--second"}'),
    };

    await createEntityAutoEnrichment(testContext, testUser, element, element.entity_type, stixLoaders);

    expect(stixLoaders.loadById).toHaveBeenCalledTimes(1);
    expect(stixLoaders.bundleById).toHaveBeenCalledTimes(1);
    expect(pushToConnector).toHaveBeenCalledTimes(2);
    expect(vi.mocked(pushToConnector).mock.calls[0][1].event).toMatchObject({
      stix_entity: '{"id":"indicator--test","snapshot":1}',
      stix_objects: '{"id":"bundle--first"}',
    });
    expect(vi.mocked(pushToConnector).mock.calls[1][1].event).toMatchObject({
      stix_entity: '{"id":"indicator--test","snapshot":1}',
      stix_objects: '{"id":"bundle--first"}',
    });
  });

  it('keeps entity payload reuse while preserving none connector bundle behavior', async () => {
    vi.mocked(getEntitiesListFromCache).mockResolvedValue([
      connectors[0],
      { ...connectors[1], enrichment_resolution: 'none' },
    ] as never);
    const stixLoaders = {
      loadById: vi.fn().mockResolvedValue('{"id":"indicator--test"}'),
      bundleById: vi.fn().mockResolvedValue('{"id":"bundle--first"}'),
    };

    await createEntityAutoEnrichment(testContext, testUser, element, element.entity_type, stixLoaders);

    expect(stixLoaders.loadById).toHaveBeenCalledTimes(1);
    expect(stixLoaders.bundleById).toHaveBeenCalledTimes(1);
    expect(vi.mocked(pushToConnector).mock.calls[0][1].event).toMatchObject({
      stix_entity: '{"id":"indicator--test"}',
      stix_objects: '{"id":"bundle--first"}',
    });
    expect(vi.mocked(pushToConnector).mock.calls[1][1].event).toMatchObject({
      stix_entity: '{"id":"indicator--test"}',
      stix_objects: null,
    });
  });
});
