import { beforeEach, describe, expect, it, vi } from 'vitest';
import { askElementEnrichmentForConnectors } from '../../../src/domain/stixCoreObject';
import { stixBundleByIdStringify, storeLoadByIdWithRefs } from '../../../src/database/middleware';
import { storeLoadByIds } from '../../../src/database/middleware-loader';
import { createWork } from '../../../src/domain/work';
import { pushToConnector } from '../../../src/database/rabbitmq';
import { getDraftContext } from '../../../src/utils/draftContext';
import { convertStoreToStix_2_1 } from '../../../src/database/stix-2-1-converter';
import { publishUserAction } from '../../../src/listener/UserActionListener';
import type { AuthContext, AuthUser } from '../../../src/types/user';

vi.mock('../../../src/database/middleware', () => ({
  buildRestrictedEntity: vi.fn(),
  createEntity: vi.fn(),
  createRelationRaw: vi.fn(),
  deleteElementById: vi.fn(),
  distributionEntities: vi.fn(),
  stixBundleByIdStringify: vi.fn(),
  storeLoadByIdWithRefs: vi.fn(),
  timeSeriesEntities: vi.fn(),
}));

vi.mock('../../../src/database/middleware-loader', () => ({
  fullEntitiesList: vi.fn(),
  internalFindByIds: vi.fn(),
  internalLoadById: vi.fn(),
  pageEntitiesConnection: vi.fn(),
  pageRegardingEntitiesConnection: vi.fn(),
  storeLoadById: vi.fn(),
  storeLoadByIds: vi.fn(),
}));

vi.mock('../../../src/domain/work', () => ({
  createWork: vi.fn(),
  worksForSource: vi.fn(),
  workToExportFile: vi.fn(),
}));

vi.mock('../../../src/database/rabbitmq', () => ({
  pushToConnector: vi.fn(),
}));

vi.mock('../../../src/utils/draftContext', () => ({
  getDraftContext: vi.fn(),
}));

vi.mock('../../../src/database/stix-2-1-converter', () => ({
  convertStoreToStix_2_1: vi.fn(),
}));

vi.mock('../../../src/listener/UserActionListener', () => ({
  buildContextDataForFile: vi.fn(),
  completeContextDataForEntity: vi.fn().mockReturnValue({}),
  publishUserAction: vi.fn(),
}));

vi.mock('../../../src/database/entity-representative', () => ({
  extractEntityRepresentativeName: vi.fn().mockReturnValue('Indicator test'),
  extractRepresentative: vi.fn(),
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
    id: 'connector-deferred',
    internal_id: 'connector-deferred',
    name: 'Deferred',
    enrichment_resolution: 'deferred',
  },
  {
    id: 'connector-none',
    internal_id: 'connector-none',
    name: 'None',
    enrichment_resolution: 'none',
  },
  {
    id: 'connector-bundle',
    internal_id: 'connector-bundle',
    name: 'Bundle',
    enrichment_resolution: 'stix_bundle',
  },
];

describe('askElementEnrichmentForConnectors', () => {
  beforeEach(() => {
    vi.clearAllMocks();
    vi.mocked(getDraftContext).mockReturnValue('');
    vi.mocked(storeLoadByIds).mockResolvedValue(connectors as never);
    vi.mocked(storeLoadByIdWithRefs).mockResolvedValue(element as never);
    vi.mocked(convertStoreToStix_2_1).mockReturnValue({ id: 'indicator--test' } as never);
    vi.mocked(stixBundleByIdStringify).mockResolvedValue('{"id":"bundle--first"}');
    vi.mocked(createWork)
      .mockResolvedValueOnce({ id: 'work-deferred' } as never)
      .mockResolvedValueOnce({ id: 'work-none' } as never)
      .mockResolvedValueOnce({ id: 'work-bundle' } as never);
    vi.mocked(pushToConnector).mockResolvedValue(undefined as never);
    vi.mocked(publishUserAction).mockResolvedValue(undefined as never);
  });

  it('skips only deferred payloads while preserving none and stix_bundle modes', async () => {
    await askElementEnrichmentForConnectors(testContext, testUser, element.internal_id, connectors.map((connector) => connector.id));

    expect(convertStoreToStix_2_1).toHaveBeenCalledTimes(1);
    expect(stixBundleByIdStringify).toHaveBeenCalledTimes(1);
    expect(vi.mocked(pushToConnector).mock.calls[0][1].event).toMatchObject({
      stix_entity: null,
      stix_objects: null,
    });
    expect(vi.mocked(pushToConnector).mock.calls[1][1].event).toMatchObject({
      stix_entity: '{"id":"indicator--test"}',
      stix_objects: null,
    });
    expect(vi.mocked(pushToConnector).mock.calls[2][1].event).toMatchObject({
      stix_entity: '{"id":"indicator--test"}',
      stix_objects: '{"id":"bundle--first"}',
    });
  });
});
