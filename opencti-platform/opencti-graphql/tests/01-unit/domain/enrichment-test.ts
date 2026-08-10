import { beforeEach, describe, expect, it, vi } from 'vitest';
import { buildAutoEnrichmentSideEffect, createEntityAutoEnrichment } from '../../../src/domain/enrichment';
import { createWork } from '../../../src/domain/work';
import { pushToConnector } from '../../../src/database/rabbitmq';
import { getEntitiesListFromCache } from '../../../src/database/cache';
import { isUserCanAccessStoreElement } from '../../../src/utils/access';
import { getDraftContext } from '../../../src/utils/draftContext';
import { resolveUserByIdFromCache } from '../../../src/domain/user';
import type { AuthContext, AuthUser } from '../../../src/types/user';
import { executeBatchMutations, BatchMutationKind } from '../../../src/modules/batch/batch-executor';
import {
  DEFAULT_ENRICHMENT_BATCH_CAPABILITY,
  parseEnrichmentBatchEnvelope,
} from '../../../src/modules/enrichment/enrichment-batch-contract';

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

  it('skips server-side STIX payload loading for deferred connectors', async () => {
    vi.mocked(getEntitiesListFromCache).mockResolvedValue(
      connectors.map((connector) => ({ ...connector, enrichment_resolution: 'deferred' })) as never,
    );
    const stixLoaders = {
      loadById: vi.fn().mockResolvedValue('{"id":"indicator--test"}'),
      bundleById: vi.fn().mockResolvedValue('{"id":"bundle--first"}'),
    };

    await createEntityAutoEnrichment(testContext, testUser, element, element.entity_type, stixLoaders);

    expect(stixLoaders.loadById).not.toHaveBeenCalled();
    expect(stixLoaders.bundleById).not.toHaveBeenCalled();
    expect(pushToConnector).toHaveBeenCalledTimes(2);
    expect(vi.mocked(pushToConnector).mock.calls[0][1].event).toMatchObject({
      stix_entity: null,
      stix_objects: null,
    });
    expect(vi.mocked(pushToConnector).mock.calls[1][1].event).toMatchObject({
      stix_entity: null,
      stix_objects: null,
    });
  });

  it('keeps the legacy entity-only payload for existing none connectors', async () => {
    vi.mocked(getEntitiesListFromCache).mockResolvedValue(
      connectors.map((connector) => ({ ...connector, enrichment_resolution: 'none' })) as never,
    );
    const stixLoaders = {
      loadById: vi.fn().mockResolvedValue('{"id":"indicator--test"}'),
      bundleById: vi.fn().mockResolvedValue('{"id":"bundle--first"}'),
    };

    await createEntityAutoEnrichment(testContext, testUser, element, element.entity_type, stixLoaders);

    expect(stixLoaders.loadById).toHaveBeenCalledTimes(1);
    expect(stixLoaders.bundleById).not.toHaveBeenCalled();
    expect(pushToConnector).toHaveBeenCalledTimes(2);
    expect(vi.mocked(pushToConnector).mock.calls[0][1].event).toMatchObject({
      stix_entity: '{"id":"indicator--test"}',
      stix_objects: null,
    });
    expect(vi.mocked(pushToConnector).mock.calls[1][1].event).toMatchObject({
      stix_entity: '{"id":"indicator--test"}',
      stix_objects: null,
    });
  });

  it('coalesces compatible batch-capable enrichment effects into one connector envelope', async () => {
    vi.mocked(getEntitiesListFromCache).mockResolvedValue([{
      ...connectors[0],
      enrichment_batch_capability: DEFAULT_ENRICHMENT_BATCH_CAPABILITY,
    }] as never);
    vi.mocked(createWork)
      .mockReset()
      .mockResolvedValueOnce({ id: 'work-1' } as never)
      .mockResolvedValueOnce({ id: 'work-2' } as never);
    const firstElement = { ...element, standard_id: 'indicator--1', internal_id: 'indicator-internal-1' };
    const secondElement = { ...element, standard_id: 'indicator--2', internal_id: 'indicator-internal-2' };

    await executeBatchMutations([
      {
        kind: BatchMutationKind.CreateEntity,
        executeWrite: async () => firstElement,
        sideEffects: () => [buildAutoEnrichmentSideEffect(testContext, testUser, firstElement, 'create', {
          loadById: vi.fn().mockResolvedValue('{"id":"indicator--1","type":"indicator"}'),
          bundleById: vi.fn().mockResolvedValue('{"type":"bundle","objects":[{"id":"indicator--1","type":"indicator"}]}'),
        })],
      },
      {
        kind: BatchMutationKind.CreateEntity,
        executeWrite: async () => secondElement,
        sideEffects: () => [buildAutoEnrichmentSideEffect(testContext, testUser, secondElement, 'create', {
          loadById: vi.fn().mockResolvedValue('{"id":"indicator--2","type":"indicator"}'),
          bundleById: vi.fn().mockResolvedValue('{"type":"bundle","objects":[{"id":"indicator--2","type":"indicator"}]}'),
        })],
      },
    ]);

    expect(pushToConnector).toHaveBeenCalledTimes(1);
    const message = vi.mocked(pushToConnector).mock.calls[0][1];
    expect(message.internal).toMatchObject({
      work_id: null,
      trigger: 'create',
      mode: 'auto',
    });
    const envelope = parseEnrichmentBatchEnvelope(
      message.event.enrichment_batch,
      DEFAULT_ENRICHMENT_BATCH_CAPABILITY,
    );
    expect(envelope.items.map((item) => item.work_id).sort()).toEqual(['work-1', 'work-2']);
    expect(envelope.items.map((item) => item.entity_id).sort()).toEqual(['indicator--1', 'indicator--2']);
  });

  it('keeps legacy connector requests on independent bounded dispatches inside a batch', async () => {
    vi.mocked(getEntitiesListFromCache).mockResolvedValue([connectors[0]] as never);
    let resolveFirstWork!: (work: { id: string }) => void;
    const firstWork = new Promise<{ id: string }>((resolve) => {
      resolveFirstWork = resolve;
    });
    vi.mocked(createWork)
      .mockReset()
      .mockImplementationOnce(() => firstWork as never)
      .mockResolvedValueOnce({ id: 'work-2' } as never);
    const firstElement = { ...element, standard_id: 'indicator--1', internal_id: 'indicator-internal-1' };
    const secondElement = { ...element, standard_id: 'indicator--2', internal_id: 'indicator-internal-2' };

    const execution = executeBatchMutations([
      {
        kind: BatchMutationKind.CreateEntity,
        executeWrite: async () => firstElement,
        sideEffects: () => [buildAutoEnrichmentSideEffect(testContext, testUser, firstElement, 'create', {
          loadById: vi.fn().mockResolvedValue('{"id":"indicator--1","type":"indicator"}'),
          bundleById: vi.fn().mockResolvedValue('{"type":"bundle","objects":[{"id":"indicator--1","type":"indicator"}]}'),
        })],
      },
      {
        kind: BatchMutationKind.CreateEntity,
        executeWrite: async () => secondElement,
        sideEffects: () => [buildAutoEnrichmentSideEffect(testContext, testUser, secondElement, 'create', {
          loadById: vi.fn().mockResolvedValue('{"id":"indicator--2","type":"indicator"}'),
          bundleById: vi.fn().mockResolvedValue('{"type":"bundle","objects":[{"id":"indicator--2","type":"indicator"}]}'),
        })],
      },
    ]);

    await vi.waitFor(() => expect(createWork).toHaveBeenCalledTimes(2));
    resolveFirstWork({ id: 'work-1' });
    await execution;

    expect(pushToConnector).toHaveBeenCalledTimes(2);
    expect(vi.mocked(pushToConnector).mock.calls.map(([, message]) => message.event.entity_id).sort()).toEqual([
      'indicator--1',
      'indicator--2',
    ]);
  });

  it('keeps the latest same-entity side effect when batch dispatch removes duplicate work', async () => {
    vi.mocked(getEntitiesListFromCache).mockResolvedValue([{
      ...connectors[0],
      auto_update: true,
      enrichment_batch_capability: DEFAULT_ENRICHMENT_BATCH_CAPABILITY,
    }] as never);
    vi.mocked(createWork)
      .mockReset()
      .mockResolvedValueOnce({ id: 'work-1' } as never);
    const firstElement = { ...element, name: 'first-snapshot' };
    const secondElement = { ...element, name: 'second-snapshot' };

    await executeBatchMutations([
      {
        kind: BatchMutationKind.UpdateAttribute,
        executeWrite: async () => firstElement,
        sideEffects: () => [buildAutoEnrichmentSideEffect(testContext, testUser, firstElement, 'update', {
          loadById: vi.fn().mockResolvedValue('{"id":"indicator--test","snapshot":1}'),
          bundleById: vi.fn().mockResolvedValue('{"type":"bundle","objects":[{"id":"indicator--test","snapshot":1}]}'),
        })],
      },
      {
        kind: BatchMutationKind.UpdateAttribute,
        executeWrite: async () => secondElement,
        sideEffects: () => [buildAutoEnrichmentSideEffect(testContext, testUser, secondElement, 'update', {
          loadById: vi.fn().mockResolvedValue('{"id":"indicator--test","snapshot":2}'),
          bundleById: vi.fn().mockResolvedValue('{"type":"bundle","objects":[{"id":"indicator--test","snapshot":2}]}'),
        })],
      },
    ]);

    expect(createWork).toHaveBeenCalledTimes(1);
    expect(pushToConnector).toHaveBeenCalledTimes(1);
    expect(vi.mocked(pushToConnector).mock.calls[0][1].event).toMatchObject({
      stix_entity: '{"id":"indicator--test","snapshot":2}',
      stix_objects: '{"type":"bundle","objects":[{"id":"indicator--test","snapshot":2}]}',
    });
  });

  it('splits incompatible batch-capable enrichment contexts deterministically', async () => {
    vi.mocked(getEntitiesListFromCache).mockResolvedValue([{
      ...connectors[0],
      enrichment_batch_capability: DEFAULT_ENRICHMENT_BATCH_CAPABILITY,
    }] as never);
    vi.mocked(getDraftContext)
      .mockReset()
      .mockReturnValueOnce('draft--1')
      .mockReturnValueOnce('draft--1')
      .mockReturnValueOnce('draft--2')
      .mockReturnValueOnce('draft--2');
    vi.mocked(createWork)
      .mockReset()
      .mockResolvedValueOnce({ id: 'work-1' } as never)
      .mockResolvedValueOnce({ id: 'work-2' } as never)
      .mockResolvedValueOnce({ id: 'work-3' } as never)
      .mockResolvedValueOnce({ id: 'work-4' } as never);
    const elements = [1, 2, 3, 4].map((suffix) => ({
      ...element,
      standard_id: `indicator--${suffix}`,
      internal_id: `indicator-internal-${suffix}`,
    }));

    await executeBatchMutations(elements.map((currentElement) => ({
      kind: BatchMutationKind.CreateEntity,
      executeWrite: async () => currentElement,
      sideEffects: () => [buildAutoEnrichmentSideEffect(testContext, testUser, currentElement, 'create', {
        loadById: vi.fn().mockResolvedValue(`{"id":"${currentElement.standard_id}","type":"indicator"}`),
        bundleById: vi.fn().mockResolvedValue(`{"type":"bundle","objects":[{"id":"${currentElement.standard_id}","type":"indicator"}]}`),
      })],
    })));

    expect(pushToConnector).toHaveBeenCalledTimes(2);
    const envelopes = vi.mocked(pushToConnector).mock.calls.map(([, message]) => parseEnrichmentBatchEnvelope(
      message.event.enrichment_batch,
      DEFAULT_ENRICHMENT_BATCH_CAPABILITY,
    ));
    expect(envelopes.map((envelope) => envelope.group_context.draft_id)).toEqual(['draft--1', 'draft--2']);
    expect(envelopes.map((envelope) => envelope.items.map((item) => item.entity_id).sort())).toEqual([
      ['indicator--1', 'indicator--2'],
      ['indicator--3', 'indicator--4'],
    ]);
  });

  it('keeps disabled auto-enrichment items out of grouped envelopes', async () => {
    vi.mocked(getEntitiesListFromCache).mockResolvedValue([{
      ...connectors[0],
      enrichment_batch_capability: DEFAULT_ENRICHMENT_BATCH_CAPABILITY,
    }] as never);
    vi.mocked(createWork)
      .mockReset()
      .mockResolvedValueOnce({ id: 'work-1' } as never)
      .mockResolvedValueOnce({ id: 'work-2' } as never);
    const enabledElements = [1, 2].map((suffix) => ({
      ...element,
      standard_id: `indicator--${suffix}`,
      internal_id: `indicator-internal-${suffix}`,
    }));
    const disabledElement = {
      ...element,
      standard_id: 'indicator--disabled',
      internal_id: 'indicator-internal-disabled',
      auto_enrichment_disable: true,
    };

    await executeBatchMutations([...enabledElements, disabledElement].map((currentElement) => ({
      kind: BatchMutationKind.CreateEntity,
      executeWrite: async () => currentElement,
      sideEffects: () => [buildAutoEnrichmentSideEffect(testContext, testUser, currentElement, 'create', {
        loadById: vi.fn().mockResolvedValue(`{"id":"${currentElement.standard_id}","type":"indicator"}`),
        bundleById: vi.fn().mockResolvedValue(`{"type":"bundle","objects":[{"id":"${currentElement.standard_id}","type":"indicator"}]}`),
      })],
    })));

    expect(createWork).toHaveBeenCalledTimes(2);
    expect(pushToConnector).toHaveBeenCalledTimes(1);
    const envelope = parseEnrichmentBatchEnvelope(
      vi.mocked(pushToConnector).mock.calls[0][1].event.enrichment_batch,
      DEFAULT_ENRICHMENT_BATCH_CAPABILITY,
    );
    expect(envelope.items.map((item) => item.entity_id).sort()).toEqual(['indicator--1', 'indicator--2']);
  });
});
