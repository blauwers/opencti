import { describe, expect, it } from 'vitest';
import {
  buildEnrichmentBatchEnvelope,
  buildEnrichmentBatchResultEnvelope,
  DEFAULT_ENRICHMENT_BATCH_CAPABILITY,
  EnrichmentBatchContractErrorCode,
  EnrichmentBatchMode,
  EnrichmentBatchProtocol,
  EnrichmentBatchResultStatus,
  EnrichmentBatchTrigger,
  normalizeEnrichmentBatchCapability,
  parseEnrichmentBatchEnvelope,
  parseEnrichmentBatchResultEnvelope,
  serializeEnrichmentBatchEnvelope,
  serializeEnrichmentBatchResultEnvelope,
  type EnrichmentBatchCandidate,
} from '../../../../src/modules/enrichment/enrichment-batch-contract';

const deferredCandidate = (overrides: Partial<EnrichmentBatchCandidate> = {}): EnrichmentBatchCandidate => ({
  connectorId: 'connector--hygiene',
  workId: 'work--1',
  entityId: 'indicator--1',
  entityType: 'Indicator',
  applicantId: null,
  draftId: null,
  mode: EnrichmentBatchMode.Auto,
  trigger: EnrichmentBatchTrigger.Create,
  resolution: 'deferred',
  playbookContext: null,
  configuration: { source: 'auto' },
  sharedOrganizationIds: ['organization--2', 'organization--1'],
  stixEntity: null,
  stixObjects: null,
  ...overrides,
});

const bundleCandidate = (overrides: Partial<EnrichmentBatchCandidate> = {}): EnrichmentBatchCandidate => ({
  ...deferredCandidate({
    resolution: 'stix_bundle',
    stixEntity: JSON.stringify({ id: 'indicator--1', type: 'indicator' }),
    stixObjects: JSON.stringify({
      type: 'bundle',
      objects: [
        { id: 'indicator--1', type: 'indicator' },
        { id: 'label--1', type: 'label' },
      ],
    }),
  }),
  ...overrides,
});

const expectContractError = (error: unknown, code: EnrichmentBatchContractErrorCode, field?: string) => {
  expect(error).toMatchObject({
    extensions: {
      data: {
        enrichment_batch_error_code: code,
        ...(field ? { field } : {}),
      },
    },
  });
};

describe('enrichment batch contract', () => {
  it('keeps capability negotiation additive and fails closed for malformed capability claims', () => {
    expect(normalizeEnrichmentBatchCapability(null)).toBeNull();
    expect(normalizeEnrichmentBatchCapability(DEFAULT_ENRICHMENT_BATCH_CAPABILITY)).toEqual({
      protocol_version: EnrichmentBatchProtocol.V1,
      max_items: 100,
      max_stix_objects: 1000,
      max_serialized_bytes: 1024 * 1024,
      max_wait_ms: 1000,
    });

    expect(() => normalizeEnrichmentBatchCapability({
      ...DEFAULT_ENRICHMENT_BATCH_CAPABILITY,
      protocol_version: 2,
    })).toThrowError(expect.objectContaining({
      extensions: expect.objectContaining({
        data: expect.objectContaining({
          enrichment_batch_error_code: EnrichmentBatchContractErrorCode.InvalidCapability,
        }),
      }),
    }));
    expect(() => normalizeEnrichmentBatchCapability({
      ...DEFAULT_ENRICHMENT_BATCH_CAPABILITY,
      max_items: 101,
    })).toThrowError(expect.objectContaining({
      extensions: expect.objectContaining({
        data: expect.objectContaining({
          enrichment_batch_error_code: EnrichmentBatchContractErrorCode.InvalidCapability,
          field: 'max_items',
        }),
      }),
    }));
  });

  it('builds a deterministic bounded envelope and round-trips the wire shape', () => {
    const first = deferredCandidate();
    const second = deferredCandidate({
      workId: 'work--2',
      entityId: 'indicator--2',
      configuration: { source: 'auto' },
      sharedOrganizationIds: ['organization--1', 'organization--2'],
    });

    const envelope = buildEnrichmentBatchEnvelope([second, first], DEFAULT_ENRICHMENT_BATCH_CAPABILITY);
    const replay = buildEnrichmentBatchEnvelope([first, second], DEFAULT_ENRICHMENT_BATCH_CAPABILITY);
    const serializedEnvelope = serializeEnrichmentBatchEnvelope(envelope);

    expect(envelope).toEqual(replay);
    expect(envelope).toMatchObject({
      protocol_version: EnrichmentBatchProtocol.V1,
      item_count: 2,
      object_count: 0,
      group_context: {
        connector_id: 'connector--hygiene',
        shared_organization_ids: ['organization--1', 'organization--2'],
      },
    });
    const bundleEnvelope = buildEnrichmentBatchEnvelope([
      bundleCandidate(),
      bundleCandidate({ workId: 'work--2', entityId: 'indicator--2' }),
    ], DEFAULT_ENRICHMENT_BATCH_CAPABILITY);
    const changedBundleEnvelope = buildEnrichmentBatchEnvelope([
      bundleCandidate(),
      bundleCandidate({
        workId: 'work--2',
        entityId: 'indicator--2',
        stixObjects: JSON.stringify({
          type: 'bundle',
          objects: [
            { id: 'indicator--2', type: 'indicator' },
            { id: 'label--1', type: 'label' },
            { id: 'label--2', type: 'label' },
          ],
        }),
      }),
    ], DEFAULT_ENRICHMENT_BATCH_CAPABILITY);
    expect(changedBundleEnvelope.batch_id).not.toBe(bundleEnvelope.batch_id);
    expect(envelope.items.map((item) => item.item_id)).toEqual([...envelope.items.map((item) => item.item_id)].sort());
    expect(parseEnrichmentBatchEnvelope(serializedEnvelope, DEFAULT_ENRICHMENT_BATCH_CAPABILITY)).toEqual(envelope);
  });

  it.each([
    ['connector_id', { connectorId: 'connector--other' }],
    ['applicant_id', { applicantId: 'user--1' }],
    ['draft_id', { draftId: 'draft--1' }],
    ['mode', { mode: EnrichmentBatchMode.Manual }],
    ['trigger', { trigger: EnrichmentBatchTrigger.Update }],
    ['resolution', bundleCandidate({ workId: 'work--2', entityId: 'indicator--2' })],
    ['playbook_context', { playbookContext: { execution_id: 'execution--2' } }],
    ['configuration', { configuration: { source: 'manual' } }],
    ['shared_organization_ids', { sharedOrganizationIds: ['organization--3'] }],
  ])('rejects mixed %s grouping context', (field, override) => {
    const first = deferredCandidate();
    const second = 'resolution' === field
      ? override as EnrichmentBatchCandidate
      : deferredCandidate({ workId: 'work--2', entityId: 'indicator--2', ...(override as Partial<EnrichmentBatchCandidate>) });

    try {
      buildEnrichmentBatchEnvelope([first, second], DEFAULT_ENRICHMENT_BATCH_CAPABILITY);
      throw new Error('Expected grouping rejection');
    } catch (error) {
      expectContractError(error, EnrichmentBatchContractErrorCode.IncompatibleGrouping, field);
    }
  });

  it('rejects invalid payload shape and connector capability overflow before live dispatch', () => {
    expect(() => buildEnrichmentBatchEnvelope([
      deferredCandidate({ stixEntity: JSON.stringify({ id: 'indicator--1' }) }),
    ], DEFAULT_ENRICHMENT_BATCH_CAPABILITY)).toThrowError(expect.objectContaining({
      extensions: expect.objectContaining({
        data: expect.objectContaining({
          enrichment_batch_error_code: EnrichmentBatchContractErrorCode.InvalidEnvelope,
        }),
      }),
    }));

    expect(() => buildEnrichmentBatchEnvelope([
      bundleCandidate(),
    ], {
      ...DEFAULT_ENRICHMENT_BATCH_CAPABILITY,
      max_stix_objects: 1,
    })).toThrowError(expect.objectContaining({
      extensions: expect.objectContaining({
        data: expect.objectContaining({
          enrichment_batch_error_code: EnrichmentBatchContractErrorCode.LimitExceeded,
          object_count: 2,
          max_stix_objects: 1,
        }),
      }),
    }));
  });

  it('maps one physical output bundle back to each logical item result', () => {
    const envelope = buildEnrichmentBatchEnvelope([
      deferredCandidate(),
      deferredCandidate({ workId: 'work--2', entityId: 'indicator--2' }),
    ], DEFAULT_ENRICHMENT_BATCH_CAPABILITY);
    const outputBundle = JSON.stringify({
      type: 'bundle',
      objects: [
        { id: 'indicator--1', type: 'indicator' },
        { id: 'indicator--2', type: 'indicator' },
        { id: 'label--shared', type: 'label' },
      ],
    });
    const resultEnvelope = buildEnrichmentBatchResultEnvelope(
      envelope,
      [
        {
          itemId: envelope.items[0].item_id,
          workId: envelope.items[0].work_id,
          status: EnrichmentBatchResultStatus.Processed,
          outputObjectIds: ['indicator--1', 'label--shared'],
        },
        {
          itemId: envelope.items[1].item_id,
          workId: envelope.items[1].work_id,
          status: EnrichmentBatchResultStatus.Processed,
          outputObjectIds: ['indicator--2', 'label--shared'],
        },
      ],
      outputBundle,
      DEFAULT_ENRICHMENT_BATCH_CAPABILITY,
    );

    expect(resultEnvelope).toMatchObject({
      batch_id: envelope.batch_id,
      result_count: 2,
      output_object_count: 3,
    });
    expect(parseEnrichmentBatchResultEnvelope(
      serializeEnrichmentBatchResultEnvelope(resultEnvelope),
      envelope,
      DEFAULT_ENRICHMENT_BATCH_CAPABILITY,
    )).toEqual(resultEnvelope);
  });

  it('supports explicit unchanged and retryable item outcomes without hiding failures', () => {
    const envelope = buildEnrichmentBatchEnvelope([
      deferredCandidate(),
      deferredCandidate({ workId: 'work--2', entityId: 'indicator--2' }),
    ], DEFAULT_ENRICHMENT_BATCH_CAPABILITY);
    const resultEnvelope = buildEnrichmentBatchResultEnvelope(
      envelope,
      [
        {
          itemId: envelope.items[0].item_id,
          workId: envelope.items[0].work_id,
          status: EnrichmentBatchResultStatus.Unchanged,
        },
        {
          itemId: envelope.items[1].item_id,
          workId: envelope.items[1].work_id,
          status: EnrichmentBatchResultStatus.Retryable,
          message: 'temporary remote dependency failure',
        },
      ],
      null,
      DEFAULT_ENRICHMENT_BATCH_CAPABILITY,
    );

    expect(resultEnvelope.results.map((result) => result.status)).toEqual([
      EnrichmentBatchResultStatus.Unchanged,
      EnrichmentBatchResultStatus.Retryable,
    ]);
  });

  it('rejects missing item results and unowned output objects', () => {
    const envelope = buildEnrichmentBatchEnvelope([
      deferredCandidate(),
      deferredCandidate({ workId: 'work--2', entityId: 'indicator--2' }),
    ], DEFAULT_ENRICHMENT_BATCH_CAPABILITY);

    expect(() => buildEnrichmentBatchResultEnvelope(
      envelope,
      [{
        itemId: envelope.items[0].item_id,
        workId: envelope.items[0].work_id,
        status: EnrichmentBatchResultStatus.Unchanged,
      }],
      null,
      DEFAULT_ENRICHMENT_BATCH_CAPABILITY,
    )).toThrowError(expect.objectContaining({
      extensions: expect.objectContaining({
        data: expect.objectContaining({
          enrichment_batch_error_code: EnrichmentBatchContractErrorCode.InvalidResult,
        }),
      }),
    }));

    expect(() => buildEnrichmentBatchResultEnvelope(
      envelope,
      [
        {
          itemId: envelope.items[0].item_id,
          workId: envelope.items[0].work_id,
          status: EnrichmentBatchResultStatus.Processed,
          outputObjectIds: ['indicator--1'],
        },
        {
          itemId: envelope.items[1].item_id,
          workId: envelope.items[1].work_id,
          status: EnrichmentBatchResultStatus.Unchanged,
        },
      ],
      JSON.stringify({
        type: 'bundle',
        objects: [
          { id: 'indicator--1', type: 'indicator' },
          { id: 'label--unowned', type: 'label' },
        ],
      }),
      DEFAULT_ENRICHMENT_BATCH_CAPABILITY,
    )).toThrowError(expect.objectContaining({
      extensions: expect.objectContaining({
        data: expect.objectContaining({
          enrichment_batch_error_code: EnrichmentBatchContractErrorCode.InvalidResult,
        }),
      }),
    }));
  });
});
