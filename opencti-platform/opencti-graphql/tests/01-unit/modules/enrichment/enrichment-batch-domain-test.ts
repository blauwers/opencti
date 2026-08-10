import { beforeEach, describe, expect, it, vi } from 'vitest';
import { elFindByIds } from '../../../../src/database/engine';
import { storeLoadById } from '../../../../src/database/middleware-loader';
import { READ_INDEX_HISTORY } from '../../../../src/database/utils';
import { submitStixBundle } from '../../../../src/domain/stix';
import {
  buildEnrichmentBatchEnvelope,
  buildEnrichmentBatchResultEnvelope,
  DEFAULT_ENRICHMENT_BATCH_CAPABILITY,
  EnrichmentBatchMode,
  EnrichmentBatchResultStatus,
  EnrichmentBatchTrigger,
  serializeEnrichmentBatchEnvelope,
  serializeEnrichmentBatchResultEnvelope,
  type EnrichmentBatchCandidate,
} from '../../../../src/modules/enrichment/enrichment-batch-contract';
import { submitEnrichmentBatchResult } from '../../../../src/modules/enrichment/enrichment-batch-domain';
import { ENTITY_TYPE_WORK } from '../../../../src/schema/internalObject';
import type { AuthContext, AuthUser } from '../../../../src/types/user';

vi.mock('../../../../src/database/engine', () => ({
  elFindByIds: vi.fn(),
}));

vi.mock('../../../../src/database/middleware-loader', () => ({
  storeLoadById: vi.fn(),
}));

vi.mock('../../../../src/domain/stix', () => ({
  submitStixBundle: vi.fn(),
}));

const testContext = {} as AuthContext;
const testUser = { id: 'user--test' } as AuthUser;
const connector = {
  id: 'connector--hygiene',
  internal_id: 'connector--hygiene',
  enrichment_batch_capability: DEFAULT_ENRICHMENT_BATCH_CAPABILITY,
};

const candidate = (overrides: Partial<EnrichmentBatchCandidate> = {}): EnrichmentBatchCandidate => ({
  connectorId: connector.internal_id,
  workId: 'work--1',
  entityId: 'indicator--1',
  entityType: 'Indicator',
  applicantId: null,
  draftId: null,
  mode: EnrichmentBatchMode.Auto,
  trigger: EnrichmentBatchTrigger.Create,
  resolution: 'deferred',
  playbookContext: null,
  configuration: null,
  sharedOrganizationIds: [],
  stixEntity: null,
  stixObjects: null,
  ...overrides,
});

describe('submitEnrichmentBatchResult', () => {
  beforeEach(() => {
    vi.clearAllMocks();
    vi.mocked(storeLoadById).mockResolvedValue(connector as never);
    vi.mocked(elFindByIds).mockImplementation(async (_context, _user, workIds) => Object.fromEntries(
      (Array.isArray(workIds) ? workIds : [workIds]).map((workId) => [workId, {
        id: workId,
        internal_id: workId,
        connector_id: connector.internal_id,
      }]),
    ) as never);
    vi.mocked(submitStixBundle).mockResolvedValue({ submissionId: 'submission--1' } as never);
  });

  it('submits one physical output bundle with idempotent multi-work attribution metadata', async () => {
    const envelope = buildEnrichmentBatchEnvelope([
      candidate(),
      candidate({ workId: 'work--2', entityId: 'indicator--2' }),
    ], DEFAULT_ENRICHMENT_BATCH_CAPABILITY);
    const outputBundle = JSON.stringify({
      type: 'bundle',
      objects: [
        { id: 'indicator--1', type: 'indicator' },
        { id: 'indicator--2', type: 'indicator' },
      ],
    });
    const resultEnvelope = buildEnrichmentBatchResultEnvelope(
      envelope,
      envelope.items.map((item) => ({
        itemId: item.item_id,
        workId: item.work_id,
        status: EnrichmentBatchResultStatus.Processed,
        outputObjectIds: [item.entity_id],
      })),
      outputBundle,
      DEFAULT_ENRICHMENT_BATCH_CAPABILITY,
    );

    await submitEnrichmentBatchResult(
      testContext,
      testUser,
      connector.internal_id,
      serializeEnrichmentBatchEnvelope(envelope),
      serializeEnrichmentBatchResultEnvelope(resultEnvelope),
    );

    expect(elFindByIds).toHaveBeenCalledTimes(1);
    expect(elFindByIds).toHaveBeenCalledWith(testContext, testUser, envelope.items.map((item) => item.work_id), {
      type: ENTITY_TYPE_WORK,
      indices: READ_INDEX_HISTORY,
      toMap: true,
    });
    expect(submitStixBundle).toHaveBeenCalledTimes(1);
    const submitOptions = vi.mocked(submitStixBundle).mock.calls[0][5] as { fingerprintContext?: unknown };
    expect(vi.mocked(submitStixBundle).mock.calls[0][3]).toBe(outputBundle);
    expect(vi.mocked(submitStixBundle).mock.calls[0][4]).toBe(envelope.items[0].work_id);
    expect(submitOptions).toMatchObject({
      idempotencyKey: `enrichment-batch-result:${envelope.batch_id}`,
      additionalWorkIds: [envelope.items[1].work_id],
      enrichmentBatchResult: serializeEnrichmentBatchResultEnvelope(resultEnvelope),
      fingerprintContext: {
        protocol_version: resultEnvelope.protocol_version,
        batch_id: resultEnvelope.batch_id,
        result_count: resultEnvelope.result_count,
        output_object_count: resultEnvelope.output_object_count,
        results: resultEnvelope.results,
      },
    });
    expect(submitOptions.fingerprintContext).not.toHaveProperty('output_bundle');
  });

  it('does not enqueue an import when every item is unchanged or failed', async () => {
    const envelope = buildEnrichmentBatchEnvelope([
      candidate(),
      candidate({ workId: 'work--2', entityId: 'indicator--2' }),
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
          status: EnrichmentBatchResultStatus.Failed,
          message: 'deterministic failure',
        },
      ],
      null,
      DEFAULT_ENRICHMENT_BATCH_CAPABILITY,
    );

    await submitEnrichmentBatchResult(
      testContext,
      testUser,
      connector.internal_id,
      serializeEnrichmentBatchEnvelope(envelope),
      serializeEnrichmentBatchResultEnvelope(resultEnvelope),
    );

    expect(submitStixBundle).not.toHaveBeenCalled();
  });

  it('rejects result attribution to a Work owned by another connector', async () => {
    const envelope = buildEnrichmentBatchEnvelope([candidate()], DEFAULT_ENRICHMENT_BATCH_CAPABILITY);
    const outputBundle = JSON.stringify({
      type: 'bundle',
      objects: [{ id: 'indicator--1', type: 'indicator' }],
    });
    const resultEnvelope = buildEnrichmentBatchResultEnvelope(
      envelope,
      [{
        itemId: envelope.items[0].item_id,
        workId: envelope.items[0].work_id,
        status: EnrichmentBatchResultStatus.Processed,
        outputObjectIds: ['indicator--1'],
      }],
      outputBundle,
      DEFAULT_ENRICHMENT_BATCH_CAPABILITY,
    );
    vi.mocked(elFindByIds).mockResolvedValue({
      'work--1': {
        id: 'work--1',
        internal_id: 'work--1',
        connector_id: 'connector--other',
      },
    } as never);

    await expect(submitEnrichmentBatchResult(
      testContext,
      testUser,
      connector.internal_id,
      serializeEnrichmentBatchEnvelope(envelope),
      serializeEnrichmentBatchResultEnvelope(resultEnvelope),
    )).rejects.toThrow('Enrichment batch result references a Work outside the connector partition');
  });
});
