import { beforeEach, describe, expect, it, vi } from 'vitest';
import { elFindByIds } from '../../../../src/database/engine';
import { storeLoadById } from '../../../../src/database/middleware-loader';
import { READ_INDEX_HISTORY } from '../../../../src/database/utils';
import { submitStixBundle } from '../../../../src/domain/stix';
import { updateProcessedTimes, updateReceivedTimes } from '../../../../src/domain/work';
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
import {
  submitEnrichmentBatchFailure,
  submitEnrichmentBatchReceived,
  submitEnrichmentBatchResult,
} from '../../../../src/modules/enrichment/enrichment-batch-domain';
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

vi.mock('../../../../src/domain/work', () => ({
  updateProcessedTimes: vi.fn(),
  updateReceivedTimes: vi.fn(),
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
    vi.mocked(updateProcessedTimes).mockResolvedValue([] as never);
    vi.mocked(updateReceivedTimes).mockResolvedValue([] as never);
  });

  it('marks one physical envelope received with one batch work transition', async () => {
    const envelope = buildEnrichmentBatchEnvelope([
      candidate(),
      candidate({ workId: 'work--2', entityId: 'indicator--2' }),
    ], DEFAULT_ENRICHMENT_BATCH_CAPABILITY);

    await submitEnrichmentBatchReceived(
      testContext,
      testUser,
      connector.internal_id,
      serializeEnrichmentBatchEnvelope(envelope),
    );

    expect(elFindByIds).toHaveBeenCalledTimes(1);
    expect(updateReceivedTimes).toHaveBeenCalledTimes(1);
    expect(updateReceivedTimes).toHaveBeenCalledWith(testContext, testUser, [
      {
        work: expect.objectContaining({ internal_id: 'work--1' }),
        message: 'Connector ready to process the operation',
      },
      {
        work: expect.objectContaining({ internal_id: 'work--2' }),
        message: 'Connector ready to process the operation',
      },
    ]);
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
    expect(updateProcessedTimes).toHaveBeenCalledTimes(1);
    expect(updateProcessedTimes).toHaveBeenCalledWith(testContext, testUser, [
      {
        work: expect.objectContaining({ internal_id: 'work--1' }),
        message: 'Connector successfully processed the operation',
        inError: false,
      },
      {
        work: expect.objectContaining({ internal_id: 'work--2' }),
        message: 'Connector successfully processed the operation',
        inError: false,
      },
    ]);
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
    expect(updateProcessedTimes).toHaveBeenCalledWith(testContext, testUser, [
      {
        work: expect.objectContaining({ internal_id: 'work--1' }),
        message: 'No changes produced by connector',
        inError: false,
      },
      {
        work: expect.objectContaining({ internal_id: 'work--2' }),
        message: 'deterministic failure',
        inError: true,
      },
    ]);
  });

  it('marks callback failures with one batch work transition', async () => {
    const envelope = buildEnrichmentBatchEnvelope([
      candidate(),
      candidate({ workId: 'work--2', entityId: 'indicator--2' }),
    ], DEFAULT_ENRICHMENT_BATCH_CAPABILITY);

    await submitEnrichmentBatchFailure(
      testContext,
      testUser,
      connector.internal_id,
      serializeEnrichmentBatchEnvelope(envelope),
      'callback failed',
    );

    expect(updateProcessedTimes).toHaveBeenCalledTimes(1);
    expect(updateProcessedTimes).toHaveBeenCalledWith(testContext, testUser, [
      {
        work: expect.objectContaining({ internal_id: 'work--1' }),
        message: 'callback failed',
        inError: true,
      },
      {
        work: expect.objectContaining({ internal_id: 'work--2' }),
        message: 'callback failed',
        inError: true,
      },
    ]);
  });

  it('rejects retryable result submission before terminal settlement', async () => {
    const envelope = buildEnrichmentBatchEnvelope([candidate()], DEFAULT_ENRICHMENT_BATCH_CAPABILITY);
    const resultEnvelope = buildEnrichmentBatchResultEnvelope(
      envelope,
      [{
        itemId: envelope.items[0].item_id,
        workId: envelope.items[0].work_id,
        status: EnrichmentBatchResultStatus.Retryable,
        message: 'retry later',
      }],
      null,
      DEFAULT_ENRICHMENT_BATCH_CAPABILITY,
    );

    await expect(submitEnrichmentBatchResult(
      testContext,
      testUser,
      connector.internal_id,
      serializeEnrichmentBatchEnvelope(envelope),
      serializeEnrichmentBatchResultEnvelope(resultEnvelope),
    )).rejects.toThrow('Retryable enrichment batch results cannot be submitted for terminal settlement');

    expect(submitStixBundle).not.toHaveBeenCalled();
    expect(updateProcessedTimes).not.toHaveBeenCalled();
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
    )).rejects.toThrow('Enrichment batch lifecycle references a Work outside the connector partition');
  });

  it('rejects received settlement for a Work owned by another connector', async () => {
    const envelope = buildEnrichmentBatchEnvelope([candidate()], DEFAULT_ENRICHMENT_BATCH_CAPABILITY);
    vi.mocked(elFindByIds).mockResolvedValue({
      'work--1': {
        id: 'work--1',
        internal_id: 'work--1',
        connector_id: 'connector--other',
      },
    } as never);

    await expect(submitEnrichmentBatchReceived(
      testContext,
      testUser,
      connector.internal_id,
      serializeEnrichmentBatchEnvelope(envelope),
    )).rejects.toThrow('Enrichment batch lifecycle references a Work outside the connector partition');

    expect(updateReceivedTimes).not.toHaveBeenCalled();
  });
});
