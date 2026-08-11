import { beforeEach, describe, expect, it, vi } from 'vitest';
import { elIndex, elLoadById } from '../../../../src/database/engine';
import { INDEX_INTERNAL_OBJECTS, READ_INDEX_INTERNAL_OBJECTS } from '../../../../src/database/utils';
import { lockResources } from '../../../../src/lock/master-lock';
import {
  assertEnrichmentBatchResultReceiptReservation,
  buildEnrichmentBatchResultReceiptId,
  buildEnrichmentBatchResultReceiptLockId,
  loadEnrichmentBatchResultReceipt,
  readEnrichmentBatchResultReceiptPayload,
  reserveEnrichmentBatchResultReceipt,
} from '../../../../src/modules/enrichment/enrichment-batch-result-receipt-domain';
import { ENTITY_TYPE_ENRICHMENT_BATCH_RESULT_RECEIPT } from '../../../../src/modules/enrichment/enrichment-batch-types';
import type { AuthContext } from '../../../../src/types/user';
import { SYSTEM_USER } from '../../../../src/utils/access';

vi.mock('../../../../src/database/engine', () => ({
  elIndex: vi.fn(),
  elLoadById: vi.fn(),
}));

vi.mock('../../../../src/lock/master-lock', () => ({
  lockResources: vi.fn(),
}));

const context = {} as AuthContext;
const input = {
  connectorId: 'connector--hygiene',
  batchId: 'enrichment-batch--1',
  serializedEnvelope: '{"batch_id":"enrichment-batch--1"}',
  serializedResult: '{"batch_id":"enrichment-batch--1","results":[]}',
};

describe('enrichment batch result receipt', () => {
  beforeEach(() => {
    vi.clearAllMocks();
    vi.mocked(lockResources).mockResolvedValue({ unlock: vi.fn() } as never);
    vi.mocked(elLoadById).mockResolvedValue(null as never);
    vi.mocked(elIndex).mockResolvedValue(undefined as never);
  });

  it('reserves one immutable receipt under a per-batch lock', async () => {
    const receipt = await reserveEnrichmentBatchResultReceipt(context, input);

    expect(lockResources).toHaveBeenCalledWith([buildEnrichmentBatchResultReceiptLockId(input.connectorId, input.batchId)]);
    expect(elLoadById).toHaveBeenCalledWith(context, SYSTEM_USER, buildEnrichmentBatchResultReceiptId(input.connectorId, input.batchId), {
      type: ENTITY_TYPE_ENRICHMENT_BATCH_RESULT_RECEIPT,
      indices: READ_INDEX_INTERNAL_OBJECTS,
    });
    expect(elIndex).toHaveBeenCalledWith(INDEX_INTERNAL_OBJECTS, expect.objectContaining({
      internal_id: buildEnrichmentBatchResultReceiptId(input.connectorId, input.batchId),
      connector_id: input.connectorId,
      batch_id: input.batchId,
      result_payload: input.serializedResult,
      result_payload_version: 1,
    }), { context });
    expect(readEnrichmentBatchResultReceiptPayload(receipt)).toBe(input.serializedResult);
  });

  it('reuses the first stored result payload when a later candidate changes', async () => {
    const firstReceipt = await reserveEnrichmentBatchResultReceipt(context, input);
    vi.mocked(elLoadById).mockResolvedValue(firstReceipt as never);

    const replayedReceipt = await reserveEnrichmentBatchResultReceipt(context, {
      ...input,
      serializedResult: '{"batch_id":"enrichment-batch--1","results":[{"status":"FAILED"}]}',
    });

    expect(elIndex).toHaveBeenCalledTimes(1);
    expect(replayedReceipt).toBe(firstReceipt);
    expect(readEnrichmentBatchResultReceiptPayload(replayedReceipt)).toBe(input.serializedResult);
  });

  it('rejects a receipt replay with a different immutable envelope identity', async () => {
    const receipt = await reserveEnrichmentBatchResultReceipt(context, input);

    expect(() => assertEnrichmentBatchResultReceiptReservation(receipt, {
      ...input,
      serializedEnvelope: '{"batch_id":"enrichment-batch--different"}',
    })).toThrow('Enrichment batch result receipt is already associated with different immutable envelope data');
  });

  it('rejects malformed stored result payload fingerprints', async () => {
    vi.mocked(elLoadById).mockResolvedValue({
      id: buildEnrichmentBatchResultReceiptId(input.connectorId, input.batchId),
      internal_id: buildEnrichmentBatchResultReceiptId(input.connectorId, input.batchId),
      standard_id: buildEnrichmentBatchResultReceiptId(input.connectorId, input.batchId),
      entity_type: ENTITY_TYPE_ENRICHMENT_BATCH_RESULT_RECEIPT,
      base_type: 'ENTITY',
      parent_types: ['Internal-Object'],
      connector_id: input.connectorId,
      batch_id: input.batchId,
      envelope_fingerprint: 'envelope-fingerprint',
      result_payload_version: 1,
      result_fingerprint: 'wrong-fingerprint',
      result_payload: input.serializedResult,
      created_at: '2026-08-11T00:00:00.000Z',
      updated_at: '2026-08-11T00:00:00.000Z',
    } as never);

    await expect(loadEnrichmentBatchResultReceipt(context, input.connectorId, input.batchId))
      .rejects.toThrow('Enrichment batch result receipt payload fingerprint does not match its result payload');
  });
});
