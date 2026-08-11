import { FunctionalError } from '../../config/errors';
import { elIndex, elLoadById } from '../../database/engine';
import { INDEX_INTERNAL_OBJECTS, READ_INDEX_INTERNAL_OBJECTS } from '../../database/utils';
import { lockResources } from '../../lock/master-lock';
import { BASE_TYPE_ENTITY } from '../../schema/general';
import { getParentTypes } from '../../schema/schemaUtils';
import type { AuthContext } from '../../types/user';
import { SYSTEM_USER } from '../../utils/access';
import { now } from '../../utils/format';
import { hashSHA256 } from '../../utils/hash';
import { ENTITY_TYPE_ENRICHMENT_BATCH_RESULT_RECEIPT, type EnrichmentBatchResultReceipt } from './enrichment-batch-types';

const ENRICHMENT_BATCH_RESULT_RECEIPT_PREFIX = 'enrichment-batch-result-receipt--';
const ENRICHMENT_BATCH_RESULT_RECEIPT_LOCK_PREFIX = 'enrichment-batch-result-receipt:';
const ENRICHMENT_BATCH_RESULT_RECEIPT_PAYLOAD_VERSION = 1;

export interface ReserveEnrichmentBatchResultReceiptInput {
  connectorId: string;
  batchId: string;
  serializedEnvelope: string;
  serializedResult: string;
}

const enrichmentBatchResultReceiptConflict = (message: string, data: Record<string, unknown> = {}) => {
  return FunctionalError(message, data);
};

const isNonEmptyString = (value: unknown): value is string => {
  return typeof value === 'string' && value.length > 0;
};

const buildEnvelopeFingerprint = (serializedEnvelope: string): string => hashSHA256(serializedEnvelope);
const buildResultFingerprint = (serializedResult: string): string => hashSHA256(serializedResult);

export const buildEnrichmentBatchResultReceiptId = (connectorId: string, batchId: string): string => {
  return `${ENRICHMENT_BATCH_RESULT_RECEIPT_PREFIX}${hashSHA256(JSON.stringify([connectorId, batchId]))}`;
};

export const buildEnrichmentBatchResultReceiptLockId = (connectorId: string, batchId: string): string => {
  return `${ENRICHMENT_BATCH_RESULT_RECEIPT_LOCK_PREFIX}${connectorId}:${batchId}`;
};

const assertEnrichmentBatchResultReceiptRecord = (receipt: unknown): EnrichmentBatchResultReceipt => {
  if (
    !receipt
    || typeof receipt !== 'object'
    || (receipt as EnrichmentBatchResultReceipt).entity_type !== ENTITY_TYPE_ENRICHMENT_BATCH_RESULT_RECEIPT
    || !isNonEmptyString((receipt as EnrichmentBatchResultReceipt).connector_id)
    || !isNonEmptyString((receipt as EnrichmentBatchResultReceipt).batch_id)
    || !isNonEmptyString((receipt as EnrichmentBatchResultReceipt).envelope_fingerprint)
    || (receipt as EnrichmentBatchResultReceipt).result_payload_version !== ENRICHMENT_BATCH_RESULT_RECEIPT_PAYLOAD_VERSION
    || !isNonEmptyString((receipt as EnrichmentBatchResultReceipt).result_fingerprint)
    || !isNonEmptyString((receipt as EnrichmentBatchResultReceipt).result_payload)
  ) {
    throw enrichmentBatchResultReceiptConflict('Enrichment batch result receipt payload is malformed');
  }
  const normalizedReceipt = receipt as EnrichmentBatchResultReceipt;
  if (normalizedReceipt.internal_id !== buildEnrichmentBatchResultReceiptId(normalizedReceipt.connector_id, normalizedReceipt.batch_id)) {
    throw enrichmentBatchResultReceiptConflict('Enrichment batch result receipt id does not match its immutable identity', {
      receipt_id: normalizedReceipt.internal_id,
      connector_id: normalizedReceipt.connector_id,
      batch_id: normalizedReceipt.batch_id,
    });
  }
  if (normalizedReceipt.result_fingerprint !== buildResultFingerprint(normalizedReceipt.result_payload)) {
    throw enrichmentBatchResultReceiptConflict('Enrichment batch result receipt payload fingerprint does not match its result payload', {
      receipt_id: normalizedReceipt.internal_id,
    });
  }
  return normalizedReceipt;
};

export const loadEnrichmentBatchResultReceipt = async (
  context: AuthContext,
  connectorId: string,
  batchId: string,
): Promise<EnrichmentBatchResultReceipt | null> => {
  const receiptId = buildEnrichmentBatchResultReceiptId(connectorId, batchId);
  const receipt = await elLoadById(context, SYSTEM_USER, receiptId, {
    type: ENTITY_TYPE_ENRICHMENT_BATCH_RESULT_RECEIPT,
    indices: READ_INDEX_INTERNAL_OBJECTS,
  });
  return receipt ? assertEnrichmentBatchResultReceiptRecord(receipt) : null;
};

export const assertEnrichmentBatchResultReceiptReservation = (
  receipt: EnrichmentBatchResultReceipt,
  input: ReserveEnrichmentBatchResultReceiptInput,
) => {
  const conflictingFields = Object.entries({
    connector_id: receipt.connector_id !== input.connectorId,
    batch_id: receipt.batch_id !== input.batchId,
    envelope_fingerprint: receipt.envelope_fingerprint !== buildEnvelopeFingerprint(input.serializedEnvelope),
  }).filter(([, conflict]) => conflict).map(([field]) => field);
  if (conflictingFields.length > 0) {
    throw enrichmentBatchResultReceiptConflict('Enrichment batch result receipt is already associated with different immutable envelope data', {
      receipt_id: receipt.internal_id,
      connector_id: receipt.connector_id,
      batch_id: receipt.batch_id,
      conflicting_fields: conflictingFields,
    });
  }
};

export const reserveEnrichmentBatchResultReceipt = async (
  context: AuthContext,
  input: ReserveEnrichmentBatchResultReceiptInput,
): Promise<EnrichmentBatchResultReceipt> => {
  const lock = await lockResources([buildEnrichmentBatchResultReceiptLockId(input.connectorId, input.batchId)]);
  try {
    const existingReceipt = await loadEnrichmentBatchResultReceipt(context, input.connectorId, input.batchId);
    if (existingReceipt) {
      assertEnrichmentBatchResultReceiptReservation(existingReceipt, input);
      return existingReceipt;
    }
    const createdAt = now();
    const receiptId = buildEnrichmentBatchResultReceiptId(input.connectorId, input.batchId);
    const receipt: EnrichmentBatchResultReceipt = {
      id: receiptId,
      internal_id: receiptId,
      standard_id: receiptId,
      entity_type: ENTITY_TYPE_ENRICHMENT_BATCH_RESULT_RECEIPT,
      base_type: BASE_TYPE_ENTITY,
      parent_types: getParentTypes(ENTITY_TYPE_ENRICHMENT_BATCH_RESULT_RECEIPT),
      connector_id: input.connectorId,
      batch_id: input.batchId,
      envelope_fingerprint: buildEnvelopeFingerprint(input.serializedEnvelope),
      result_payload_version: ENRICHMENT_BATCH_RESULT_RECEIPT_PAYLOAD_VERSION,
      result_fingerprint: buildResultFingerprint(input.serializedResult),
      result_payload: input.serializedResult,
      created_at: createdAt,
      updated_at: createdAt,
    };
    await elIndex(INDEX_INTERNAL_OBJECTS, receipt, { context });
    return receipt;
  } finally {
    await lock.unlock();
  }
};

export const readEnrichmentBatchResultReceiptPayload = (receipt: EnrichmentBatchResultReceipt): string => {
  return assertEnrichmentBatchResultReceiptRecord(receipt).result_payload;
};
