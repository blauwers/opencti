import { FunctionalError, UnsupportedError } from '../../config/errors';
import { elFindByIds } from '../../database/engine';
import { storeLoadById } from '../../database/middleware-loader';
import { READ_INDEX_HISTORY } from '../../database/utils';
import { submitStixBundle } from '../../domain/stix';
import { updateProcessedTimes, updateReceivedTimes } from '../../domain/work';
import { ENTITY_TYPE_CONNECTOR, ENTITY_TYPE_WORK } from '../../schema/internalObject';
import type { AuthContext, AuthUser } from '../../types/user';
import type { BasicStoreEntityConnector } from '../../types/connector';
import type { Work } from '../../types/work';
import {
  buildEnrichmentBatchResultEnvelope,
  normalizeEnrichmentBatchCapability,
  parseEnrichmentBatchEnvelope,
  parseEnrichmentBatchResultEnvelope,
  serializeEnrichmentBatchEnvelope,
  serializeEnrichmentBatchResultEnvelope,
  type EnrichmentBatchCapability,
  type EnrichmentBatchEnvelope,
  type EnrichmentBatchResultEnvelope,
  EnrichmentBatchResultStatus,
} from './enrichment-batch-contract';
import { readEnrichmentBatchResultReceiptPayload, reserveEnrichmentBatchResultReceipt } from './enrichment-batch-result-receipt-domain';

const ENRICHMENT_BATCH_RESULT_IDEMPOTENCY_PREFIX = 'enrichment-batch-result:';
const ENRICHMENT_BATCH_RECEIVED_MESSAGE = 'Connector ready to process the operation';

const buildEnrichmentBatchResultFingerprintContext = (resultEnvelope: EnrichmentBatchResultEnvelope) => ({
  protocol_version: resultEnvelope.protocol_version,
  batch_id: resultEnvelope.batch_id,
  result_count: resultEnvelope.result_count,
  output_object_count: resultEnvelope.output_object_count,
  results: resultEnvelope.results,
});

const loadValidatedEnrichmentBatchContext = async (
  context: AuthContext,
  user: AuthUser,
  connectorId: string,
  envelopeInput: string,
) => {
  const connector = await storeLoadById<BasicStoreEntityConnector>(context, user, connectorId, ENTITY_TYPE_CONNECTOR);
  if (!connector) {
    throw UnsupportedError('Invalid connector', { connectorId });
  }
  const capability = normalizeEnrichmentBatchCapability(connector.enrichment_batch_capability ?? null);
  if (!capability) {
    throw FunctionalError('Connector does not advertise enrichment batch support', { connectorId });
  }
  const envelope = parseEnrichmentBatchEnvelope(envelopeInput, capability);
  if (envelope.group_context.connector_id !== connector.internal_id) {
    throw FunctionalError('Enrichment batch envelope does not belong to connector', {
      connector_id: connector.internal_id,
      envelope_connector_id: envelope.group_context.connector_id,
    });
  }
  return { connector, capability, envelope };
};

const loadAndAssertBatchWorksBelongToConnector = async (
  context: AuthContext,
  user: AuthUser,
  connector: BasicStoreEntityConnector,
  workIds: string[],
) => {
  const worksById = await elFindByIds<Work>(context, user, workIds, {
    type: ENTITY_TYPE_WORK,
    indices: READ_INDEX_HISTORY,
    toMap: true,
  }) as Record<string, Work>;
  for (const workId of workIds) {
    const work = worksById[workId];
    if (!work || work.connector_id !== connector.internal_id) {
      throw FunctionalError('Enrichment batch lifecycle references a Work outside the connector partition', {
        connector_id: connector.internal_id,
        work_id: workId,
      });
    }
  }
  return worksById;
};

const buildEnrichmentBatchWorkMessage = (result: EnrichmentBatchResultEnvelope['results'][number]) => {
  if (result.message) {
    return result.message;
  }
  if (result.status === EnrichmentBatchResultStatus.Unchanged) {
    return 'No changes produced by connector';
  }
  return 'Connector successfully processed the operation';
};

const settleEnrichmentBatchResultWorks = async (
  context: AuthContext,
  user: AuthUser,
  worksById: Record<string, Work>,
  resultEnvelope: EnrichmentBatchResultEnvelope,
) => {
  await updateProcessedTimes(context, user, resultEnvelope.results.map((result) => ({
    work: worksById[result.work_id],
    message: buildEnrichmentBatchWorkMessage(result),
    inError: result.status === EnrichmentBatchResultStatus.Failed,
  })));
};

const submitTerminalEnrichmentBatchResult = async (
  context: AuthContext,
  user: AuthUser,
  connector: BasicStoreEntityConnector,
  capability: EnrichmentBatchCapability,
  envelope: EnrichmentBatchEnvelope,
  candidateResultEnvelope: EnrichmentBatchResultEnvelope,
): Promise<boolean> => {
  if (candidateResultEnvelope.results.some((result) => result.status === EnrichmentBatchResultStatus.Retryable)) {
    throw FunctionalError('Retryable enrichment batch results cannot be submitted for terminal settlement', {
      connector_id: connector.internal_id,
      batch_id: envelope.batch_id,
    });
  }
  const worksById = await loadAndAssertBatchWorksBelongToConnector(
    context,
    user,
    connector,
    candidateResultEnvelope.results.map((result) => result.work_id),
  );
  const serializedEnvelope = serializeEnrichmentBatchEnvelope(envelope);
  const receipt = await reserveEnrichmentBatchResultReceipt(context, {
    connectorId: connector.internal_id,
    batchId: envelope.batch_id,
    serializedEnvelope,
    serializedResult: serializeEnrichmentBatchResultEnvelope(candidateResultEnvelope),
  });
  const resultEnvelope = parseEnrichmentBatchResultEnvelope(
    readEnrichmentBatchResultReceiptPayload(receipt),
    envelope,
    capability,
  );

  const processedResults = resultEnvelope.results.filter((result) => result.status === EnrichmentBatchResultStatus.Processed);
  if (resultEnvelope.output_bundle && processedResults.length > 0) {
    const primaryWorkId = processedResults[0].work_id;
    await submitStixBundle(context, user, connector.internal_id, resultEnvelope.output_bundle, primaryWorkId, {
      idempotencyKey: `${ENRICHMENT_BATCH_RESULT_IDEMPOTENCY_PREFIX}${resultEnvelope.batch_id}`,
      enrichmentBatchResult: serializeEnrichmentBatchResultEnvelope(resultEnvelope),
      additionalWorkIds: processedResults.slice(1).map((result) => result.work_id),
      fingerprintContext: buildEnrichmentBatchResultFingerprintContext(resultEnvelope),
    });
  }
  await settleEnrichmentBatchResultWorks(context, user, worksById, resultEnvelope);
  return true;
};

export const submitEnrichmentBatchReceived = async (
  context: AuthContext,
  user: AuthUser,
  connectorId: string,
  envelopeInput: string,
): Promise<boolean> => {
  const { connector, envelope } = await loadValidatedEnrichmentBatchContext(context, user, connectorId, envelopeInput);
  const workIds = envelope.items.map((item) => item.work_id);
  const worksById = await loadAndAssertBatchWorksBelongToConnector(context, user, connector, workIds);
  await updateReceivedTimes(context, user, workIds.map((workId) => ({
    work: worksById[workId],
    message: ENRICHMENT_BATCH_RECEIVED_MESSAGE,
  })));
  return true;
};

export const submitEnrichmentBatchFailure = async (
  context: AuthContext,
  user: AuthUser,
  connectorId: string,
  envelopeInput: string,
  message: string,
): Promise<boolean> => {
  const { connector, capability, envelope } = await loadValidatedEnrichmentBatchContext(context, user, connectorId, envelopeInput);
  const resultEnvelope = buildEnrichmentBatchResultEnvelope(
    envelope,
    envelope.items.map((item) => ({
      itemId: item.item_id,
      workId: item.work_id,
      status: EnrichmentBatchResultStatus.Failed,
      message,
    })),
    null,
    capability,
  );
  return submitTerminalEnrichmentBatchResult(context, user, connector, capability, envelope, resultEnvelope);
};

export const submitEnrichmentBatchResult = async (
  context: AuthContext,
  user: AuthUser,
  connectorId: string,
  envelopeInput: string,
  resultInput: string,
): Promise<boolean> => {
  const { connector, capability, envelope } = await loadValidatedEnrichmentBatchContext(context, user, connectorId, envelopeInput);
  const resultEnvelope = parseEnrichmentBatchResultEnvelope(resultInput, envelope, capability);
  return submitTerminalEnrichmentBatchResult(context, user, connector, capability, envelope, resultEnvelope);
};
