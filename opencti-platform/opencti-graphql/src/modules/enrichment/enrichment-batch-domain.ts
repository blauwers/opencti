import { FunctionalError, UnsupportedError } from '../../config/errors';
import { elFindByIds } from '../../database/engine';
import { storeLoadById } from '../../database/middleware-loader';
import { READ_INDEX_HISTORY } from '../../database/utils';
import { submitStixBundle } from '../../domain/stix';
import { ENTITY_TYPE_CONNECTOR, ENTITY_TYPE_WORK } from '../../schema/internalObject';
import type { AuthContext, AuthUser } from '../../types/user';
import type { BasicStoreEntityConnector } from '../../types/connector';
import type { Work } from '../../types/work';
import {
  normalizeEnrichmentBatchCapability,
  parseEnrichmentBatchEnvelope,
  parseEnrichmentBatchResultEnvelope,
  serializeEnrichmentBatchResultEnvelope,
  type EnrichmentBatchResultEnvelope,
  EnrichmentBatchResultStatus,
} from './enrichment-batch-contract';

const ENRICHMENT_BATCH_RESULT_IDEMPOTENCY_PREFIX = 'enrichment-batch-result:';

const buildEnrichmentBatchResultFingerprintContext = (resultEnvelope: EnrichmentBatchResultEnvelope) => ({
  protocol_version: resultEnvelope.protocol_version,
  batch_id: resultEnvelope.batch_id,
  result_count: resultEnvelope.result_count,
  output_object_count: resultEnvelope.output_object_count,
  results: resultEnvelope.results,
});

const assertBatchResultWorksBelongToConnector = async (
  context: AuthContext,
  user: AuthUser,
  connector: BasicStoreEntityConnector,
  resultEnvelope: EnrichmentBatchResultEnvelope,
) => {
  const workIds = resultEnvelope.results.map((result) => result.work_id);
  const worksById = await elFindByIds<Work>(context, user, workIds, {
    type: ENTITY_TYPE_WORK,
    indices: READ_INDEX_HISTORY,
    toMap: true,
  }) as Record<string, Work>;
  for (const result of resultEnvelope.results) {
    const work = worksById[result.work_id];
    if (!work || work.connector_id !== connector.internal_id) {
      throw FunctionalError('Enrichment batch result references a Work outside the connector partition', {
        connector_id: connector.internal_id,
        work_id: result.work_id,
      });
    }
  }
};

export const submitEnrichmentBatchResult = async (
  context: AuthContext,
  user: AuthUser,
  connectorId: string,
  envelopeInput: string,
  resultInput: string,
): Promise<boolean> => {
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
  const resultEnvelope = parseEnrichmentBatchResultEnvelope(resultInput, envelope, capability);
  await assertBatchResultWorksBelongToConnector(context, user, connector, resultEnvelope);

  const processedResults = resultEnvelope.results.filter((result) => result.status === EnrichmentBatchResultStatus.Processed);
  if (!resultEnvelope.output_bundle || processedResults.length === 0) {
    return true;
  }
  const primaryWorkId = processedResults[0].work_id;
  await submitStixBundle(context, user, connectorId, resultEnvelope.output_bundle, primaryWorkId, {
    idempotencyKey: `${ENRICHMENT_BATCH_RESULT_IDEMPOTENCY_PREFIX}${resultEnvelope.batch_id}`,
    enrichmentBatchResult: serializeEnrichmentBatchResultEnvelope(resultEnvelope),
    additionalWorkIds: processedResults.slice(1).map((result) => result.work_id),
    fingerprintContext: buildEnrichmentBatchResultFingerprintContext(resultEnvelope),
  });
  return true;
};
