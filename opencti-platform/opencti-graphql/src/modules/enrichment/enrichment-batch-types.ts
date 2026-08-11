export const ENTITY_TYPE_ENRICHMENT_BATCH_RESULT_RECEIPT = 'EnrichmentBatchResultReceipt';

export interface EnrichmentBatchResultReceipt {
  id: string;
  internal_id: string;
  _index?: string;
  standard_id: string;
  entity_type: typeof ENTITY_TYPE_ENRICHMENT_BATCH_RESULT_RECEIPT;
  base_type: 'ENTITY';
  parent_types: string[];
  connector_id: string;
  batch_id: string;
  envelope_fingerprint: string;
  result_payload_version: 1;
  result_fingerprint: string;
  result_payload: string;
  created_at: string;
  updated_at: string;
}
