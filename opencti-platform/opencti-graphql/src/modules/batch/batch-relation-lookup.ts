import { getInstanceIds } from '../../schema/identifier';
import type { BasicStoreBase } from '../../types/store';
import { getBatchExecutionMetadata, isBatchWriteBoundaryOpen, setBatchExecutionMetadata } from './batch-executor';

const BATCH_CREATED_ENTITY_IDS_METADATA_KEY = 'batch.relation-lookup.created-entity-ids';

type RelationEndpointInput = {
  from?: BasicStoreBase;
  to?: BasicStoreBase;
};

const getBatchCreatedEntityIds = (): Set<string> | undefined => {
  return getBatchExecutionMetadata<Set<string>>(BATCH_CREATED_ENTITY_IDS_METADATA_KEY);
};

export const registerBatchCreatedEntity = (element: BasicStoreBase): void => {
  if (!isBatchWriteBoundaryOpen()) {
    return;
  }
  let createdEntityIds = getBatchCreatedEntityIds();
  if (!createdEntityIds) {
    createdEntityIds = new Set<string>();
    setBatchExecutionMetadata(BATCH_CREATED_ENTITY_IDS_METADATA_KEY, createdEntityIds);
  }
  getInstanceIds(element).forEach((id) => createdEntityIds?.add(id));
};

export const hasBatchCreatedEntityParticipant = (participantIds: string[]): boolean => {
  if (!isBatchWriteBoundaryOpen()) {
    return false;
  }
  const createdEntityIds = getBatchCreatedEntityIds();
  return createdEntityIds !== undefined && participantIds.some((participantId) => createdEntityIds.has(participantId));
};

export const hasBatchCreatedRelationEndpoint = (input: RelationEndpointInput): boolean => {
  if (!isBatchWriteBoundaryOpen()) {
    return false;
  }
  const createdEntityIds = getBatchCreatedEntityIds();
  if (!createdEntityIds) {
    return false;
  }
  return [input.from, input.to].some((endpoint) => {
    return endpoint ? getInstanceIds(endpoint).some((id) => createdEntityIds.has(id)) : false;
  });
};
