import { Promise } from 'bluebird';
import { map } from 'ramda';
import { createWork, createWorks } from './work';
import { pushToConnector } from '../database/rabbitmq';
import { ENTITY_TYPE_CONNECTOR } from '../schema/internalObject';
import { isStixObject } from '../schema/stixCoreObject';
import { getEntitiesListFromCache } from '../database/cache';
import { CONNECTOR_INTERNAL_ENRICHMENT, ENRICHMENT_RESOLUTION_DEFERRED, ENRICHMENT_RESOLUTION_STIX_BUNDLE, INPUT_GRANTED_REFS } from '../schema/general';
import { isStixMatchFilterGroup } from '../utils/filtering/filtering-stix/stix-filtering';
import { isFilterGroupNotEmpty } from '../utils/filtering/filtering-utils';
import { isUserCanAccessStoreElement, SYSTEM_USER } from '../utils/access';
import { getDraftContext } from '../utils/draftContext';
import { promiseMap } from '../utils/promiseUtils';
import { resolveUserByIdFromCache } from './user';
import { convertStoreToStix_2_1 } from '../database/stix-2-1-converter';
import {
  buildEnrichmentBatchEnvelope,
  buildEnrichmentBatchGroupContext,
  EnrichmentBatchContractErrorCode,
  EnrichmentBatchMode,
  EnrichmentBatchTrigger,
  normalizeEnrichmentBatchCapability,
  serializeEnrichmentBatchEnvelope,
} from '../modules/enrichment/enrichment-batch-contract';
import { BATCH_SIDE_EFFECT_MAX_ACTIVE_AUTO_ENRICHMENTS, BATCH_SIDE_EFFECT_SEAL_DESCRIPTORS, BatchSideEffectKind } from '../modules/batch/batch-executor';

const AUTO_ENRICHMENT_BATCH_DESCRIPTOR_KEY = 'auto-enrichment.dispatch.v1';

const resolveEnrichmentResolution = (connector) => connector.enrichment_resolution ?? ENRICHMENT_RESOLUTION_STIX_BUNDLE;

const buildLegacyEnrichmentMessage = (element, work, trigger, draftContext, stixEntity, stixObjects) => ({
  internal: {
    work_id: work.id,
    applicant_id: null,
    draft_id: draftContext ?? null,
    trigger,
    mode: 'auto',
  },
  event: {
    event_type: CONNECTOR_INTERNAL_ENRICHMENT,
    entity_id: element.standard_id,
    entity_type: element.entity_type,
    stix_entity: stixEntity,
    stix_objects: stixObjects,
  },
});

const buildBatchEnrichmentMessage = (envelope) => ({
  internal: {
    work_id: null,
    applicant_id: envelope.group_context.applicant_id,
    draft_id: envelope.group_context.draft_id,
    trigger: envelope.group_context.trigger,
    mode: envelope.group_context.mode,
  },
  event: {
    event_type: CONNECTOR_INTERNAL_ENRICHMENT,
    enrichment_batch: serializeEnrichmentBatchEnvelope(envelope),
  },
});

const extractSharedOrganizationIds = (element) => {
  return (element[INPUT_GRANTED_REFS] ?? [])
    .map((organization) => organization?.standard_id)
    .filter((organizationId) => typeof organizationId === 'string' && organizationId.length > 0);
};

const isEnrichmentBatchLimitError = (error) => {
  return error?.extensions?.data?.enrichment_batch_error_code === EnrichmentBatchContractErrorCode.LimitExceeded;
};

const pushLegacyEnrichmentItem = async (item) => {
  const { connector, element, work, trigger, draftContext, stixEntity, stixObjects } = item;
  await pushToConnector(
    connector.internal_id,
    buildLegacyEnrichmentMessage(element, work, trigger, draftContext, stixEntity, stixObjects),
  );
};

const pushBatchEnvelopeOrLegacyItems = async (connector, capability, items) => {
  if (items.length < 2) {
    await Promise.all(items.map((item) => pushLegacyEnrichmentItem(item)));
    return;
  }
  const pendingItems = [];
  const flushPendingItems = async () => {
    if (pendingItems.length < 2) {
      await Promise.all(pendingItems.map((item) => pushLegacyEnrichmentItem(item)));
      pendingItems.length = 0;
      return;
    }
    const envelope = buildEnrichmentBatchEnvelope(
      pendingItems.map((item) => item.candidate),
      capability,
    );
    await pushToConnector(connector.internal_id, buildBatchEnrichmentMessage(envelope));
    pendingItems.length = 0;
  };

  for (const item of items) {
    try {
      buildEnrichmentBatchEnvelope(
        [...pendingItems, item].map((pendingItem) => pendingItem.candidate),
        capability,
      );
      pendingItems.push(item);
    } catch (error) {
      if (!isEnrichmentBatchLimitError(error)) {
        throw error;
      }
      await flushPendingItems();
      try {
        buildEnrichmentBatchEnvelope([item.candidate], capability);
        pendingItems.push(item);
      } catch (singleItemError) {
        if (!isEnrichmentBatchLimitError(singleItemError)) {
          throw singleItemError;
        }
        await pushLegacyEnrichmentItem(item);
      }
    }
  }
  await flushPendingItems();
};

const buildPreparedEnrichmentItem = async (request, connector, work) => {
  const stixResolutionMode = resolveEnrichmentResolution(connector);
  const stixEntity = stixResolutionMode === ENRICHMENT_RESOLUTION_DEFERRED ? null : await request.loadStixEntity();
  const stixObjects = stixResolutionMode === ENRICHMENT_RESOLUTION_STIX_BUNDLE ? await request.loadStixObjects() : null;
  return {
    connector,
    element: request.element,
    work,
    trigger: request.trigger,
    draftContext: request.draftContext,
    stixEntity,
    stixObjects,
    candidate: {
      connectorId: connector.internal_id,
      workId: work.id,
      entityId: request.element.standard_id,
      entityType: request.element.entity_type,
      applicantId: null,
      draftId: request.draftContext ?? null,
      mode: EnrichmentBatchMode.Auto,
      trigger: request.trigger === 'create' ? EnrichmentBatchTrigger.Create : EnrichmentBatchTrigger.Update,
      resolution: stixResolutionMode,
      playbookContext: null,
      configuration: null,
      sharedOrganizationIds: extractSharedOrganizationIds(request.element),
      stixEntity,
      stixObjects,
    },
  };
};

const buildAutoEnrichmentRequest = (context, user, element, scope, trigger, stixLoaders) => {
  const draftContext = getDraftContext(context, user) || null;
  let stixEntityPromise;
  let stixObjectsPromise;
  return {
    context,
    user,
    element,
    scope,
    trigger,
    draftContext,
    contextOutOfDraft: { ...context, draft_context: '' },
    loadStixEntity: () => {
      stixEntityPromise ??= stixLoaders.loadById();
      return stixEntityPromise;
    },
    loadStixObjects: () => {
      stixObjectsPromise ??= stixLoaders.bundleById();
      return stixObjectsPromise;
    },
  };
};

const publishLegacyEnrichmentRequest = async (request, targetConnectors) => {
  const elementStandardId = request.element.standard_id;
  // Create a work for each connector
  const workMessage = request.draftContext ? `Enrichment (${elementStandardId}) in draft ${request.draftContext}` : `Enrichment (${elementStandardId})`;
  const workList = await Promise.all(
    map((connector) => {
      return createWork(request.contextOutOfDraft, request.user, connector, workMessage, elementStandardId, { draftContext: request.draftContext }).then((work) => {
        return { connector, work };
      });
    }, targetConnectors),
  );
  // Send message to all correct connectors queues
  for (let index = 0; index < workList.length; index += 1) {
    const workListElement = workList[index];
    const { connector, work } = workListElement;
    await pushLegacyEnrichmentItem(await buildPreparedEnrichmentItem(request, connector, work));
  }
  return workList;
};

const publishEventToConnectors = async (context, user, element, targetConnectors, trigger, stixLoaders) => {
  return publishLegacyEnrichmentRequest(
    buildAutoEnrichmentRequest(context, user, element, element.entity_type, trigger, stixLoaders),
    targetConnectors,
  );
};

const publishBatchAutoEnrichmentRequests = async (requests) => {
  const legacyItems = [];
  const selectedItemsByKey = new Map();
  for (const request of requests) {
    if (!isStixObject(request.element.entity_type) || request.element.auto_enrichment_disable) {
      continue;
    }
    const targetConnectors = await findConnectorsForElementEnrichment(
      request.context,
      request.user,
      request.element,
      request.scope,
      { mode: request.trigger === 'create' ? 'creation' : 'update' },
    );
    const legacyConnectors = [];
    for (const connector of targetConnectors) {
      const capability = normalizeEnrichmentBatchCapability(connector.enrichment_batch_capability ?? null);
      if (!capability) {
        legacyConnectors.push(connector);
        continue;
      }
      const dedupeKey = `${connector.internal_id}:${request.trigger}:${request.draftContext ?? ''}:${request.element.standard_id}`;
      selectedItemsByKey.set(dedupeKey, { request, connector, capability });
    }
    if (legacyConnectors.length > 0) {
      legacyConnectors.forEach((connector) => legacyItems.push({ request, connector }));
    }
  }

  const selectedItems = Array.from(selectedItemsByKey.values());
  const workItems = [...legacyItems, ...selectedItems];
  if (workItems.length === 0) {
    return;
  }
  const createdWorks = await createWorks(
    workItems[0].request.contextOutOfDraft,
    workItems[0].request.user,
    workItems.map(({ request, connector }) => ({
      connector,
      friendlyName: request.draftContext
        ? `Enrichment (${request.element.standard_id}) in draft ${request.draftContext}`
        : `Enrichment (${request.element.standard_id})`,
      sourceId: request.element.standard_id,
      args: { draftContext: request.draftContext },
    })),
  );

  await promiseMap(
    legacyItems,
    async ({ request, connector }, index) => {
      await pushLegacyEnrichmentItem(await buildPreparedEnrichmentItem(request, connector, createdWorks[index]));
    },
    BATCH_SIDE_EFFECT_MAX_ACTIVE_AUTO_ENRICHMENTS,
  );

  const preparedItems = await promiseMap(
    selectedItems,
    async ({ request, connector, capability }, index) => {
      return {
        ...(await buildPreparedEnrichmentItem(request, connector, createdWorks[legacyItems.length + index])),
        capability,
      };
    },
    BATCH_SIDE_EFFECT_MAX_ACTIVE_AUTO_ENRICHMENTS,
  );

  const batchGroups = new Map();
  for (const item of preparedItems) {
    const groupContext = buildEnrichmentBatchGroupContext(item.candidate);
    const groupKey = `${item.connector.internal_id}:${groupContext.context_fingerprint}`;
    const batchGroup = batchGroups.get(groupKey) ?? { connector: item.connector, capability: item.capability, items: [] };
    batchGroup.items.push(item);
    batchGroups.set(groupKey, batchGroup);
  }

  for (const batchGroup of batchGroups.values()) {
    await pushBatchEnvelopeOrLegacyItems(batchGroup.connector, batchGroup.capability, batchGroup.items);
  }
};

export const buildAutoEnrichmentSideEffect = (context, user, element, trigger, stixLoaders) => ({
  kind: BatchSideEffectKind.AutoEnrichment,
  sealDescriptor: trigger === 'create'
    ? BATCH_SIDE_EFFECT_SEAL_DESCRIPTORS.autoEnrichmentCreateEntity
    : BATCH_SIDE_EFFECT_SEAL_DESCRIPTORS.autoEnrichmentUpdateEntity,
  batchDescriptor: {
    key: AUTO_ENRICHMENT_BATCH_DESCRIPTOR_KEY,
    execute: async (sideEffects) => publishBatchAutoEnrichmentRequests(sideEffects.map((sideEffect) => sideEffect.batchPayload)),
  },
  batchPayload: buildAutoEnrichmentRequest(context, user, element, element.entity_type, trigger, stixLoaders),
  execute: async () => {
    if (trigger === 'create') {
      await createEntityAutoEnrichment(context, user, element, element.entity_type, stixLoaders);
    } else {
      await updateEntityAutoEnrichment(context, user, element, element.entity_type, stixLoaders);
    }
  },
});

export const updateEntityAutoEnrichment = async (context, user, element, scope, stixLoaders) => {
  if (!isStixObject(element.entity_type)) {
    return null; // we only enrich stix core objects
  }
  if (element.auto_enrichment_disable) {
    return null;
  }
  // Get the list of compatible connectors
  const targetConnectors = await findConnectorsForElementEnrichment(context, user, element, scope, { mode: 'update' });
  return publishEventToConnectors(context, user, element, targetConnectors, 'update', stixLoaders);
};

export const createEntityAutoEnrichment = async (context, user, element, scope, stixLoaders) => {
  if (!isStixObject(element.entity_type)) {
    return null; // we only enrich stix core objects
  }
  if (element.auto_enrichment_disable) {
    return null;
  }
  // Get the list of compatible connectors
  const targetConnectors = await findConnectorsForElementEnrichment(context, user, element, scope, { mode: 'creation' });
  return publishEventToConnectors(context, user, element, targetConnectors, 'create', stixLoaders);
};

const findConnectorsForElementEnrichment = async (context, user, element, scope, opts = {}) => {
  const connectors = await getEntitiesListFromCache(context, user, ENTITY_TYPE_CONNECTOR);
  return filterConnectorsForElementEnrichment(context, connectors, element, scope, opts);
};

export const filterConnectorsForElementEnrichment = async (context, connectors, element, scope, opts = {}) => {
  const { mode = 'creation' } = opts;
  // first filter active & enrichment connectors only
  const activeConnectors = connectors.filter((conn) => conn.active === true && conn.connector_type === CONNECTOR_INTERNAL_ENRICHMENT);
  const targetConnectors = [];
  for (let i = 0; i < activeConnectors.length; i += 1) {
    const conn = activeConnectors[i];
    const scopeMatch = scope ? (conn.connector_scope ?? []).some((s) => s.toLowerCase() === scope.toLowerCase()) : true;
    let hasAccessToElement = false;
    let autoTrigger = false;
    if (mode === 'creation') {
      autoTrigger = conn.connector_trigger_filters ? await isStixMatchConnectorFilter(context, element, conn.connector_trigger_filters) : conn.auto === true;
    } else if (mode === 'update') {
      autoTrigger = conn.auto_update;
    }
    // check access rights of the connector user on the element
    // isUserCanAccessStoreElement
    if (conn.connector_user_id) {
      const connectorUser = await resolveUserByIdFromCache(context, conn.connector_user_id);
      hasAccessToElement = connectorUser && (await isUserCanAccessStoreElement(context, connectorUser, element));
    }
    if (scopeMatch && autoTrigger && hasAccessToElement) {
      targetConnectors.push(conn);
    }
  }
  return targetConnectors;
};

const isStixMatchConnectorFilter = async (context, element, stringFilters) => {
  if (!stringFilters) {
    return true; // no filters -> match all
  }
  const jsonFilters = JSON.parse(stringFilters);
  if (!isFilterGroupNotEmpty(jsonFilters)) {
    return true; // filters empty -> match all
  }
  const stix = convertStoreToStix_2_1(element);
  return isStixMatchFilterGroup(context, SYSTEM_USER, stix, jsonFilters);
};
