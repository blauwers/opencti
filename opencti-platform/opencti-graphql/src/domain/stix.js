import mime from 'mime-types';
import { invertObj, map } from 'ramda';
import { deleteElementById, mergeEntities, updateAttribute } from '../database/middleware';
import { isStixObject } from '../schema/stixCoreObject';
import { isStixRelationship } from '../schema/stixRelationship';
import { FunctionalError, UnsupportedError } from '../config/errors';
import { connectorsForExport } from './connector';
import { findById as findMarkingDefinitionById, markingDefinitionDeleteAndUpdateGroups } from './markingDefinition';
import { now, observableValue } from '../utils/format';
import { createWork, loadWorkById, updateBatchSubmissionExpectation, updateExpectationsNumber } from './work';
import { pushToConnector, pushToWorkerForConnector } from '../database/rabbitmq';
import { isStixDomainObjectShareableContainer } from '../schema/stixDomainObject';
import { ABSTRACT_STIX_CORE_OBJECT, ABSTRACT_STIX_OBJECT, buildRefRelationKey, CONNECTOR_INTERNAL_EXPORT_FILE, INPUT_GRANTED_REFS } from '../schema/general';
import { isEmptyField, UPDATE_OPERATION_ADD, UPDATE_OPERATION_REMOVE } from '../database/utils';
import { extractEntityRepresentativeName } from '../database/entity-representative';
import { notify } from '../database/redis';
import { BUS_TOPICS } from '../config/conf';
import { internalFindByIds, internalLoadById, storeLoadById } from '../database/middleware-loader';
import { completeContextDataForEntity, publishUserAction } from '../listener/UserActionListener';
import { checkAndConvertFilters } from '../utils/filtering/filtering-utils';
import { specialTypesExtensions } from '../database/file-storage';
import { getExportFilter } from '../utils/getExportFilter';
import { getEntitiesListFromCache } from '../database/cache';
import { ENTITY_TYPE_MARKING_DEFINITION } from '../schema/stixMetaObject';
import { checkUserCanShareMarkings } from './user';
import { ENTITY_TYPE_CONNECTOR } from '../schema/internalObject';
import { ACTION_TYPE_SHARE, ACTION_TYPE_UNSHARE, createListTask } from './backgroundTask-common';
import { objectOrganization, RELATION_GRANTED_TO } from '../schema/stixRefRelationship';
import { ENTITY_TYPE_IDENTITY_ORGANIZATION } from '../modules/organization/organization-types';
import { elFindByIds } from '../database/engine';
import { addExportGeneratedCount } from '../manager/telemetryManager';
import {
  assertBatchReplayPayload,
  buildBatchAdmission,
  buildBatchQueueMessage,
  buildBatchReplayLockId,
  hasExplicitBatchIdempotencyKey,
  prepareBundleSubmission,
} from '../modules/batch/batch-domain';
import {
  advanceBatchDeliveryState,
  buildRootBatchDeliveryEnvelope,
  buildRootBatchDeliveryId,
  isBatchDeliveryStateAtLeast,
  readBatchDeliveryQueueMessage,
  recordBatchDeliveryError,
  reserveBatchDelivery,
} from '../modules/batch/batch-delivery-domain';
import {
  buildBatchSubmissionId,
  ensureBatchSubmissionDeliveryMetadata,
  isBatchSubmissionStateAtLeast,
  loadBatchSubmission,
  readBatchSubmissionQueueMessage,
  recordBatchSubmissionError,
  reserveBatchSubmission,
  advanceBatchSubmissionState,
} from '../modules/batch/batch-submission-domain';
import { resolveRequiredBatchDeliveryProtocol } from '../modules/batch/batch-worker-runtime-domain';
import {
  BatchDeliveryBranchKind,
  BatchDeliveryKind,
  BatchDeliveryProtocol,
  BatchDeliveryState,
  BatchExecutionMode,
  BatchSubmissionState,
  BatchSubmissionWorkOrigin,
} from '../modules/batch/batch-types';
import { lockResources } from '../lock/master-lock';
import { generateWorkId } from '../schema/identifier';

export const stixDelete = async (context, user, id, opts = {}) => {
  const element = await internalLoadById(context, user, id);
  if (element) {
    if (isStixObject(element.entity_type) || isStixRelationship(element.entity_type)) {
      // To handle delete synchronization events, we force the forceDelete flag to true, because we don't want delete events to create trash entries on synchronized platforms
      // THIS IS NOT IDEAL: we ideally would need to add the forceDelete flag to all delete related methods on the API,
      // and let the worker call this method with the flag set to true in case of synchronization
      if (element.entity_type === ENTITY_TYPE_MARKING_DEFINITION) {
        await markingDefinitionDeleteAndUpdateGroups(context, user, element.id, { forceDelete: true });
      } else {
        const forceDelete = opts.forceDelete !== undefined ? opts.forceDelete : true;
        await deleteElementById(context, user, element.id, element.entity_type, { forceDelete });
      }
      return element.id;
    }
    throw UnsupportedError('This method can only delete Stix element', { id: element.id, type: element.entity_type });
  }
  throw FunctionalError(`Cannot delete the stix element, ${id} cannot be found.`);
};

export const stixObjectMerge = async (context, user, targetId, sourceIds) => {
  return mergeEntities(context, user, targetId, sourceIds);
};

const buildBatchAdmissionFromSubmission = (submission, preparedBundle) => {
  return {
    ...buildBatchAdmission(
      submission.connector_id,
      submission.work_id,
      preparedBundle,
      submission.internal_id,
      submission.root_delivery_id,
      submission.required_delivery_protocol,
    ),
    batchId: submission.bundle_id,
    bundleId: submission.bundle_id,
    executionPreference: submission.execution_preference,
    executionMode: submission.execution_mode,
    executionReason: submission.execution_reason,
    eligibleExecutionModes: submission.eligible_execution_modes,
    waitUntil: submission.wait_until,
    idempotencyKey: submission.idempotency_key,
    cleanupInconsistentBundle: submission.cleanup_inconsistent_bundle,
  };
};

const buildTrackedBatchQueueMessage = (admission, applicantId, requiredDeliveryProtocol) => {
  if (requiredDeliveryProtocol !== BatchDeliveryProtocol.V2) {
    return buildBatchQueueMessage(admission, applicantId);
  }
  return buildBatchQueueMessage(admission, applicantId, buildRootBatchDeliveryEnvelope(admission.submissionId));
};

const reserveOrLoadBatchSubmission = async (context, user, connector, connectorId, workId, preparedBundle) => {
  const existingSubmission = await loadBatchSubmission(context, connectorId, preparedBundle.idempotencyKey);
  if (existingSubmission) {
    assertBatchReplayPayload(preparedBundle, existingSubmission.payload_fingerprint);
    return ensureBatchSubmissionDeliveryMetadata(
      context,
      existingSubmission,
      buildRootBatchDeliveryId(existingSubmission.internal_id),
    );
  }

  let targetWorkId = workId;
  let workTimestamp = null;
  let workOrigin = BatchSubmissionWorkOrigin.CallerProvided;
  if (isEmptyField(workId)) {
    const generatedWork = generateWorkId(connector.internal_id);
    targetWorkId = generatedWork.id;
    workTimestamp = generatedWork.timestamp;
    workOrigin = BatchSubmissionWorkOrigin.Generated;
  }
  const submissionId = buildBatchSubmissionId(connectorId, preparedBundle.idempotencyKey);
  const rootDeliveryId = buildRootBatchDeliveryId(submissionId);
  const requiredDeliveryProtocol = await resolveRequiredBatchDeliveryProtocol();
  const admission = buildBatchAdmission(
    connectorId,
    targetWorkId,
    preparedBundle,
    submissionId,
    rootDeliveryId,
    requiredDeliveryProtocol,
  );
  return reserveBatchSubmission(context, {
    admission,
    applicantId: user.internal_id,
    payloadFingerprint: preparedBundle.payloadFingerprint,
    queueMessage: buildTrackedBatchQueueMessage(admission, user.internal_id, requiredDeliveryProtocol),
    rootDeliveryId,
    requiredDeliveryProtocol,
    workOrigin,
    workTimestamp,
  });
};

const reserveRootBatchDelivery = async (context, submission) => {
  let rootDelivery = await reserveBatchDelivery(context, {
    deliveryId: submission.root_delivery_id,
    submissionId: submission.internal_id,
    parentDeliveryId: null,
    deliveryKind: BatchDeliveryKind.Root,
    branchKind: BatchDeliveryBranchKind.Root,
    branchSequence: 0,
    branchOrdinal: 0,
    payloadFingerprint: submission.payload_fingerprint,
    queueMessage: readBatchSubmissionQueueMessage(submission),
    requiredWorkerProtocol: submission.required_delivery_protocol,
  });
  if (
    isBatchSubmissionStateAtLeast(submission, BatchSubmissionState.Published)
    && !isBatchDeliveryStateAtLeast(rootDelivery, BatchDeliveryState.Published)
  ) {
    rootDelivery = await advanceBatchDeliveryState(context, rootDelivery, BatchDeliveryState.Published, {
      published_at: submission.published_at ?? now(),
    });
  }
  return rootDelivery;
};

const assertBatchDeliveryPublicationProtocolAvailable = async (submission) => {
  if (submission.required_delivery_protocol !== BatchDeliveryProtocol.V2) {
    return;
  }
  const liveProtocol = await resolveRequiredBatchDeliveryProtocol();
  if (liveProtocol !== BatchDeliveryProtocol.V2) {
    throw FunctionalError('Batch delivery protocol v2 publication is unavailable', {
      submission_id: submission.internal_id,
      root_delivery_id: submission.root_delivery_id,
      required_delivery_protocol: submission.required_delivery_protocol,
      live_delivery_protocol: liveProtocol,
    });
  }
};

const bindBatchSubmissionWork = async (context, user, connector, submission) => {
  if (isBatchSubmissionStateAtLeast(submission, BatchSubmissionState.WorkBound)) {
    return submission;
  }
  if (submission.work_origin === BatchSubmissionWorkOrigin.Generated) {
    const existingWork = await loadWorkById(context, user, submission.work_id);
    if (!existingWork) {
      await createWork(context, user, connector, `${connector.name} run @ ${submission.created_at}`, connector.internal_id, {
        receivedTime: submission.created_at,
        preallocatedWork: {
          id: submission.work_id,
          timestamp: submission.work_timestamp ?? submission.created_at,
        },
      });
    }
  }
  return advanceBatchSubmissionState(context, submission, BatchSubmissionState.WorkBound);
};

const submitExplicitBatch = async (context, user, connector, connectorId, workId, preparedBundle) => {
  let submission = await reserveOrLoadBatchSubmission(context, user, connector, connectorId, workId, preparedBundle);
  let rootDelivery = await reserveRootBatchDelivery(context, submission);
  submission = await bindBatchSubmissionWork(context, user, connector, submission);
  if (!isBatchSubmissionStateAtLeast(submission, BatchSubmissionState.ExpectationRecorded)) {
    if (submission.execution_mode !== BatchExecutionMode.LegacySplit) {
      await updateBatchSubmissionExpectation(context, user, submission.work_id, 1, submission.internal_id);
    }
    submission = await advanceBatchSubmissionState(context, submission, BatchSubmissionState.ExpectationRecorded, {
      expectation_recorded_at: now(),
    });
  }
  if (!isBatchSubmissionStateAtLeast(submission, BatchSubmissionState.Published)) {
    try {
      await assertBatchDeliveryPublicationProtocolAvailable(submission);
      await pushToWorkerForConnector(submission.connector_id, readBatchDeliveryQueueMessage(rootDelivery));
      rootDelivery = await advanceBatchDeliveryState(context, rootDelivery, BatchDeliveryState.Published, {
        published_at: now(),
      });
      submission = await advanceBatchSubmissionState(context, submission, BatchSubmissionState.Published, {
        published_at: now(),
      });
    } catch (error) {
      try {
        await recordBatchDeliveryError(context, rootDelivery, error);
      } catch {
        // Keep the original dispatch error as the retry signal.
      }
      try {
        await recordBatchSubmissionError(context, submission, error);
      } catch {
        // Keep the original dispatch error as the retry signal.
      }
      throw error;
    }
  }
  return buildBatchAdmissionFromSubmission(submission, preparedBundle);
};

const submitUntrackedBatch = async (context, user, connector, connectorId, workId, preparedBundle) => {
  let targetWorkId = workId;
  if (isEmptyField(workId)) {
    const workName = `${connector.name} run @ ${now()}`;
    const work = await createWork(context, user, connector, workName, connector.internal_id, { receivedTime: now() });
    targetWorkId = work.id;
  }
  const admission = buildBatchAdmission(connectorId, targetWorkId, preparedBundle);
  if (admission.executionMode !== BatchExecutionMode.LegacySplit) {
    await updateExpectationsNumber(context, user, targetWorkId, 1);
  }
  await pushToWorkerForConnector(connectorId, buildBatchQueueMessage(admission, user.internal_id));
  return admission;
};

export const submitStixBundle = async (
  context,
  user,
  connectorId,
  bundle,
  work_id,
  options = {},
) => {
  const preparedBundle = prepareBundleSubmission(bundle, options);
  // Create work and send the bundle to ingestion.
  const connector = await storeLoadById(context, user, connectorId, ENTITY_TYPE_CONNECTOR);
  if (!connector) {
    throw UnsupportedError('Invalid connector', { connectorId });
  }
  if (!hasExplicitBatchIdempotencyKey(options)) {
    return submitUntrackedBatch(context, user, connector, connectorId, work_id, preparedBundle);
  }

  let replayLock;
  try {
    replayLock = await lockResources([buildBatchReplayLockId(connectorId, preparedBundle.idempotencyKey)]);
    return await submitExplicitBatch(context, user, connector, connectorId, work_id, preparedBundle);
  } finally {
    if (replayLock) {
      await replayLock.unlock();
    }
  }
};

export const sendStixBundle = async (
  context,
  user,
  connectorId,
  bundle,
  work_id,
  split_bundles = false,
  cleanup_inconsistent_bundle = false,
  wait_until = /** @type {string | null | undefined} */ (undefined),
) => {
  try {
    await submitStixBundle(context, user, connectorId, bundle, work_id, {
      splitBundles: split_bundles,
      cleanupInconsistentBundle: cleanup_inconsistent_bundle,
      waitUntil: wait_until,
    });
    return true;
  } catch (err) {
    throw UnsupportedError('Invalid bundle', { cause: err });
  }
};

export const askListExport = async (context, user, exportContext, format, selectedIds, listParams, type, contentMaxMarkings, fileMarkings) => {
  if (!exportContext || !exportContext?.entity_type) {
    throw FunctionalError('entity_type is missing from askListExport');
  }

  // Telemetry: one export generation requested (attempts semantics).
  addExportGeneratedCount();

  const connectors = await connectorsForExport(context, user, format, true);
  const markingLevels = await Promise.all(contentMaxMarkings.map(async (id) => {
    return await findMarkingDefinitionById(context, user, id);
  }));
  await checkUserCanShareMarkings(context, user, markingLevels);
  const fileNameMarkingLevels = markingLevels.map((markingLevel) => markingLevel?.definition).join('_');

  const entity = exportContext.entity_id ? await storeLoadById(context, user, exportContext.entity_id, ABSTRACT_STIX_CORE_OBJECT) : null;
  const { entity_type } = exportContext;

  const toFileName = (connector) => {
    const fileNamePart = `${entity_type}_${type}.${mime.extension(format) ? mime.extension(format) : specialTypesExtensions[format] ?? 'unknown'}`;
    return `${now()}_${fileNameMarkingLevels || 'TLP:ALL'}_(${connector.name})_${fileNamePart}`;
  };

  const markingList = await getEntitiesListFromCache(context, user, ENTITY_TYPE_MARKING_DEFINITION);

  const { markingFilter, mainFilter } = await getExportFilter(user, { markingList, contentMaxMarkings, objectIdsList: selectedIds });
  const extendedListParams = { ...listParams, visible_columns: exportContext.visible_columns };

  const baseEvent = {
    format, // extension mime type
    export_type: type, // Simple or full
    // Related to entity (if export concern, must be hosted on a specific entity)
    entity_id: entity?.id,
    entity_name: entity ? extractEntityRepresentativeName(entity) : 'global',
    entity_type, // Exported entity type
    // All the params needed to execute the export on python connector
    file_markings: fileMarkings,
    main_filter: mainFilter,
    access_filter: markingFilter,
  };
  const buildExportMessage = (work, fileName) => {
    const internal = {
      work_id: work.id, // Related action for history
      applicant_id: user.id, // User asking for the import
    };
    if (selectedIds && selectedIds.length > 0) {
      return {
        internal,
        event: {
          event_type: CONNECTOR_INTERNAL_EXPORT_FILE,
          export_scope: 'selection', // query or selection or single
          file_name: fileName, // Export expected file name
          selected_ids: selectedIds, // ids that are both selected via checkboxes and respect the filtering
          list_params: extendedListParams,
          ...baseEvent,
        },
      };
    }
    return {
      internal,
      event: {
        export_scope: 'query', // query or selection or single
        file_name: fileName, // Export expected file name
        list_params: extendedListParams,
        ...baseEvent,
      },
    };
  };
  // noinspection UnnecessaryLocalVariableJS
  const worksForExport = await Promise.all(
    map(async (connector) => {
      const fileIdentifier = toFileName(connector);
      const path = `export/${entity_type}${entity ? `/${entity.id}` : ''}`;
      const work = await createWork(context, user, connector, fileIdentifier, path, { fileMarkings });
      const message = buildExportMessage(work, fileIdentifier);
      await pushToConnector(connector.internal_id, message);
      return work;
    }, connectors),
  );
  await publishUserAction({
    user,
    event_access: 'extended',
    event_type: 'command',
    event_scope: 'export',
    context_data: baseEvent,
  });
  return worksForExport;
};

export const askEntityExport = async (context, user, format, entity, type, contentMaxMarkings, fileMarkings) => {
  // Telemetry: one export generation requested (attempts semantics).
  addExportGeneratedCount();

  const connectors = await connectorsForExport(context, user, format, true);
  const markingLevels = await Promise.all(contentMaxMarkings.map(async (id) => {
    return await findMarkingDefinitionById(context, user, id);
  }));
  await checkUserCanShareMarkings(context, user, markingLevels);
  const fileNameMarkingLevels = markingLevels.map((markingLevel) => markingLevel?.definition).join('_');
  const toFileName = (connector) => {
    const fileNamePart = `${entity.entity_type}-${entity.name || observableValue(entity)}_${type}.${mime.extension(format) ? mime.extension(format) : specialTypesExtensions[format] ?? 'unknown'}`;
    return `${now()}_${fileNameMarkingLevels || 'TLP:ALL'}_(${connector.name})_${fileNamePart}`;
  };
  const markingList = await getEntitiesListFromCache(context, user, ENTITY_TYPE_MARKING_DEFINITION);
  const { markingFilter, mainFilter } = await getExportFilter(user, { markingList, contentMaxMarkings, objectIdsList: [entity.id] });

  const baseEvent = {
    format,
    export_scope: 'single', // query or selection or single
    entity_id: entity.id, // Location of the file export = the exported element
    entity_name: extractEntityRepresentativeName(entity),
    entity_type: entity.entity_type, // Exported entity type
    export_type: type, // Simple or full
    file_markings: fileMarkings,
    main_filter: mainFilter,
    access_filter: markingFilter,
  };
  const buildExportMessage = (work, fileName) => {
    return {
      internal: {
        work_id: work.id, // Related action for history
        applicant_id: user.id, // User asking for the import
        trigger: null, // Export as no specific trigger
        mode: 'manual',
      },
      event: {
        event_type: CONNECTOR_INTERNAL_EXPORT_FILE,
        file_name: fileName, // Export expected file name
        ...baseEvent,
      },
    };
  };

  // noinspection UnnecessaryLocalVariableJS
  const worksForExport = await Promise.all(
    map(async (connector) => { // can be refactored to native map
      const fileIdentifier = toFileName(connector);
      const path = `export/${entity.entity_type}/${entity.id}`;
      const work = await createWork(context, user, connector, fileIdentifier, path, { fileMarkings });
      const message = buildExportMessage(work, fileIdentifier);
      await pushToConnector(connector.internal_id, message);
      return work;
    }, connectors),
  );
  const contextData = completeContextDataForEntity(baseEvent, entity);
  await publishUserAction({
    user,
    event_access: 'extended',
    event_type: 'command',
    event_scope: 'export',
    context_data: contextData,
  });
  return worksForExport;
};

export const exportTransformFilters = async (context, user, filteringArgs, orderOptions, userId) => {
  const orderingInversed = invertObj(orderOptions);
  const { filters } = filteringArgs;
  const convertedFilters = await checkAndConvertFilters(context, user, filters, userId, elFindByIds);
  return {
    ...filteringArgs,
    orderBy: filteringArgs.orderBy in orderingInversed
      ? orderingInversed[filteringArgs.orderBy]
      : filteringArgs.orderBy,
    filters: convertedFilters,
  };
};

const createSharingTask = async (context, type, containerId, organizationId) => {
  const organizationIds = Array.isArray(organizationId) ? organizationId : [organizationId];
  const organizations = await internalFindByIds(context, context.user, organizationIds, { baseData: true, baseFields: ['name'] });
  const organizationNames = organizations.map((o) => o.name).join('|');
  const sharingDescription = `${type} with organization ${organizationNames}`;
  // orderMode is on created_at, see buildQueryFilters in backgroundTask
  // need to be desc for share/unshare to have events in the right order in stream (entity send before relations)
  // containerId required to send an event after all container content is shared.
  const input = {
    description: sharingDescription,
    ids: [containerId],
    actions: [{ type, context: { values: organizationIds }, containerId }],
    scope: 'KNOWLEDGE',
    orderMode: 'asc',
  };
  await createListTask(context, context.user, input);
};

export const addOrganizationRestriction = async (context, user, fromId, organizationId, directContainerSharing) => {
  const organizationIds = Array.isArray(organizationId) ? organizationId : [organizationId];
  const from = await internalLoadById(context, user, fromId);
  const currentGrants = from[buildRefRelationKey(RELATION_GRANTED_TO)] ?? [];
  const organizationsNotCurrentlyGranted = organizationIds.filter((o) => !currentGrants.includes(o));
  // If entity is not sharable or if entity is already shared with organizations, we can return without doing anything
  if (!objectOrganization.isRefExistingForTypes(from.entity_type, ENTITY_TYPE_IDENTITY_ORGANIZATION)
    || organizationsNotCurrentlyGranted.length === 0
  ) {
    return from;
  }

  // If container, create a sharing task
  if (isStixDomainObjectShareableContainer(from.entity_type) && !directContainerSharing) {
    await createSharingTask(context, ACTION_TYPE_SHARE, from.internal_id, organizationsNotCurrentlyGranted);
    return from;
  }
  // If standard, just share directly
  const updates = [{ key: INPUT_GRANTED_REFS, value: organizationsNotCurrentlyGranted, operation: UPDATE_OPERATION_ADD }];
  // We skip references validation when updating organization sharing
  const data = await updateAttribute(context, user, fromId, from.entity_type, updates, { bypassValidation: true });
  return notify(BUS_TOPICS[ABSTRACT_STIX_OBJECT].EDIT_TOPIC, data.element, user);
};

export const removeOrganizationRestriction = async (context, user, fromId, organizationId, directContainerSharing) => {
  const organizationIds = Array.isArray(organizationId) ? organizationId : [organizationId];
  const from = await internalLoadById(context, user, fromId);
  const currentGrants = from[buildRefRelationKey(RELATION_GRANTED_TO)] ?? [];
  const organizationsCurrentlyGranted = organizationIds.filter((o) => currentGrants.includes(o));
  // If entity is not sharable or if entity is already shared with organizations, we can return without doing anything
  if (!objectOrganization.isRefExistingForTypes(from.entity_type, ENTITY_TYPE_IDENTITY_ORGANIZATION)
    || organizationsCurrentlyGranted.length === 0
  ) {
    return from;
  }
  // If container, create a sharing task
  if (isStixDomainObjectShareableContainer(from.entity_type) && !directContainerSharing) {
    await createSharingTask(context, ACTION_TYPE_UNSHARE, from.internal_id, organizationsCurrentlyGranted);
    return from;
  }
  // If standard, just share directly
  const updates = [{ key: INPUT_GRANTED_REFS, value: organizationsCurrentlyGranted, operation: UPDATE_OPERATION_REMOVE }];
  // We skip references validation when updating organization sharing
  const data = await updateAttribute(context, user, fromId, from.entity_type, updates, { bypassValidation: true });
  return notify(BUS_TOPICS[ABSTRACT_STIX_OBJECT].EDIT_TOPIC, data.element, user);
};
