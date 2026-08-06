import * as R from 'ramda';
import { delEditContext, notify, setEditContext } from '../database/redis';
import { createEntity, deleteElementById, updateAttribute } from '../database/middleware';
import { internalFindByIds, fullEntitiesList, fullRelationsList, topEntitiesList, pageEntitiesConnection, storeLoadById } from '../database/middleware-loader';
import { booleanConf, BUS_TOPICS, logApp } from '../config/conf';
import { ENTITY_TYPE_MARKING_DEFINITION } from '../schema/stixMetaObject';
import { ENTITY_TYPE_GROUP, ENTITY_TYPE_USER } from '../schema/internalObject';
import { SYSTEM_USER } from '../utils/access';
import { RELATION_ACCESSES_TO, RELATION_MEMBER_OF } from '../schema/internalRelationship';
import { groupAddRelation, groupEditField, groupMaxShareableMarkings } from './group';
import { READ_RELATIONSHIPS_INDICES } from '../database/utils';

const BATCH_MARKING_PERFORMANCE_LOG = booleanConf('app:performance_logger', false);
const BATCH_MARKING_SIDE_EFFECT_LOG_MESSAGE = '[BATCH] Marking definition side effect';

const logBatchMarkingStep = (event, step, startedAt, extra = {}) => {
  if (!BATCH_MARKING_PERFORMANCE_LOG) {
    return;
  }
  logApp.info(BATCH_MARKING_SIDE_EFFECT_LOG_MESSAGE, {
    event,
    step,
    ...(event === 'started' ? {} : { duration_ms: Date.now() - startedAt }),
    ...extra,
  });
};

export const findById = (context, user, markingDefinitionId) => {
  return storeLoadById(context, user, markingDefinitionId, ENTITY_TYPE_MARKING_DEFINITION);
};

// Force looking with prefix wildcard for markings
export const findMarkingsPaginated = (context, user, args) => {
  return pageEntitiesConnection(context, user, [ENTITY_TYPE_MARKING_DEFINITION], { ...args, useWildcardPrefix: true });
};

// Force looking with prefix wildcard for markings
export const findAllMarkings = (context, user, args) => {
  return fullEntitiesList(context, user, [ENTITY_TYPE_MARKING_DEFINITION], { ...args, useWildcardPrefix: true });
};

const notifyMembersOfNewMarking = async (context, user, newMarking) => {
  const markingTrace = { marking_id: newMarking.id, marking_type: newMarking.definition_type };
  const loadMarkingsStartedAt = Date.now();
  logBatchMarkingStep('started', 'notify_members.load_markings', loadMarkingsStartedAt, markingTrace);
  const allMarkings = await findAllMarkings(context, SYSTEM_USER);
  logBatchMarkingStep('completed', 'notify_members.load_markings', loadMarkingsStartedAt, {
    ...markingTrace,
    marking_count: allMarkings.length,
  });
  const userGroupedMarkings = R.groupBy((m) => m.definition_type, allMarkings);
  const otherExistingTypeMarkingIds = (userGroupedMarkings[newMarking.definition_type] ?? []).map((m) => m.internal_id);
  const groupIds = new Set();
  const groupUsers = new Map();
  const loadRelationsStartedAt = Date.now();
  logBatchMarkingStep('started', 'notify_members.load_relations', loadRelationsStartedAt, {
    ...markingTrace,
    existing_type_marking_count: otherExistingTypeMarkingIds.length,
  });
  const relations = await fullRelationsList(context, SYSTEM_USER, [RELATION_ACCESSES_TO, RELATION_MEMBER_OF], { indices: READ_RELATIONSHIPS_INDICES });
  logBatchMarkingStep('completed', 'notify_members.load_relations', loadRelationsStartedAt, {
    ...markingTrace,
    existing_type_marking_count: otherExistingTypeMarkingIds.length,
    relation_count: relations.length,
  });
  for (let index = 0; index < relations.length; index += 1) {
    // group <- RELATION_ACCESSES_TO -> marking
    const { fromId, entity_type, toId } = relations[index];
    if (entity_type === RELATION_ACCESSES_TO && otherExistingTypeMarkingIds.includes(toId)) {
      groupIds.add(fromId);
    }
    // user <- RELATION_MEMBER_OF -> group
    if (entity_type === RELATION_MEMBER_OF) {
      if (groupUsers.has(toId)) {
        const users = groupUsers.get(toId);
        users.push(fromId);
        groupUsers.set(toId, users);
      } else {
        groupUsers.set(toId, [fromId]);
      }
    }
  }
  const groups = Array.from(groupIds);
  if (groups.length > 0) {
    const userIds = groups.map((groupId) => (groupUsers.get(groupId) ?? [])).flat();
    const loadUsersStartedAt = Date.now();
    logBatchMarkingStep('started', 'notify_members.load_users', loadUsersStartedAt, {
      ...markingTrace,
      group_count: groups.length,
      user_count: userIds.length,
    });
    const users = await internalFindByIds(context, SYSTEM_USER, userIds);
    logBatchMarkingStep('completed', 'notify_members.load_users', loadUsersStartedAt, {
      ...markingTrace,
      group_count: groups.length,
      user_count: users.length,
    });
    const notifyUsersStartedAt = Date.now();
    logBatchMarkingStep('started', 'notify_members.notify_users', notifyUsersStartedAt, {
      ...markingTrace,
      group_count: groups.length,
      user_count: users.length,
    });
    await notify(BUS_TOPICS[ENTITY_TYPE_USER].EDIT_TOPIC, users, user);
    logBatchMarkingStep('completed', 'notify_members.notify_users', notifyUsersStartedAt, {
      ...markingTrace,
      group_count: groups.length,
      user_count: users.length,
    });
  }
};

const updateGroupsAfterAddingMarking = async (context, markingCreated) => {
  // marking creation --> update the markings of the groups with auto_new_marking = true
  const filters = {
    mode: 'and',
    filters: [{ key: 'auto_new_marking', values: [true] }],
    filterGroups: [],
  };
  const markingTrace = { marking_id: markingCreated.id, marking_type: markingCreated.definition_type };
  const loadGroupsStartedAt = Date.now();
  logBatchMarkingStep('started', 'update_groups.load_auto_new_marking_groups', loadGroupsStartedAt, markingTrace);
  const groupsWithAutoNewMarking = await topEntitiesList(context, SYSTEM_USER, [ENTITY_TYPE_GROUP], { filters });
  logBatchMarkingStep('completed', 'update_groups.load_auto_new_marking_groups', loadGroupsStartedAt, {
    ...markingTrace,
    group_count: groupsWithAutoNewMarking?.length ?? 0,
  });
  if (groupsWithAutoNewMarking && groupsWithAutoNewMarking.length > 0) {
    const markingId = markingCreated.id;
    const markingType = markingCreated.definition_type;
    // add marking in allowed markings
    const addRelationsStartedAt = Date.now();
    logBatchMarkingStep('started', 'update_groups.add_relations', addRelationsStartedAt, {
      ...markingTrace,
      group_count: groupsWithAutoNewMarking.length,
    });
    await Promise.all(groupsWithAutoNewMarking.map((group) => {
      return groupAddRelation(context, SYSTEM_USER, group.id, { relationship_type: RELATION_ACCESSES_TO, toId: markingId });
    }));
    logBatchMarkingStep('completed', 'update_groups.add_relations', addRelationsStartedAt, {
      ...markingTrace,
      group_count: groupsWithAutoNewMarking.length,
    });
    // add marking in max shareable markings
    const loadMaxShareableMarkingsStartedAt = Date.now();
    logBatchMarkingStep('started', 'update_groups.load_max_shareable_markings', loadMaxShareableMarkingsStartedAt, {
      ...markingTrace,
      group_count: groupsWithAutoNewMarking.length,
    });
    const completeGroupsWithAutoNewMarking = await Promise.all(groupsWithAutoNewMarking.map(async (g) => ({
      ...g,
      max_shareable_marking: await groupMaxShareableMarkings(context, g),
    })));
    logBatchMarkingStep('completed', 'update_groups.load_max_shareable_markings', loadMaxShareableMarkingsStartedAt, {
      ...markingTrace,
      group_count: groupsWithAutoNewMarking.length,
    });
    const groupsWithShareableMarkingToUpdate = completeGroupsWithAutoNewMarking.filter((g) => {
      const shareableMarkingOfTypeWithGreaterOrder = (g.max_shareable_marking ?? [])
        .find((m) => m.definition_type === markingType && m.x_opencti_order > markingCreated.x_opencti_order);
      // we need to update the group max shareable markings if it has no shareable marking of the same definition type with a greater order
      return shareableMarkingOfTypeWithGreaterOrder === undefined;
    });
    const editMaxShareableMarkingsStartedAt = Date.now();
    logBatchMarkingStep('started', 'update_groups.edit_max_shareable_markings', editMaxShareableMarkingsStartedAt, {
      ...markingTrace,
      group_count: groupsWithShareableMarkingToUpdate.length,
    });
    await Promise.all(groupsWithShareableMarkingToUpdate.map((group) => {
      const finalMarkings = [
        ...(group.max_shareable_markings ?? []).filter(({ type: t }) => t !== markingType),
        ...[{ type: markingType, value: markingId }],
      ];
      return groupEditField(context, SYSTEM_USER, group.id, [{
        key: 'max_shareable_markings',
        value: finalMarkings,
      }]);
    }));
    logBatchMarkingStep('completed', 'update_groups.edit_max_shareable_markings', editMaxShareableMarkingsStartedAt, {
      ...markingTrace,
      group_count: groupsWithShareableMarkingToUpdate.length,
    });
  }
};

export const addAllowedMarkingDefinition = async (context, user, markingDefinition) => {
  const markingColor = markingDefinition.x_opencti_color ? markingDefinition.x_opencti_color : '#ffffff';
  const markingToCreate = {
    ...markingDefinition,
    x_opencti_color: markingColor,
  };
  // Force context out of draft to force creation in live index
  const contextOutOfDraft = { ...context, draft_context: '' };
  const createEntityStartedAt = Date.now();
  logBatchMarkingStep('started', 'create_entity', createEntityStartedAt, {
    requested_stix_id: markingDefinition.stix_id,
    marking_type: markingDefinition.definition_type,
  });
  const { element, isCreation } = await createEntity(contextOutOfDraft, user, markingToCreate, ENTITY_TYPE_MARKING_DEFINITION, { complete: true });
  logBatchMarkingStep('completed', 'create_entity', createEntityStartedAt, {
    marking_id: element.id,
    marking_type: element.definition_type,
    is_creation: isCreation,
  });
  if (isCreation) {
    // marking creation --> update the markings of the groups with auto_new_marking = true
    const updateGroupsStartedAt = Date.now();
    logBatchMarkingStep('started', 'update_groups', updateGroupsStartedAt, {
      marking_id: element.id,
      marking_type: element.definition_type,
    });
    await updateGroupsAfterAddingMarking(contextOutOfDraft, element);
    logBatchMarkingStep('completed', 'update_groups', updateGroupsStartedAt, {
      marking_id: element.id,
      marking_type: element.definition_type,
    });
    // users of group impacted must be refreshed
    const notifyMembersStartedAt = Date.now();
    logBatchMarkingStep('started', 'notify_members', notifyMembersStartedAt, {
      marking_id: element.id,
      marking_type: element.definition_type,
    });
    await notifyMembersOfNewMarking(contextOutOfDraft, user, element);
    logBatchMarkingStep('completed', 'notify_members', notifyMembersStartedAt, {
      marking_id: element.id,
      marking_type: element.definition_type,
    });
  }
  const notifyAddedStartedAt = Date.now();
  logBatchMarkingStep('started', 'notify_added', notifyAddedStartedAt, {
    marking_id: element.id,
    marking_type: element.definition_type,
    is_creation: isCreation,
  });
  const notified = await notify(BUS_TOPICS[ENTITY_TYPE_MARKING_DEFINITION].ADDED_TOPIC, element, user);
  logBatchMarkingStep('completed', 'notify_added', notifyAddedStartedAt, {
    marking_id: element.id,
    marking_type: element.definition_type,
    is_creation: isCreation,
  });
  return notified;
};

export const markingDefinitionDelete = async (context, user, markingDefinitionId) => {
  return markingDefinitionDeleteAndUpdateGroups(context, user, markingDefinitionId, {});
};

export const markingDefinitionDeleteAndUpdateGroups = async (context, user, markingDefinitionId, opts) => {
  // remove the marking from the groups max shareable markings config if needed
  const groupsWithMarkingInShareableMarkings = await fullEntitiesList(context, SYSTEM_USER, [ENTITY_TYPE_GROUP], {
    filters: {
      mode: 'and',
      filters: [{ key: 'max_shareable_markings.value', values: [markingDefinitionId], operator: 'eq', mode: 'or' }],
      filterGroups: [],
    },
  });
  if (groupsWithMarkingInShareableMarkings.length > 0) {
    const markingDefinition = await findById(context, user, markingDefinitionId);
    const editShareableMarkingsPromises = [];
    groupsWithMarkingInShareableMarkings.forEach((group) => {
      const type = markingDefinition.definition_type;
      const value = (group.max_shareable_markings ?? []).filter(({ type: t, value: v }) => t !== type && v !== 'none');
      editShareableMarkingsPromises.push(groupEditField(context, user, group.id, [{ key: 'max_shareable_markings', value }]));
    });
    await Promise.all(editShareableMarkingsPromises);
  }
  // delete the marking
  const element = await deleteElementById(context, user, markingDefinitionId, ENTITY_TYPE_MARKING_DEFINITION, opts);
  // users of group impacted must be refreshed
  await notifyMembersOfNewMarking(context, user, element);
  return notify(BUS_TOPICS[ENTITY_TYPE_MARKING_DEFINITION].DELETE_TOPIC, element, user).then(() => markingDefinitionId);
};

export const markingDefinitionEditField = async (context, user, markingDefinitionId, input, opts = {}) => {
  const { element } = await updateAttribute(context, user, markingDefinitionId, ENTITY_TYPE_MARKING_DEFINITION, input, opts);
  // users of group impacted must be refreshed
  await notifyMembersOfNewMarking(context, user, element);
  return notify(BUS_TOPICS[ENTITY_TYPE_MARKING_DEFINITION].EDIT_TOPIC, element, user);
};

export const markingDefinitionCleanContext = async (context, user, markingDefinitionId) => {
  await delEditContext(user, markingDefinitionId);
  return storeLoadById(context, user, markingDefinitionId, ENTITY_TYPE_MARKING_DEFINITION).then((markingDefinition) => {
    return notify(BUS_TOPICS[ENTITY_TYPE_MARKING_DEFINITION].EDIT_TOPIC, markingDefinition, user);
  });
};

export const markingDefinitionEditContext = async (context, user, markingDefinitionId, input) => {
  await setEditContext(user, markingDefinitionId, input);
  return storeLoadById(context, user, markingDefinitionId, ENTITY_TYPE_MARKING_DEFINITION).then((markingDefinition) => {
    return notify(BUS_TOPICS[ENTITY_TYPE_MARKING_DEFINITION].EDIT_TOPIC, markingDefinition, user);
  });
};
