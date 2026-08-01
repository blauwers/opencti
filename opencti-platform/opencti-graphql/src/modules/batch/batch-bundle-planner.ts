import { STIX_EXT_OCTI } from '../../types/stix-2-1-extensions';

export interface BatchBundlePlanObject {
  dependencyIds: string[];
  executionPhase: number;
  id: string;
  index: number;
  type: string;
}

export interface BatchBundlePlanPhase {
  objectIds: string[];
  phase: number;
}

export interface BatchBundlePlan {
  executionPhases: BatchBundlePlanPhase[];
  ignoredObjectCount: number;
  incompatibleObjectIds: string[];
  objectCount: number;
  objects: BatchBundlePlanObject[];
  plannedObjectCount: number;
}

type BatchBundlePlannerOptions = {
  cleanupInconsistentBundle?: boolean;
};

type IndexedBundleObject = {
  id: string;
  index: number;
  object: Record<string, any>;
  type: string;
};

const getObjectInternalIds = (object: Record<string, any>): string[] => {
  const internalIds: string[] = [];
  if (typeof object.x_opencti_id === 'string' && object.x_opencti_id.length > 0) {
    internalIds.push(object.x_opencti_id);
  }
  const extensionId = object.extensions?.[STIX_EXT_OCTI]?.id;
  if (typeof extensionId === 'string' && extensionId.length > 0) {
    internalIds.push(extensionId);
  }
  return internalIds;
};

const isReferenceKey = (key: string): boolean => key.endsWith('_ref') || key.endsWith('_refs');

const toReferenceIds = (key: string, value: unknown): string[] => {
  if (!isReferenceKey(key) || value === null || value === undefined) {
    return [];
  }
  if (key.endsWith('_refs')) {
    return Array.isArray(value)
      ? value.filter((reference): reference is string => typeof reference === 'string' && reference.length > 0)
      : [];
  }
  return typeof value === 'string' && value.length > 0 ? [value] : [];
};

const indexBundleObjects = (objects: Record<string, any>[]) => {
  const canonicalObjects = new Map<string, IndexedBundleObject>();
  const aliases = new Map<string, string>();
  const firstIndexes = new Map<string, number>();

  objects.forEach((object, index) => {
    if (!object || typeof object !== 'object' || typeof object.id !== 'string' || object.id.length === 0) {
      throw new Error(`Invalid batch bundle object at index ${index}`);
    }
    if (typeof object.type !== 'string' || object.type.length === 0) {
      throw new Error(`Invalid batch bundle object type at index ${index}`);
    }
    if (!firstIndexes.has(object.id)) {
      firstIndexes.set(object.id, index);
    }
    canonicalObjects.set(object.id, {
      id: object.id,
      index: firstIndexes.get(object.id) as number,
      object,
      type: object.type,
    });
    aliases.set(object.id, object.id);
    getObjectInternalIds(object).forEach((internalId) => aliases.set(internalId, object.id));
  });

  const ignoredObjectCount = objects.length - canonicalObjects.size;
  return { aliases, canonicalObjects, ignoredObjectCount };
};

const isMissingReference = (referenceId: string, aliases: Map<string, string>): boolean => {
  return !aliases.has(referenceId);
};

const isCompatibleObject = (
  object: IndexedBundleObject,
  aliases: Map<string, string>,
  cleanupInconsistentBundle: boolean,
): boolean => {
  if (!cleanupInconsistentBundle) {
    return true;
  }
  if (object.type === 'relationship') {
    return typeof object.object.source_ref === 'string'
      && typeof object.object.target_ref === 'string'
      && !isMissingReference(object.object.source_ref, aliases)
      && !isMissingReference(object.object.target_ref, aliases);
  }
  if (object.type === 'sighting') {
    return typeof object.object.sighting_of_ref === 'string'
      && !isMissingReference(object.object.sighting_of_ref, aliases)
      && Array.isArray(object.object.where_sighted_refs)
      && object.object.where_sighted_refs.some((referenceId: unknown) => (
        typeof referenceId === 'string' && !isMissingReference(referenceId, aliases)
      ));
  }
  return true;
};

const collectDependencyIds = (
  object: IndexedBundleObject,
  aliases: Map<string, string>,
  compatibleObjectIds: Set<string>,
  cleanupInconsistentBundle: boolean,
): string[] => {
  const dependencyIds: string[] = [];
  Object.entries(object.object).forEach(([key, value]) => {
    toReferenceIds(key, value).forEach((referenceId) => {
      if (cleanupInconsistentBundle && isMissingReference(referenceId, aliases)) {
        return;
      }
      const canonicalId = aliases.get(referenceId);
      if (!canonicalId || canonicalId === object.id || !compatibleObjectIds.has(canonicalId)) {
        return;
      }
      if (!dependencyIds.includes(canonicalId)) {
        dependencyIds.push(canonicalId);
      }
    });
  });
  return dependencyIds;
};

const assignExecutionPhases = (objects: BatchBundlePlanObject[]): BatchBundlePlanObject[] => {
  const objectsById = new Map(objects.map((object) => [object.id, object]));
  const phases = new Map<string, number>();
  const visiting = new Set<string>();

  const resolvePhase = (objectId: string): number => {
    const knownPhase = phases.get(objectId);
    if (knownPhase !== undefined) {
      return knownPhase;
    }
    const object = objectsById.get(objectId);
    if (!object) {
      return 0;
    }
    visiting.add(objectId);
    let phase = 0;
    object.dependencyIds.forEach((dependencyId) => {
      if (visiting.has(dependencyId)) {
        return;
      }
      phase = Math.max(phase, resolvePhase(dependencyId) + 1);
    });
    visiting.delete(objectId);
    phases.set(objectId, phase);
    return phase;
  };

  return objects.map((object) => ({
    ...object,
    executionPhase: resolvePhase(object.id),
  }));
};

const buildExecutionPhases = (objects: BatchBundlePlanObject[]): BatchBundlePlanPhase[] => {
  const phases = new Map<number, string[]>();
  objects.forEach((object) => {
    const objectIds = phases.get(object.executionPhase) ?? [];
    objectIds.push(object.id);
    phases.set(object.executionPhase, objectIds);
  });
  return Array.from(phases.entries())
    .sort(([left], [right]) => left - right)
    .map(([phase, objectIds]) => ({ phase, objectIds }));
};

export const planStixBundleObjects = (
  objects: Record<string, any>[],
  options: BatchBundlePlannerOptions = {},
): BatchBundlePlan => {
  const cleanupInconsistentBundle = options.cleanupInconsistentBundle === true;
  const { aliases, canonicalObjects, ignoredObjectCount } = indexBundleObjects(objects);
  const indexedObjects = Array.from(canonicalObjects.values()).sort((left, right) => left.index - right.index);
  const compatibleObjects = indexedObjects.filter((object) => isCompatibleObject(object, aliases, cleanupInconsistentBundle));
  const compatibleObjectIds = new Set(compatibleObjects.map((object) => object.id));
  const plannedObjects = assignExecutionPhases(compatibleObjects.map((object) => ({
    dependencyIds: collectDependencyIds(object, aliases, compatibleObjectIds, cleanupInconsistentBundle),
    executionPhase: 0,
    id: object.id,
    index: object.index,
    type: object.type,
  })));

  return {
    executionPhases: buildExecutionPhases(plannedObjects),
    ignoredObjectCount,
    incompatibleObjectIds: indexedObjects
      .filter((object) => !compatibleObjectIds.has(object.id))
      .map((object) => object.id),
    objectCount: objects.length,
    objects: plannedObjects,
    plannedObjectCount: plannedObjects.length,
  };
};
