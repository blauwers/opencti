import { STIX_EXT_OCTI } from '../../types/stix-2-1-extensions';
import { isSupportedStixType } from '../../schema/identifier';

export interface BatchBundleObjectNormalization {
  externalReferenceIndexes?: number[];
  killChainPhaseIndexes?: number[];
  referenceValues?: Record<string, string | string[] | null>;
}

export interface BatchBundlePlanObject {
  dependencyIds: string[];
  executionPhase: number;
  id: string;
  index: number;
  normalization?: BatchBundleObjectNormalization;
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
  orderedObjectIds: string[];
  plannedObjectCount: number;
}

type BatchBundlePlannerOptions = {
  cleanupInconsistentBundle?: boolean;
};

type IndexedBundleObject = {
  id: string;
  index: number;
  object: Record<string, any>;
  originalObject: Record<string, any>;
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

const isReferenceValue = (value: unknown): value is string => typeof value === 'string' && value.length > 0;

const isSupportedStixId = (id: string): boolean => {
  if (!id.includes('--')) {
    return true;
  }
  const [rawType] = id.split('--');
  if (!rawType.startsWith('x-mitre-') && !rawType.startsWith('x-opencti-')) {
    return true;
  }
  const type = rawType.replace(/^x-mitre-/, '').replace(/^x-opencti-/, '');
  return isSupportedStixType(type);
};

const toReferenceIds = (key: string, value: unknown): string[] => {
  if (!isReferenceKey(key) || value === null || value === undefined) {
    return [];
  }
  if (key.endsWith('_refs')) {
    return Array.isArray(value) ? value.filter(isReferenceValue) : [];
  }
  return isReferenceValue(value) ? [value] : [];
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
      object: structuredClone(object),
      originalObject: object,
      type: object.type,
    });
    aliases.set(object.id, object.id);
    getObjectInternalIds(object).forEach((internalId) => aliases.set(internalId, object.id));
  });

  const ignoredObjectCount = objects.length - canonicalObjects.size;
  return { aliases, canonicalObjects, ignoredObjectCount };
};

const buildEmbeddedReferenceKey = (value: Record<string, any>, kind: 'external_references' | 'kill_chain_phases'): string | null => {
  if (kind === 'external_references') {
    const hasIdentityValue = (fieldValue: unknown): boolean => (
      fieldValue !== undefined
      && fieldValue !== null
      && (typeof fieldValue !== 'string' || fieldValue.length > 0)
    );
    if (hasIdentityValue(value.url)) {
      return JSON.stringify({ url: value.url });
    }
    if (hasIdentityValue(value.source_name) && hasIdentityValue(value.external_id)) {
      return JSON.stringify({ source_name: value.source_name, external_id: value.external_id });
    }
    return null;
  }
  return JSON.stringify({ phase_name: value.phase_name, kill_chain_name: value.kill_chain_name });
};

const deduplicateEmbeddedValues = (
  values: unknown,
  kind: 'external_references' | 'kill_chain_phases',
): { indexes?: number[]; values: unknown } => {
  if (!Array.isArray(values)) {
    return { values };
  }
  const retainedIndexes: number[] = [];
  const retainedValues: unknown[] = [];
  const keys = new Set<string>();
  values.forEach((value, index) => {
    if (!value || typeof value !== 'object') {
      return;
    }
    const key = buildEmbeddedReferenceKey(value as Record<string, any>, kind);
    if (key !== null && !keys.has(key)) {
      keys.add(key);
      retainedIndexes.push(index);
      retainedValues.push(value);
    }
  });
  if (retainedIndexes.length === values.length) {
    return { values };
  }
  return { indexes: retainedIndexes, values: retainedValues };
};

const valuesDiffer = (left: unknown, right: unknown): boolean => JSON.stringify(left) !== JSON.stringify(right);

const buildObjectNormalization = (object: IndexedBundleObject): BatchBundleObjectNormalization | undefined => {
  const referenceValues: Record<string, string | string[] | null> = {};
  Object.entries(object.object).forEach(([key, value]) => {
    if (isReferenceKey(key) && valuesDiffer(value, object.originalObject[key])) {
      referenceValues[key] = value as string | string[] | null;
    }
  });

  const externalReferences = deduplicateEmbeddedValues(object.originalObject.external_references, 'external_references');
  const killChainPhases = deduplicateEmbeddedValues(object.originalObject.kill_chain_phases, 'kill_chain_phases');
  const normalization: BatchBundleObjectNormalization = {};
  if (Object.keys(referenceValues).length > 0) {
    normalization.referenceValues = referenceValues;
  }
  if (externalReferences.indexes) {
    normalization.externalReferenceIndexes = externalReferences.indexes;
  }
  if (killChainPhases.indexes) {
    normalization.killChainPhaseIndexes = killChainPhases.indexes;
  }
  return Object.keys(normalization).length > 0 ? normalization : undefined;
};

const normalizeBundleObjects = (
  indexedObjects: IndexedBundleObject[],
  aliases: Map<string, string>,
  canonicalObjects: Map<string, IndexedBundleObject>,
  cleanupInconsistentBundle: boolean,
): Set<string> => {
  const enlistedObjectIds = new Set<string>();
  const incompatibleObjectIds = new Set<string>();
  const referenceGraph = new Map<string, Set<string>>();

  const enlistObject = (lookupId: string, parentIds: Set<string>): number => {
    const canonicalId = aliases.get(lookupId);
    if (!canonicalId) {
      return 0;
    }
    const object = canonicalObjects.get(canonicalId);
    if (!object) {
      return 0;
    }
    if (enlistedObjectIds.has(canonicalId) || incompatibleObjectIds.has(canonicalId)) {
      return 1;
    }

    const objectRefs = referenceGraph.get(canonicalId) ?? new Set<string>();
    referenceGraph.set(canonicalId, objectRefs);
    let dependencyCount = 1;

    Object.entries(object.object).forEach(([key, value]) => {
      if (key.endsWith('_refs') && value !== null && value !== undefined) {
        const retainedRefs: string[] = [];
        const retainedRefIds = new Set<string>();
        const refs = Array.isArray(value) ? value : [];
        refs.filter(isReferenceValue).forEach((referenceId) => {
          const referenceCanonicalId = aliases.get(referenceId);
          const isMissingReference = referenceCanonicalId === undefined;
          const isUnsupportedReference = !isSupportedStixId(referenceId);
          const referencedObjectRefs = referenceCanonicalId ? referenceGraph.get(referenceCanonicalId) : undefined;
          const isDependencyBackEdge = referencedObjectRefs?.has(canonicalId) === true;
          const shouldRetain = !isUnsupportedReference
            && !(cleanupInconsistentBundle && isMissingReference)
            && referenceCanonicalId !== canonicalId
            && !parentIds.has(referenceCanonicalId ?? referenceId)
            && !isDependencyBackEdge;
          if (!shouldRetain) {
            return;
          }
          if (referenceCanonicalId) {
            objectRefs.add(referenceCanonicalId);
            parentIds.add(referenceCanonicalId);
            dependencyCount += enlistObject(referenceCanonicalId, parentIds);
            parentIds.delete(referenceCanonicalId);
          }
          if (!retainedRefIds.has(referenceId)) {
            retainedRefIds.add(referenceId);
            retainedRefs.push(referenceId);
          }
        });
        object.object[key] = retainedRefs;
      } else if (key.endsWith('_ref')) {
        const referenceId = isReferenceValue(value) ? value : undefined;
        const referenceCanonicalId = referenceId ? aliases.get(referenceId) : undefined;
        const isMissingReference = referenceId !== undefined && referenceCanonicalId === undefined;
        const isUnsupportedReference = referenceId !== undefined && !isSupportedStixId(referenceId);
        const referencedObjectRefs = referenceCanonicalId ? referenceGraph.get(referenceCanonicalId) : undefined;
        const isDependencyBackEdge = referencedObjectRefs?.has(canonicalId) === true;
        const shouldRetain = referenceId !== undefined
          && !isUnsupportedReference
          && !(cleanupInconsistentBundle && isMissingReference)
          && referenceCanonicalId !== canonicalId
          && !parentIds.has(referenceCanonicalId ?? referenceId)
          && !isDependencyBackEdge;
        if (shouldRetain && referenceId) {
          if (referenceCanonicalId) {
            objectRefs.add(referenceCanonicalId);
            parentIds.add(referenceCanonicalId);
            dependencyCount += enlistObject(referenceCanonicalId, parentIds);
            parentIds.delete(referenceCanonicalId);
          }
        } else {
          object.object[key] = null;
        }
      } else if (key === 'external_references') {
        object.object[key] = deduplicateEmbeddedValues(value, key).values;
      } else if (key === 'kill_chain_phases') {
        object.object[key] = deduplicateEmbeddedValues(value, key).values;
      }
    });

    const isCompatible = isSupportedStixId(object.id) && (object.type === 'relationship'
      ? isReferenceValue(object.object.source_ref) && isReferenceValue(object.object.target_ref)
      : object.type === 'sighting'
        ? isReferenceValue(object.object.sighting_of_ref) && Array.isArray(object.object.where_sighted_refs) && object.object.where_sighted_refs.length > 0
        : true);
    if (isCompatible) {
      enlistedObjectIds.add(canonicalId);
    } else {
      incompatibleObjectIds.add(canonicalId);
    }
    return dependencyCount;
  };

  indexedObjects.forEach((object) => enlistObject(object.id, new Set<string>()));
  return incompatibleObjectIds;
};

const collectDependencyIds = (
  object: IndexedBundleObject,
  aliases: Map<string, string>,
  compatibleObjectIds: Set<string>,
): string[] => {
  const dependencyIds: string[] = [];
  const dependencyIdSet = new Set<string>();
  Object.entries(object.object).forEach(([key, value]) => {
    toReferenceIds(key, value).forEach((referenceId) => {
      const canonicalId = aliases.get(referenceId);
      if (!canonicalId || canonicalId === object.id || !compatibleObjectIds.has(canonicalId)) {
        return;
      }
      if (!dependencyIdSet.has(canonicalId)) {
        dependencyIdSet.add(canonicalId);
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
  const incompatibleObjectIds = normalizeBundleObjects(indexedObjects, aliases, canonicalObjects, cleanupInconsistentBundle);
  const compatibleObjects = indexedObjects.filter((object) => !incompatibleObjectIds.has(object.id));
  const compatibleObjectIds = new Set(compatibleObjects.map((object) => object.id));
  const plannedObjects = assignExecutionPhases(compatibleObjects.map((object) => ({
    dependencyIds: collectDependencyIds(object, aliases, compatibleObjectIds),
    executionPhase: 0,
    id: object.id,
    index: object.index,
    normalization: buildObjectNormalization(object),
    type: object.type,
  })));
  const executionPhases = buildExecutionPhases(plannedObjects);

  return {
    executionPhases,
    ignoredObjectCount,
    incompatibleObjectIds: indexedObjects
      .filter((object) => incompatibleObjectIds.has(object.id))
      .map((object) => object.id),
    objectCount: objects.length,
    objects: plannedObjects,
    orderedObjectIds: executionPhases.flatMap((phase) => phase.objectIds),
    plannedObjectCount: plannedObjects.length,
  };
};
