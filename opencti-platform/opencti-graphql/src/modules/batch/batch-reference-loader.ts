import DataLoader from 'dataloader';
import nconf from 'nconf';
import { internalFindByIds } from '../../database/middleware-loader';
import { isNotEmptyField } from '../../database/utils';
import { getInstanceIds } from '../../schema/identifier';
import type { BasicStoreObject } from '../../types/store';
import type { AuthContext, AuthUser } from '../../types/user';
import { getBatchExecutionScope } from './batch-executor';

const MAX_BATCH_SIZE = nconf.get('elasticsearch:batch_loader_max_size') ?? 300;

export type InputResolveRefsBatchLoader = {
  load: (id: string) => Promise<BasicStoreObject[]>;
  invalidate: (ids: string[]) => void;
};

type ExistingEntityIdsLookup = {
  ids: string[];
  type: string;
};

export type ExistingEntityIdsBatchLoader = {
  load: (lookup: ExistingEntityIdsLookup) => Promise<BasicStoreObject[]>;
  invalidate: (ids: string[]) => void;
};

export type StoreLoadByIdWithRefsLookup<TOpts extends object = Record<string, unknown>> = {
  id: string;
  opts?: TOpts;
  user: AuthUser;
};

export type StoreLoadByIdWithRefsBatchLoader<TOpts extends object = Record<string, unknown>> = {
  load: (lookup: StoreLoadByIdWithRefsLookup<TOpts>) => Promise<BasicStoreObject | null>;
  invalidate: (ids: string[]) => void;
};

type StoreLoadByIdsWithRefs<TOpts extends object> = (
  context: AuthContext,
  user: AuthUser,
  ids: string[],
  opts?: TOpts,
) => Promise<BasicStoreObject[]>;

const buildOptionsKey = <TOpts extends object>(opts: TOpts | undefined): string => {
  if (!opts) {
    return '{}';
  }
  return JSON.stringify(Object.fromEntries(Object.entries(opts).sort(([left], [right]) => left.localeCompare(right))));
};

const normalizeTrackedIds = (ids: string[]): string[] => {
  return Array.from(new Set(ids.filter((id) => isNotEmptyField(id))));
};

const collectElementIds = (elements: BasicStoreObject[] | BasicStoreObject | null): string[] => {
  if (!elements) {
    return [];
  }
  const values = Array.isArray(elements) ? elements : [elements];
  return normalizeTrackedIds(values.flatMap((element) => getInstanceIds(element)));
};

const createTrackedLoader = <K, V>(
  loadFn: (keys: ReadonlyArray<K>) => Promise<ArrayLike<V> | ReadonlyArray<V>>,
  cacheKeyFn: (key: K) => string,
  getLookupIds: (key: K) => string[],
  getResultIds: (value: V) => string[],
) => {
  const trackedLookups = new Map<string, { ids: Set<string>; lookup: K }>();
  const lookupKeysById = new Map<string, Set<string>>();
  let executionScope = getBatchExecutionScope();
  const dataLoader = new DataLoader<K, V, string>(loadFn, {
    maxBatchSize: MAX_BATCH_SIZE,
    cacheKeyFn,
  });

  const reset = () => {
    dataLoader.clearAll();
    trackedLookups.clear();
    lookupKeysById.clear();
  };

  const resetForNewExecutionScope = () => {
    const currentScope = getBatchExecutionScope();
    if (currentScope !== executionScope) {
      reset();
      executionScope = currentScope;
    }
  };

  const trackIds = (lookup: K, ids: string[]) => {
    const cacheKey = cacheKeyFn(lookup);
    const tracked = trackedLookups.get(cacheKey) ?? { ids: new Set<string>(), lookup };
    normalizeTrackedIds(ids).forEach((id) => {
      if (tracked.ids.has(id)) {
        return;
      }
      tracked.ids.add(id);
      const lookupKeys = lookupKeysById.get(id) ?? new Set<string>();
      lookupKeys.add(cacheKey);
      lookupKeysById.set(id, lookupKeys);
    });
    trackedLookups.set(cacheKey, tracked);
  };

  const forgetLookup = (cacheKey: string) => {
    const tracked = trackedLookups.get(cacheKey);
    if (!tracked) {
      return;
    }
    tracked.ids.forEach((id) => {
      const lookupKeys = lookupKeysById.get(id);
      if (!lookupKeys) {
        return;
      }
      lookupKeys.delete(cacheKey);
      if (lookupKeys.size === 0) {
        lookupKeysById.delete(id);
      }
    });
    trackedLookups.delete(cacheKey);
  };

  const invalidate = (ids: string[]) => {
    resetForNewExecutionScope();
    const cacheKeys = new Set<string>();
    normalizeTrackedIds(ids).forEach((id) => {
      lookupKeysById.get(id)?.forEach((cacheKey) => cacheKeys.add(cacheKey));
    });
    cacheKeys.forEach((cacheKey) => {
      const tracked = trackedLookups.get(cacheKey);
      if (!tracked) {
        return;
      }
      dataLoader.clear(tracked.lookup);
      forgetLookup(cacheKey);
    });
  };

  return {
    invalidate,
    load: (lookup: K) => {
      resetForNewExecutionScope();
      trackIds(lookup, getLookupIds(lookup));
      const result = dataLoader.load(lookup);
      void result.then(
        (value) => trackIds(lookup, getResultIds(value)),
        () => {
          const cacheKey = cacheKeyFn(lookup);
          dataLoader.clear(lookup);
          forgetLookup(cacheKey);
        },
      );
      return result;
    },
  };
};

export const createInputResolveRefsBatchLoader = (
  context: AuthContext,
  user: AuthUser,
): InputResolveRefsBatchLoader => {
  const loadFn = async (ids: ReadonlyArray<string>): Promise<BasicStoreObject[][]> => {
    const expectedIds = new Set(ids.filter((id) => isNotEmptyField(id)));
    if (expectedIds.size === 0) {
      return ids.map(() => []);
    }

    const resolvedElements = await internalFindByIds<BasicStoreObject>(context, user, Array.from(expectedIds)) as BasicStoreObject[];
    const elementsById = new Map<string, BasicStoreObject[]>();
    for (let index = 0; index < resolvedElements.length; index += 1) {
      const resolvedElement = resolvedElements[index];
      const instanceIds = getInstanceIds(resolvedElement);
      for (let idIndex = 0; idIndex < instanceIds.length; idIndex += 1) {
        const instanceId = instanceIds[idIndex];
        if (!expectedIds.has(instanceId)) {
          continue;
        }
        const matchingElements = elementsById.get(instanceId);
        if (matchingElements) {
          matchingElements.push(resolvedElement);
        } else {
          elementsById.set(instanceId, [resolvedElement]);
        }
      }
    }

    return ids.map((id) => elementsById.get(id) ?? []);
  };

  return createTrackedLoader(
    loadFn,
    (id) => id,
    (id) => [id],
    (elements) => collectElementIds(elements),
  );
};

export const createExistingEntityIdsBatchLoader = (
  context: AuthContext,
  user: AuthUser,
): ExistingEntityIdsBatchLoader => {
  const loadFn = async (lookups: ReadonlyArray<ExistingEntityIdsLookup>): Promise<BasicStoreObject[][]> => {
    const idsByType = new Map<string, Set<string>>();
    for (let index = 0; index < lookups.length; index += 1) {
      const lookup = lookups[index];
      const typeIds = idsByType.get(lookup.type) ?? new Set<string>();
      lookup.ids.filter((id) => isNotEmptyField(id)).forEach((id) => typeIds.add(id));
      idsByType.set(lookup.type, typeIds);
    }

    const resolvedByType = new Map<string, BasicStoreObject[]>();
    await Promise.all(Array.from(idsByType.entries()).map(async ([type, ids]) => {
      const resolvedElements = ids.size === 0
        ? []
        : await internalFindByIds<BasicStoreObject>(context, user, Array.from(ids), { type }) as BasicStoreObject[];
      resolvedByType.set(type, resolvedElements);
    }));

    return lookups.map((lookup) => {
      const expectedIds = new Set(lookup.ids.filter((id) => isNotEmptyField(id)));
      if (expectedIds.size === 0) {
        return [];
      }
      return (resolvedByType.get(lookup.type) ?? []).filter((element) => {
        return getInstanceIds(element).some((id) => expectedIds.has(id));
      });
    });
  };

  return createTrackedLoader(
    loadFn,
    (lookup) => `${lookup.type}:${normalizeTrackedIds(lookup.ids).sort().join('|')}`,
    (lookup) => lookup.ids,
    (elements) => collectElementIds(elements),
  );
};

export const createStoreLoadByIdWithRefsBatchLoader = <TOpts extends object>(
  context: AuthContext,
  loadByIdsWithRefs: StoreLoadByIdsWithRefs<TOpts>,
): StoreLoadByIdWithRefsBatchLoader<TOpts> => {
  const userCacheKeys = new Map<AuthUser, number>();
  let nextUserCacheKey = 0;
  const getUserCacheKey = (user: AuthUser) => {
    const existingKey = userCacheKeys.get(user);
    if (existingKey !== undefined) {
      return existingKey;
    }
    const cacheKey = nextUserCacheKey;
    nextUserCacheKey += 1;
    userCacheKeys.set(user, cacheKey);
    return cacheKey;
  };
  const loadFn = async (lookups: ReadonlyArray<StoreLoadByIdWithRefsLookup<TOpts>>): Promise<(BasicStoreObject | null)[]> => {
    const groupsByUser = new Map<AuthUser, Map<string, {
      ids: Set<string>;
      opts?: TOpts;
      resolvedElements?: BasicStoreObject[];
    }>>();
    for (let index = 0; index < lookups.length; index += 1) {
      const lookup = lookups[index];
      const optionsKey = buildOptionsKey(lookup.opts);
      const groupsByOptions = groupsByUser.get(lookup.user) ?? new Map();
      const group = groupsByOptions.get(optionsKey) ?? { ids: new Set<string>(), opts: lookup.opts };
      if (isNotEmptyField(lookup.id)) {
        group.ids.add(lookup.id);
      }
      groupsByOptions.set(optionsKey, group);
      groupsByUser.set(lookup.user, groupsByOptions);
    }

    await Promise.all(Array.from(groupsByUser.entries()).flatMap(([user, groupsByOptions]) => {
      return Array.from(groupsByOptions.values()).map(async (group) => {
        group.resolvedElements = group.ids.size === 0
          ? []
          : await loadByIdsWithRefs(context, user, Array.from(group.ids), group.opts);
      });
    }));

    return lookups.map((lookup) => {
      const resolvedElements = groupsByUser.get(lookup.user)?.get(buildOptionsKey(lookup.opts))?.resolvedElements ?? [];
      return resolvedElements.find((element) => getInstanceIds(element).includes(lookup.id)) ?? null;
    });
  };

  return createTrackedLoader(
    loadFn,
    (lookup) => `${getUserCacheKey(lookup.user)}:${buildOptionsKey(lookup.opts)}:${lookup.id}`,
    (lookup) => [lookup.id],
    (element) => collectElementIds(element),
  );
};
