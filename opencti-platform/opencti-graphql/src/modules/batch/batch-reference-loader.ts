import DataLoader from 'dataloader';
import nconf from 'nconf';
import { internalFindByIds } from '../../database/middleware-loader';
import { isNotEmptyField } from '../../database/utils';
import { getInstanceIds } from '../../schema/identifier';
import type { BasicStoreObject } from '../../types/store';
import type { AuthContext, AuthUser } from '../../types/user';

const MAX_BATCH_SIZE = nconf.get('elasticsearch:batch_loader_max_size') ?? 300;

export type InputResolveRefsBatchLoader = {
  load: (id: string) => Promise<BasicStoreObject[]>;
};

type ExistingEntityIdsLookup = {
  ids: string[];
  type: string;
};

export type ExistingEntityIdsBatchLoader = {
  load: (lookup: ExistingEntityIdsLookup) => Promise<BasicStoreObject[]>;
};

export type StoreLoadByIdWithRefsLookup<TOpts extends object = Record<string, unknown>> = {
  id: string;
  opts?: TOpts;
  user: AuthUser;
};

export type StoreLoadByIdWithRefsBatchLoader<TOpts extends object = Record<string, unknown>> = {
  load: (lookup: StoreLoadByIdWithRefsLookup<TOpts>) => Promise<BasicStoreObject | null>;
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

  const dataLoader = new DataLoader<string, BasicStoreObject[]>(loadFn, {
    maxBatchSize: MAX_BATCH_SIZE,
    cache: false,
  });
  return {
    load: (id: string) => dataLoader.load(id),
  };
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

  const dataLoader = new DataLoader<ExistingEntityIdsLookup, BasicStoreObject[]>(loadFn, {
    maxBatchSize: MAX_BATCH_SIZE,
    cache: false,
  });
  return {
    load: (lookup: ExistingEntityIdsLookup) => dataLoader.load(lookup),
  };
};

export const createStoreLoadByIdWithRefsBatchLoader = <TOpts extends object>(
  context: AuthContext,
  loadByIdsWithRefs: StoreLoadByIdsWithRefs<TOpts>,
): StoreLoadByIdWithRefsBatchLoader<TOpts> => {
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

  const dataLoader = new DataLoader<StoreLoadByIdWithRefsLookup<TOpts>, BasicStoreObject | null>(loadFn, {
    maxBatchSize: MAX_BATCH_SIZE,
    cache: false,
  });
  return {
    load: (lookup: StoreLoadByIdWithRefsLookup<TOpts>) => dataLoader.load(lookup),
  };
};
