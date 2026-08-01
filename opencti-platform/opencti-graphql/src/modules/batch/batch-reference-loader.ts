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
