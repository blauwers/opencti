import jsonCanonicalize from 'canonicalize';
import { getBatchExecutionMetadata, isBatchWriteBoundaryOpen, setBatchExecutionMetadata } from './batch-executor';

const BATCH_ENTITY_CREATE_PROMISES_METADATA_KEY = 'batch.entity-create.promises';

type BatchEntityCreatePromiseCache = Map<string, Promise<unknown>>;

const isPlainObject = (value: object): boolean => {
  const prototype = Object.getPrototypeOf(value);
  return prototype === Object.prototype || prototype === null;
};

const isCacheableJsonValue = (value: unknown, visiting = new Set<object>()): boolean => {
  if (value === null || value === undefined || typeof value === 'string' || typeof value === 'boolean') {
    return true;
  }
  if (typeof value === 'number') {
    return Number.isFinite(value);
  }
  if (typeof value !== 'object') {
    return false;
  }
  if (visiting.has(value)) {
    return false;
  }
  if (Array.isArray(value)) {
    visiting.add(value);
    const isCacheable = value.every((item) => isCacheableJsonValue(item, visiting));
    visiting.delete(value);
    return isCacheable;
  }
  if (!isPlainObject(value)) {
    return false;
  }
  visiting.add(value);
  const isCacheable = Object.values(value).every((item) => isCacheableJsonValue(item, visiting));
  visiting.delete(value);
  return isCacheable;
};

export const buildBatchEntityCreateCacheKey = (
  type: string,
  input: Record<string, unknown>,
  opts: Record<string, unknown>,
): string | undefined => {
  if (!isCacheableJsonValue(input) || !isCacheableJsonValue(opts)) {
    return undefined;
  }
  return jsonCanonicalize({ input, opts, type }) as string;
};

export const executeBatchCoalescedEntityCreate = <T>(
  type: string,
  input: Record<string, unknown>,
  opts: Record<string, unknown>,
  execute: () => Promise<T>,
): Promise<T> => {
  if (!isBatchWriteBoundaryOpen()) {
    return execute();
  }
  const cacheKey = buildBatchEntityCreateCacheKey(type, input, opts);
  if (!cacheKey) {
    return execute();
  }
  let cache = getBatchExecutionMetadata<BatchEntityCreatePromiseCache>(BATCH_ENTITY_CREATE_PROMISES_METADATA_KEY);
  if (!cache) {
    cache = new Map<string, Promise<unknown>>();
    setBatchExecutionMetadata(BATCH_ENTITY_CREATE_PROMISES_METADATA_KEY, cache);
  }
  const existing = cache.get(cacheKey) as Promise<T> | undefined;
  if (existing) {
    return existing;
  }
  const created = execute();
  cache.set(cacheKey, created);
  const release = () => {
    if (cache?.get(cacheKey) === created) {
      cache.delete(cacheKey);
    }
  };
  void created.then(release, release);
  return created;
};
