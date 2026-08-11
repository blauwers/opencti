import nconf from 'nconf';

/*
 * Extension of conf.js to start using TypeScript.
 */

const DEFAULT_REQUEST_TIMEOUT_MS = 20 * 60 * 1000;
const DEFAULT_BATCH_REQUEST_TIMEOUT_MS = 60 * 60 * 1000;

const positiveNumberOr = (value: unknown, fallback: number): number => {
  const parsed = Number(value);
  return Number.isFinite(parsed) && parsed > 0 ? parsed : fallback;
};

export const resolveBatchRequestTimeout = (
  requestTimeout: unknown,
  batchRequestTimeout: unknown,
): number => {
  return Math.max(
    positiveNumberOr(requestTimeout, DEFAULT_REQUEST_TIMEOUT_MS),
    positiveNumberOr(batchRequestTimeout, DEFAULT_BATCH_REQUEST_TIMEOUT_MS),
  );
};

export const stringArrayConf = (key: string) => {
  const configValue = nconf.get(key);
  if (!Array.isArray(configValue)) {
    return [];
  }
  return configValue.filter((entry): entry is string => typeof entry === 'string');
};
