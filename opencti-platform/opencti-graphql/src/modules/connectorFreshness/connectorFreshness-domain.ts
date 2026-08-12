import { ForbiddenAccess, ValidationError } from '../../config/errors';
import {
  CONNECTOR_FRESHNESS_STATUS,
  redisAcquireConnectorFreshnessBatch,
  redisCompleteConnectorFreshness,
  redisReleaseConnectorFreshness,
  type ConnectorFreshnessDecision as RedisConnectorFreshnessDecision,
  type ConnectorFreshnessStatus as RedisConnectorFreshnessStatus,
} from '../../database/redis';
import { storeLoadById } from '../../database/middleware-loader';
import { ENTITY_TYPE_CONNECTOR } from '../../schema/internalObject';
import type { BasicStoreEntityConnector } from '../../types/connector';
import type { AuthContext, AuthUser } from '../../types/user';
import {
  type ConnectorFreshnessAcquireInput,
  type ConnectorFreshnessCompleteInput,
  type ConnectorFreshnessDecision,
  type ConnectorFreshnessReleaseInput,
  ConnectorFreshnessStatus,
} from '../../generated/graphql';

const MAX_FRESHNESS_KEYS = 100;
const MAX_FRESHNESS_KEY_LENGTH = 1024;
const MAX_NAMESPACE_LENGTH = 64;
const MIN_FRESHNESS_TTL_SECONDS = 1;
const MAX_FRESHNESS_TTL_SECONDS = 31 * 24 * 60 * 60;
const MIN_LEASE_TTL_SECONDS = 1;
const MAX_LEASE_TTL_SECONDS = 5 * 60;
const NAMESPACE_PATTERN = /^[a-z0-9][a-z0-9:_-]*$/i;

const validateNamespace = (namespace: string) => {
  if (!namespace || namespace.length > MAX_NAMESPACE_LENGTH || !NAMESPACE_PATTERN.test(namespace)) {
    throw ValidationError(
      `Freshness namespace must match ${NAMESPACE_PATTERN} and be at most ${MAX_NAMESPACE_LENGTH} characters`,
      'namespace',
    );
  }
};

const validateKeys = (keys: string[]) => {
  if (!Array.isArray(keys) || keys.length === 0 || keys.length > MAX_FRESHNESS_KEYS) {
    throw ValidationError(`Freshness keys must contain between 1 and ${MAX_FRESHNESS_KEYS} values`, 'keys');
  }
  if (new Set(keys).size !== keys.length) {
    throw ValidationError('Freshness keys must not contain duplicates', 'keys');
  }
  keys.forEach((key) => {
    if (!key || key.length > MAX_FRESHNESS_KEY_LENGTH) {
      throw ValidationError(`Freshness keys must be non-empty and at most ${MAX_FRESHNESS_KEY_LENGTH} characters`, 'keys');
    }
  });
};

const validateTtl = (field: string, value: number, min: number, max: number) => {
  if (!Number.isInteger(value) || value < min || value > max) {
    throw ValidationError(`${field} must be an integer between ${min} and ${max} seconds`, field);
  }
};

const ensureConnectorOwnership = async (context: AuthContext, user: AuthUser, connectorId: string) => {
  const connector = await storeLoadById<BasicStoreEntityConnector>(context, user, connectorId, ENTITY_TYPE_CONNECTOR);
  if (!connector || (connector.connector_user_id !== user.id && connector.connector_user_id !== user.internal_id)) {
    throw ForbiddenAccess('You are not allowed to manage freshness for this connector');
  }
};

const mapConnectorFreshnessStatus = (status: RedisConnectorFreshnessStatus): ConnectorFreshnessStatus => {
  switch (status) {
    case CONNECTOR_FRESHNESS_STATUS.Fresh:
      return ConnectorFreshnessStatus.Fresh;
    case CONNECTOR_FRESHNESS_STATUS.Acquired:
      return ConnectorFreshnessStatus.Acquired;
    case CONNECTOR_FRESHNESS_STATUS.InFlight:
      return ConnectorFreshnessStatus.InFlight;
    default:
      throw ValidationError('Unexpected connector freshness status', 'status');
  }
};

const mapConnectorFreshnessDecision = (decision: RedisConnectorFreshnessDecision): ConnectorFreshnessDecision => ({
  key: decision.key,
  status: mapConnectorFreshnessStatus(decision.status),
  lease_token: decision.leaseToken,
  retry_after_ms: decision.retryAfterMs,
});

export const acquireConnectorFreshness = async (
  context: AuthContext,
  user: AuthUser,
  input: ConnectorFreshnessAcquireInput,
): Promise<ConnectorFreshnessDecision[]> => {
  validateNamespace(input.namespace);
  validateKeys(input.keys);
  validateTtl('lease_ttl_seconds', input.lease_ttl_seconds, MIN_LEASE_TTL_SECONDS, MAX_LEASE_TTL_SECONDS);
  await ensureConnectorOwnership(context, user, input.connector_id);
  const decisions = await redisAcquireConnectorFreshnessBatch(
    input.connector_id,
    input.namespace,
    input.keys,
    input.lease_ttl_seconds,
    input.force_refresh ?? false,
  );
  return decisions.map(mapConnectorFreshnessDecision);
};

export const completeConnectorFreshness = async (
  context: AuthContext,
  user: AuthUser,
  input: ConnectorFreshnessCompleteInput,
): Promise<boolean> => {
  validateNamespace(input.namespace);
  validateKeys([input.key]);
  validateTtl('freshness_ttl_seconds', input.freshness_ttl_seconds, MIN_FRESHNESS_TTL_SECONDS, MAX_FRESHNESS_TTL_SECONDS);
  await ensureConnectorOwnership(context, user, input.connector_id);
  return redisCompleteConnectorFreshness(
    input.connector_id,
    input.namespace,
    input.key,
    input.lease_token,
    input.freshness_ttl_seconds,
  );
};

export const releaseConnectorFreshness = async (
  context: AuthContext,
  user: AuthUser,
  input: ConnectorFreshnessReleaseInput,
): Promise<boolean> => {
  validateNamespace(input.namespace);
  validateKeys([input.key]);
  await ensureConnectorOwnership(context, user, input.connector_id);
  return redisReleaseConnectorFreshness(
    input.connector_id,
    input.namespace,
    input.key,
    input.lease_token,
  );
};
