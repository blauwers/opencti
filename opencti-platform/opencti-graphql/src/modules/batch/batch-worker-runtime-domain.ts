import { booleanConf, logApp } from '../../config/conf';
import { redisGetBatchWorkerRuntimeCapabilitySnapshot, redisSetBatchWorkerRuntimeCapability } from '../../database/redis';
import { now } from '../../utils/format';
import { hashSHA256 } from '../../utils/hash';
import { BatchDeliveryProtocol, type BatchWorkerRuntimeCapability, type BatchWorkerRuntimeCapabilityInput } from './batch-types';

const BATCH_DELIVERY_PROTOCOL_V2_CONFIG_KEY = 'app:batch_delivery_protocol_v2_enabled';
const BATCH_WORKER_RUNTIME_ID_MAX_LENGTH = 128;
const BATCH_WORKER_RUNTIME_ID_PATTERN = /^[A-Za-z0-9._:-]+$/;
const LEGACY_WORKER_RUNTIME_PREFIX = 'legacy-worker--';

export const buildLegacyBatchWorkerRuntimeId = (actorId: string): string => {
  return `${LEGACY_WORKER_RUNTIME_PREFIX}${hashSHA256(actorId)}`;
};

export const normalizeBatchWorkerRuntimeCapability = (
  input: BatchWorkerRuntimeCapabilityInput | null | undefined,
  actorId: string,
): BatchWorkerRuntimeCapability => {
  const workerId = input?.worker_id;
  const protocolMax = input?.batch_delivery_protocol_max;
  if (
    typeof workerId === 'string'
    && workerId.length > 0
    && workerId.length <= BATCH_WORKER_RUNTIME_ID_MAX_LENGTH
    && BATCH_WORKER_RUNTIME_ID_PATTERN.test(workerId)
    && protocolMax === BatchDeliveryProtocol.V2
  ) {
    return {
      worker_id: workerId,
      batch_delivery_protocol_max: BatchDeliveryProtocol.V2,
      observed_at: now(),
    };
  }
  return {
    worker_id: buildLegacyBatchWorkerRuntimeId(actorId),
    batch_delivery_protocol_max: BatchDeliveryProtocol.V1,
    observed_at: now(),
  };
};

export const recordBatchWorkerRuntimeCapability = async (
  input: BatchWorkerRuntimeCapabilityInput | null | undefined,
  actorId: string,
): Promise<BatchWorkerRuntimeCapability> => {
  const capability = normalizeBatchWorkerRuntimeCapability(input, actorId);
  await redisSetBatchWorkerRuntimeCapability(capability);
  return capability;
};

export const readBatchWorkerFleetProtocolFloor = async (): Promise<BatchDeliveryProtocol | null> => {
  const { capabilities, atCapacity } = await redisGetBatchWorkerRuntimeCapabilitySnapshot();
  if (atCapacity) {
    return BatchDeliveryProtocol.V1;
  }
  if (capabilities.length === 0) {
    return null;
  }
  return capabilities.reduce<BatchDeliveryProtocol>(
    (floor, capability) => Math.min(floor, capability.batch_delivery_protocol_max) as BatchDeliveryProtocol,
    BatchDeliveryProtocol.V2,
  );
};

export const isBatchDeliveryProtocolV2PublicationEnabled = (): boolean => {
  return booleanConf(BATCH_DELIVERY_PROTOCOL_V2_CONFIG_KEY, false);
};

export const resolveRequiredBatchDeliveryProtocol = async (options: {
  publicationEnabled?: boolean;
  readFleetProtocolFloor?: () => Promise<BatchDeliveryProtocol | null>;
} = {}): Promise<BatchDeliveryProtocol> => {
  const publicationEnabled = options.publicationEnabled ?? isBatchDeliveryProtocolV2PublicationEnabled();
  if (!publicationEnabled) {
    return BatchDeliveryProtocol.V1;
  }
  try {
    const readFleetProtocolFloor = options.readFleetProtocolFloor ?? readBatchWorkerFleetProtocolFloor;
    const fleetProtocolFloor = await readFleetProtocolFloor();
    return fleetProtocolFloor !== null && fleetProtocolFloor >= BatchDeliveryProtocol.V2
      ? BatchDeliveryProtocol.V2
      : BatchDeliveryProtocol.V1;
  } catch (error) {
    logApp.warn('[BATCH] Unable to read worker delivery protocol floor; keeping publication on v1', { cause: error });
    return BatchDeliveryProtocol.V1;
  }
};
