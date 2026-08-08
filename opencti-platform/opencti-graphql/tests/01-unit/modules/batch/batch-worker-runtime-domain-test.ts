import { beforeEach, describe, expect, it, vi } from 'vitest';
import {
  buildLegacyBatchWorkerRuntimeId,
  normalizeBatchWorkerRuntimeCapability,
  readBatchWorkerFleetProtocolFloor,
  recordBatchWorkerRuntimeCapability,
  resolveRequiredBatchDeliveryProtocol,
} from '../../../../src/modules/batch/batch-worker-runtime-domain';
import { BatchDeliveryProtocol } from '../../../../src/modules/batch/batch-types';
import {
  redisGetBatchWorkerRuntimeCapabilitySnapshot,
  redisSetBatchWorkerRuntimeCapability,
} from '../../../../src/database/redis';

vi.mock('../../../../src/database/redis', () => ({
  redisGetBatchWorkerRuntimeCapabilitySnapshot: vi.fn(),
  redisSetBatchWorkerRuntimeCapability: vi.fn(),
}));

describe('batch worker runtime capability gate', () => {
  beforeEach(() => {
    vi.clearAllMocks();
  });

  it('treats missing or malformed capability evidence as a bounded v1 worker', () => {
    const missing = normalizeBatchWorkerRuntimeCapability(undefined, 'connector-user-1');
    const malformed = normalizeBatchWorkerRuntimeCapability({
      worker_id: 'worker with spaces',
      batch_delivery_protocol_max: 2,
    }, 'connector-user-1');

    expect(missing).toMatchObject({
      worker_id: buildLegacyBatchWorkerRuntimeId('connector-user-1'),
      batch_delivery_protocol_max: BatchDeliveryProtocol.V1,
    });
    expect(malformed).toMatchObject({
      worker_id: buildLegacyBatchWorkerRuntimeId('connector-user-1'),
      batch_delivery_protocol_max: BatchDeliveryProtocol.V1,
    });
  });

  it('records an explicit protocol-v2 worker capability', async () => {
    vi.mocked(redisSetBatchWorkerRuntimeCapability).mockResolvedValue(undefined as any);

    const capability = await recordBatchWorkerRuntimeCapability({
      worker_id: 'worker-1',
      batch_delivery_protocol_max: 2,
    }, 'connector-user-1');

    expect(capability).toMatchObject({
      worker_id: 'worker-1',
      batch_delivery_protocol_max: BatchDeliveryProtocol.V2,
    });
    expect(redisSetBatchWorkerRuntimeCapability).toHaveBeenCalledWith(capability);
  });

  it('computes the fleet floor from every live worker capability', async () => {
    vi.mocked(redisGetBatchWorkerRuntimeCapabilitySnapshot).mockResolvedValue({
      capabilities: [
        {
          worker_id: 'worker-v2',
          batch_delivery_protocol_max: BatchDeliveryProtocol.V2,
          observed_at: '2026-08-08T20:00:00.000Z',
        },
        {
          worker_id: 'worker-v1',
          batch_delivery_protocol_max: BatchDeliveryProtocol.V1,
          observed_at: '2026-08-08T20:00:00.000Z',
        },
      ],
      atCapacity: false,
    });

    await expect(readBatchWorkerFleetProtocolFloor()).resolves.toBe(BatchDeliveryProtocol.V1);
  });

  it('fails closed when the bounded capability window is full', async () => {
    vi.mocked(redisGetBatchWorkerRuntimeCapabilitySnapshot).mockResolvedValue({
      capabilities: [
        {
          worker_id: 'worker-v2',
          batch_delivery_protocol_max: BatchDeliveryProtocol.V2,
          observed_at: '2026-08-08T20:00:00.000Z',
        },
      ],
      atCapacity: true,
    });

    await expect(readBatchWorkerFleetProtocolFloor()).resolves.toBe(BatchDeliveryProtocol.V1);
  });

  it('keeps v2 publication disabled until the flag is on and the entire live fleet is at protocol 2', async () => {
    await expect(resolveRequiredBatchDeliveryProtocol({
      publicationEnabled: false,
      readFleetProtocolFloor: async () => BatchDeliveryProtocol.V2,
    })).resolves.toBe(BatchDeliveryProtocol.V1);
    await expect(resolveRequiredBatchDeliveryProtocol({
      publicationEnabled: true,
      readFleetProtocolFloor: async () => null,
    })).resolves.toBe(BatchDeliveryProtocol.V1);
    await expect(resolveRequiredBatchDeliveryProtocol({
      publicationEnabled: true,
      readFleetProtocolFloor: async () => BatchDeliveryProtocol.V1,
    })).resolves.toBe(BatchDeliveryProtocol.V1);
    await expect(resolveRequiredBatchDeliveryProtocol({
      publicationEnabled: true,
      readFleetProtocolFloor: async () => BatchDeliveryProtocol.V2,
    })).resolves.toBe(BatchDeliveryProtocol.V2);
  });
});
