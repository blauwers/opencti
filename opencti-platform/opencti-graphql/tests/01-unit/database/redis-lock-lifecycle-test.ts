import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';

const {
  mockAcquire,
  mockConfGet,
  mockExtend,
  mockRedlockConstructor,
  mockRedisClient,
  mockRelease,
} = vi.hoisted(() => {
  const values = new Map<string, unknown>([
    ['app:concurrency:extension_threshold', 5000],
    ['app:concurrency:max_ttl', 60000],
    ['app:concurrency:retry_count', 100],
    ['app:concurrency:retry_delay', 250],
    ['app:concurrency:retry_jitter', 100],
    ['redis:ca', []],
    ['redis:database', 0],
    ['redis:hostname', 'redis'],
    ['redis:hostnames', []],
    ['redis:mode', 'single'],
    ['redis:nat_map', []],
    ['redis:port', 6379],
  ]);
  return {
    mockAcquire: vi.fn(),
    mockConfGet: vi.fn((key: string) => values.get(key)),
    mockExtend: vi.fn(),
    mockRedlockConstructor: vi.fn(),
    mockRedisClient: {
      on: vi.fn(),
      zrange: vi.fn(),
      zremrangebyscore: vi.fn(),
    },
    mockRelease: vi.fn(),
  };
});

vi.mock('@sesamecare-oss/redlock', () => ({
  Redlock: class {
    constructor(clients: unknown, settings: unknown) {
      mockRedlockConstructor(clients, settings);
    }

    acquire(...args: unknown[]) {
      return mockAcquire(...args);
    }

    extend(...args: unknown[]) {
      return mockExtend(...args);
    }

    release(...args: unknown[]) {
      return mockRelease(...args);
    }
  },
}));

vi.mock('ioredis', () => {
  class MockRedis {
    static Cluster = MockRedis;

    constructor() {
      return mockRedisClient;
    }
  }

  return {
    Cluster: MockRedis,
    Redis: MockRedis,
  };
});

vi.mock('graphql-redis-subscriptions', () => ({
  RedisPubSub: class {},
}));

vi.mock('../../../src/config/conf', async (importOriginal) => {
  const actual = await importOriginal<typeof import('../../../src/config/conf')>();
  return {
    ...actual,
    default: { ...actual.default, get: mockConfGet },
    booleanConf: vi.fn(() => false),
    configureCA: vi.fn(() => ({})),
    DEV_MODE: false,
    getStoppingState: vi.fn(() => false),
    loadCert: vi.fn(),
    logApp: {
      debug: vi.fn(),
      error: vi.fn(),
      info: vi.fn(),
      warn: vi.fn(),
    },
    REDIS_PREFIX: '',
    TOPIC_PREFIX: '',
  };
});

vi.mock('../../../src/config/credentials', () => ({
  enrichWithRemoteCredentials: vi.fn(async (_scope: string, value: unknown) => value),
}));

vi.mock('../../../src/database/cache', () => ({
  refreshLocalCacheForEntity: vi.fn(),
}));

vi.mock('../../../src/schema/schema-relationsRef', () => ({
  schemaRelationsRefDefinition: {},
}));

import { initializeOnlyRedisLockClient, lockResource } from '../../../src/database/redis';

describe('redis lock lifecycle', () => {
  beforeEach(async () => {
    vi.useFakeTimers();
    vi.clearAllMocks();
    mockRedisClient.on.mockReturnValue(mockRedisClient);
    mockRedisClient.zrange.mockResolvedValue([]);
    mockRedisClient.zremrangebyscore.mockResolvedValue(undefined);
    await initializeOnlyRedisLockClient();
  });

  afterEach(() => {
    vi.clearAllTimers();
    vi.useRealTimers();
  });

  it('keeps batch acquisition retries out of automatic extension and release', async () => {
    const acquiredLock = { expiration: Date.now() + 60000 };
    const extendedLock = { expiration: Date.now() + 60000 };
    mockAcquire.mockResolvedValue(acquiredLock);
    mockExtend.mockResolvedValue(extendedLock);
    mockRelease.mockResolvedValue(undefined);

    const lock = await lockResource(['identity--one'], {
      retryCount: 3600,
      extensionRetryCount: 100,
      releaseRetryCount: 0,
    });

    expect(mockRedlockConstructor).toHaveBeenCalledWith([mockRedisClient], {
      retryCount: 3600,
      retryDelay: 250,
      retryJitter: 100,
    });
    expect(mockAcquire).toHaveBeenCalledWith(['{locks}:identity--one'], 60000);

    await lock.extend();
    expect(mockExtend).toHaveBeenCalledWith(acquiredLock, 60000, { retryCount: 100 });

    await lock.unlock();
    expect(mockRelease).toHaveBeenCalledWith(extendedLock, { retryCount: 0 });
  });
});
