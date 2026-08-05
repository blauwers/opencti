import { beforeEach, describe, expect, it, vi } from 'vitest';

const {
  mockAssertExchange,
  mockAssertQueue,
  mockBindQueue,
  mockDeleteQueue,
  mockConnect,
} = vi.hoisted(() => {
  const assertExchange = vi.fn((_exchange: string, _type: string, _options: unknown, callback: (error: null, result: unknown) => void) => {
    callback(null, {});
  });
  const assertQueue = vi.fn((_queue: string, _options: unknown, callback: (error: null, result: unknown) => void) => {
    callback(null, {});
  });
  const bindQueue = vi.fn((_queue: string, _exchange: string, _routing: string, _options: unknown, callback: (error: null, result: unknown) => void) => {
    callback(null, {});
  });
  const deleteQueue = vi.fn((_queue: string, _options: unknown, callback: (error: null, result: unknown) => void) => {
    callback(null, {});
  });
  const connect = vi.fn((_uri: string, _options: unknown, callback: (error: null, connection: unknown) => void) => {
    callback(null, {
      on: vi.fn(),
      close: vi.fn(),
      createConfirmChannel: (channelCallback: (error: null, channel: unknown) => void) => {
        channelCallback(null, {
          on: vi.fn(),
          close: vi.fn(),
          assertExchange,
          assertQueue,
          bindQueue,
          deleteQueue,
        });
      },
    });
  });
  return {
    mockAssertExchange: assertExchange,
    mockAssertQueue: assertQueue,
    mockBindQueue: bindQueue,
    mockDeleteQueue: deleteQueue,
    mockConnect: connect,
  };
});

vi.mock('amqplib/callback_api', () => ({
  default: {
    connect: mockConnect,
    credentials: { plain: vi.fn(() => ({})) },
  },
}));

vi.mock('../../../src/config/conf', () => ({
  default: { get: vi.fn(() => undefined) },
  booleanConf: vi.fn(() => false),
  configureCA: vi.fn(() => ({})),
  loadCert: vi.fn(),
  logApp: {
    info: vi.fn(),
    error: vi.fn(),
    warn: vi.fn(),
    debug: vi.fn(),
  },
}));

vi.mock('../../../src/config/tracing', () => ({
  telemetry: vi.fn((_ctx: unknown, _user: unknown, _name: unknown, _attrs: unknown, fn: () => unknown) => fn()),
}));

vi.mock('../../../src/database/utils', () => ({
  isEmptyField: vi.fn((v: unknown) => !v),
  RABBIT_QUEUE_PREFIX: 'opencti_',
  wait: vi.fn(),
}));

vi.mock('../../../src/database/middleware-loader', () => ({
  fullEntitiesList: vi.fn(async () => []),
}));

vi.mock('../../../src/database/raw-file-storage', () => ({
  s3ConnectionConfig: vi.fn(() => ({})),
}));

vi.mock('../../../src/utils/access', () => ({
  SYSTEM_USER: {},
}));

vi.mock('../../../src/schema/internalObject', () => ({
  ENTITY_TYPE_BACKGROUND_TASK: 'Background-Task',
  ENTITY_TYPE_CONNECTOR: 'Connector',
  ENTITY_TYPE_SYNC: 'Sync',
}));

vi.mock('../../../src/modules/playbook/playbook-types', () => ({
  ENTITY_TYPE_PLAYBOOK: 'Playbook',
}));

vi.mock('lru-cache', () => {
  class FakeLRUCache {
    private readonly cache = new Map<string, unknown>();

    get(key: string) {
      return this.cache.get(key);
    }

    set(key: string, value: unknown) {
      this.cache.set(key, value);
    }

    delete(key: string) {
      return this.cache.delete(key);
    }
  }
  return { LRUCache: FakeLRUCache };
});

import { ensureConnectorQueues, unregisterConnector } from '../../../src/database/rabbitmq';

describe('rabbitmq: connector queue ensure cache', () => {
  beforeEach(() => {
    vi.clearAllMocks();
  });

  it('reuses the recent queue verification for repeated heartbeats', async () => {
    await ensureConnectorQueues('connector--cached', 'Cached Connector', 'EXTERNAL_IMPORT', ['indicator']);
    await ensureConnectorQueues('connector--cached', 'Cached Connector', 'EXTERNAL_IMPORT', ['indicator']);

    expect(mockConnect).toHaveBeenCalledTimes(1);
    expect(mockAssertExchange).toHaveBeenCalledTimes(2);
    expect(mockAssertQueue).toHaveBeenCalledTimes(2);
    expect(mockBindQueue).toHaveBeenCalledTimes(2);
  });

  it('rechecks queues when the connector configuration changes', async () => {
    await ensureConnectorQueues('connector--changed', 'Original Name', 'EXTERNAL_IMPORT', ['indicator']);
    await ensureConnectorQueues('connector--changed', 'Updated Name', 'EXTERNAL_IMPORT', ['indicator']);

    expect(mockConnect).toHaveBeenCalledTimes(2);
  });

  it('rechecks queues after connector unregister', async () => {
    await ensureConnectorQueues('connector--removed', 'Removed Connector', 'EXTERNAL_IMPORT', ['indicator']);
    await unregisterConnector('connector--removed');
    await ensureConnectorQueues('connector--removed', 'Removed Connector', 'EXTERNAL_IMPORT', ['indicator']);

    expect(mockConnect).toHaveBeenCalledTimes(4);
  });
});
