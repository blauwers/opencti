import { describe, expect, it, vi, beforeEach } from 'vitest';

vi.mock('../../../src/database/middleware', () => ({
  MutationIntent: {
    Semantic: 'semantic',
    Touch: 'touch',
  },
  updateAttribute: vi.fn(),
  createEntity: vi.fn(),
  patchAttribute: vi.fn(),
  patchAttributeFromLoadedWithRefsInBatch: vi.fn(),
  deleteElementById: vi.fn(),
  internalDeleteElementById: vi.fn(),
}));
vi.mock('../../../src/database/engine', () => ({
  elLoadById: vi.fn(), elUpdate: vi.fn(), elList: vi.fn(), elCount: vi.fn(), elFindByIds: vi.fn(),
}));
vi.mock('../../../src/database/redis', () => ({
  notify: vi.fn(),
  setEditContext: vi.fn(),
  delEditContext: vi.fn(),
  redisGetWork: vi.fn(),
  redisSetConnectorHealthMetrics: vi.fn(),
  redisGetConnectorHealthMetrics: vi.fn(),
  redisSetConnectorLogs: vi.fn(),
}));
vi.mock('../../../src/database/rabbitmq', () => ({
  unregisterConnector: vi.fn(), registerConnectorQueues: vi.fn(), ensureConnectorQueues: vi.fn(),
  purgeConnectorQueues: vi.fn(), getConnectorQueueDetails: vi.fn(), unregisterExchanges: vi.fn(),
}));
vi.mock('../../../src/database/repository', async (importOriginal) => {
  const actual = await importOriginal<typeof import('../../../src/database/repository')>();
  return {
    ...actual,
    connector: vi.fn(),
    connectors: vi.fn(),
    connectorsFor: vi.fn(),
    completeConnector: vi.fn(),
  };
});
vi.mock('../../../src/database/middleware-loader', () => ({
  storeLoadById: vi.fn(), fullEntitiesList: vi.fn(), internalFindByIds: vi.fn(), internalLoadById: vi.fn(), pageEntitiesConnection: vi.fn(),
}));
vi.mock('../../../src/listener/UserActionListener', () => ({
  publishUserAction: vi.fn(), completeContextDataForEntity: vi.fn(),
}));
vi.mock('../../../src/modules/catalog/catalog-domain', () => ({
  computeConnectorTargetContract: vi.fn(), getSupportedContractsByImage: vi.fn(),
}));
vi.mock('../../../src/database/cache', () => ({ getEntitiesMapFromCache: vi.fn() }));
vi.mock('../../../src/manager/telemetryManager', () => ({
  addConnectorDeployedCount: vi.fn(), addWorkbenchDraftConvertionCount: vi.fn(), addWorkbenchValidationCount: vi.fn(),
}));
vi.mock('../../../src/modules/user/user-domain', () => ({ createOnTheFlyUser: vi.fn() }));
vi.mock('../../../src/modules/draftWorkspace/draftWorkspace-domain', () => ({ addDraftWorkspace: vi.fn() }));
vi.mock('../../../src/utils/platformCrypto', () => ({
  getPlatformCrypto: vi.fn(),
}));
vi.mock('../../../src/domain/connector-sync-crypto', () => ({
  encryptSynchronizerCredential: vi.fn(), decryptSynchronizerCredential: vi.fn(),
}));
vi.mock('../../../src/modules/ingestion/ingestion-common', () => ({
  verifyIngestionUri: vi.fn(),
}));
vi.mock('../../../src/domain/connector-utils', () => ({
  testSync: vi.fn(), createSyncHttpUri: vi.fn(),
}));
vi.mock('../../../src/database/file-storage', () => ({
  loadFile: vi.fn(), uploadJobImport: vi.fn(), defaultValidationMode: vi.fn(),
}));
vi.mock('../../../src/database/entity-representative', () => ({ extractEntityRepresentativeName: vi.fn() }));
vi.mock('../../../src/utils/http-client', () => ({ getHttpClient: vi.fn() }));
vi.mock('../../../src/utils/confidence-level', () => ({ controlUserConfidenceAgainstElement: vi.fn() }));
vi.mock('../../../src/config/conf', async () => {
  const actual = await vi.importActual('../../../src/config/conf');
  return { ...actual, logApp: { warn: vi.fn(), error: vi.fn(), info: vi.fn(), debug: vi.fn() } };
});

import { encryptSynchronizerCredential } from '../../../src/domain/connector-sync-crypto';
import { testSync } from '../../../src/domain/connector-utils';
import { MutationIntent, updateAttribute, createEntity, patchAttribute, patchAttributeFromLoadedWithRefsInBatch } from '../../../src/database/middleware';
import { internalFindByIds, storeLoadById } from '../../../src/database/middleware-loader';
import { notify } from '../../../src/database/redis';
import { ensureConnectorQueues } from '../../../src/database/rabbitmq';
import { completeConnector } from '../../../src/database/repository';
import { getEntitiesMapFromCache } from '../../../src/database/cache';
import { getHttpClient } from '../../../src/utils/http-client';
import { createOnTheFlyUser } from '../../../src/modules/user/user-domain';
import { verifyIngestionUri } from '../../../src/modules/ingestion/ingestion-common';
import {
  syncEditField,
  registerSync,
  findSyncById,
  testSync as connectorTestSync,
  fetchRemoteStreams,
  invalidateConnectorHeartbeatCache,
  pingConnector,
} from '../../../src/domain/connector';
import { publishUserAction } from '../../../src/listener/UserActionListener';

const fakeContext = {} as any;
const fakeUser = { id: 'user-1', name: 'Test User', capabilities: [] } as any;

describe('connector.ts — syncEditField token encryption', () => {
  beforeEach(() => {
    vi.clearAllMocks();
    vi.mocked(encryptSynchronizerCredential).mockImplementation(async (v: string | null | undefined) => v ? `encrypted:${v}` : v);
    vi.mocked(verifyIngestionUri).mockImplementation(() => undefined);
    vi.mocked(updateAttribute).mockResolvedValue({ element: { id: 'x', name: 'y' } } as never);
    vi.mocked(publishUserAction).mockResolvedValue([] as void[]);
    vi.mocked(notify).mockResolvedValue(undefined as never);
  });

  it('should encrypt token value before calling updateAttribute', async () => {
    const input = [
      { key: 'token', value: ['my-plain-token'] },
      { key: 'name', value: ['updated name'] },
    ];

    await syncEditField(fakeContext, fakeUser, 'test-sync-id', input);

    expect(encryptSynchronizerCredential).toHaveBeenCalledWith('my-plain-token');
    expect(input[0].value[0]).toBe('encrypted:my-plain-token');
    expect(input[1].value[0]).toBe('updated name');
  });

  it('should not encrypt when input has no token key', async () => {
    const input = [{ key: 'name', value: ['updated name'] }];

    await syncEditField(fakeContext, fakeUser, 'test-sync-id', input);

    expect(encryptSynchronizerCredential).not.toHaveBeenCalled();
  });

  it('should not encrypt when token value is empty string', async () => {
    const input = [{ key: 'token', value: [''] }];

    await syncEditField(fakeContext, fakeUser, 'test-sync-id', input);

    expect(encryptSynchronizerCredential).not.toHaveBeenCalled();
  });

  it('should validate uri against deny list when uri is edited', async () => {
    const input = [{ key: 'uri', value: ['https://example.allowed.com'] }];

    await syncEditField(fakeContext, fakeUser, 'test-sync-id', input);

    expect(verifyIngestionUri).toHaveBeenCalledWith('https://example.allowed.com');
    expect(updateAttribute).toHaveBeenCalled();
  });

  it('should reject uri edition when uri is denied', async () => {
    vi.mocked(verifyIngestionUri).mockImplementation(() => {
      throw new Error('This URI is not allowed for ingestion.');
    });
    const input = [{ key: 'uri', value: ['https://example.denied.com'] }];

    await expect(syncEditField(fakeContext, fakeUser, 'test-sync-id', input))
      .rejects.toThrow('This URI is not allowed for ingestion.');

    expect(updateAttribute).not.toHaveBeenCalled();
  });

  it('should fail fast on denied uri before token encryption', async () => {
    vi.mocked(verifyIngestionUri).mockImplementation(() => {
      throw new Error('This URI is not allowed for ingestion.');
    });
    const input = [
      { key: 'uri', value: ['https://example.denied.com'] },
      { key: 'token', value: ['my-plain-token'] },
    ];

    await expect(syncEditField(fakeContext, fakeUser, 'test-sync-id', input))
      .rejects.toThrow('This URI is not allowed for ingestion.');

    expect(encryptSynchronizerCredential).not.toHaveBeenCalled();
    expect(updateAttribute).not.toHaveBeenCalled();
  });
});

describe('connector.ts — registerSync token encryption', () => {
  beforeEach(() => {
    vi.clearAllMocks();
    vi.mocked(encryptSynchronizerCredential).mockImplementation(async (v: string | null | undefined) => v ? `encrypted:${v}` : v);
    vi.mocked(verifyIngestionUri).mockImplementation(() => undefined);
    vi.mocked(testSync).mockResolvedValue('Connection success' as never);
    vi.mocked(createEntity).mockResolvedValue({
      element: { id: 'test-sync-id', internal_id: 'test-sync-id' },
      isCreation: true,
    } as never);
    vi.mocked(publishUserAction).mockResolvedValue([] as void[]);
  });

  it('should encrypt token before createEntity', async () => {
    const input = {
      name: 'Test synchronizer',
      uri: 'http://remote-opencti.invalid',
      token: 'secret-stream-token',
      stream_id: 'live',
      user_id: fakeUser.id,
      listen_deletion: false,
      no_dependencies: false,
    } as never;

    await registerSync(fakeContext, fakeUser, input);

    expect(verifyIngestionUri).toHaveBeenCalledWith('http://remote-opencti.invalid');
    expect(encryptSynchronizerCredential).toHaveBeenCalledWith('secret-stream-token');
    expect(testSync).toHaveBeenCalled();
    expect(createEntity).toHaveBeenCalled();
  });

  it('should not encrypt when token is absent', async () => {
    const input = {
      name: 'Test synchronizer no token',
      uri: 'http://remote-opencti.invalid',
      stream_id: 'live',
      user_id: fakeUser.id,
      listen_deletion: false,
      no_dependencies: false,
    } as never;

    await registerSync(fakeContext, fakeUser, input);

    expect(verifyIngestionUri).toHaveBeenCalledWith('http://remote-opencti.invalid');
    expect(encryptSynchronizerCredential).not.toHaveBeenCalled();
    expect(createEntity).toHaveBeenCalled();
  });

  it('should reject creation when uri is denied', async () => {
    vi.mocked(verifyIngestionUri).mockImplementation(() => {
      throw new Error('This URI is not allowed for ingestion.');
    });
    const input = {
      name: 'Test synchronizer denied uri',
      uri: 'http://example.denied.com',
      token: 'secret-stream-token',
      stream_id: 'live',
      user_id: fakeUser.id,
      listen_deletion: false,
      no_dependencies: false,
    } as never;

    await expect(registerSync(fakeContext, fakeUser, input))
      .rejects.toThrow('This URI is not allowed for ingestion.');

    expect(testSync).not.toHaveBeenCalled();
    expect(createEntity).not.toHaveBeenCalled();
  });

  it('should fail fast on denied uri before auto user creation', async () => {
    vi.mocked(verifyIngestionUri).mockImplementation(() => {
      throw new Error('This URI is not allowed for ingestion.');
    });
    vi.mocked(createOnTheFlyUser).mockResolvedValue({ id: 'auto-user-id' } as never);

    const input = {
      name: 'Test synchronizer denied uri with automatic user',
      uri: 'http://example.denied.com',
      stream_id: 'live',
      user_id: 'auto-user-name',
      automatic_user: true,
      confidence_level: 50,
      listen_deletion: false,
      no_dependencies: false,
    } as never;

    await expect(registerSync(fakeContext, fakeUser, input))
      .rejects.toThrow('This URI is not allowed for ingestion.');

    expect(createOnTheFlyUser).not.toHaveBeenCalled();
    expect(testSync).not.toHaveBeenCalled();
    expect(createEntity).not.toHaveBeenCalled();
  });
});

describe('connector.ts — findSyncById', () => {
  beforeEach(() => {
    vi.clearAllMocks();
    vi.mocked(storeLoadById).mockResolvedValue({ id: 'test-sync-id', name: 'My Sync' } as never);
  });

  it('should delegate to storeLoadById', async () => {
    const result = await findSyncById(fakeContext, fakeUser, 'test-sync-id');

    expect(storeLoadById).toHaveBeenCalledWith(fakeContext, fakeUser, 'test-sync-id', 'Sync');
    expect(result).toEqual({ id: 'test-sync-id', name: 'My Sync' });
  });
});

describe('connector.ts — testSync deny-list coverage', () => {
  beforeEach(() => {
    vi.clearAllMocks();
    vi.mocked(verifyIngestionUri).mockImplementation(() => undefined);
    vi.mocked(testSync).mockResolvedValue('Connection success' as never);
  });

  it('should validate uri against deny list before testing sync', async () => {
    const input = {
      uri: 'https://example.allowed.com',
      token: '',
      ssl_verify: false,
    } as never;

    await connectorTestSync(fakeContext, fakeUser, input);

    expect(verifyIngestionUri).toHaveBeenCalledWith('https://example.allowed.com');
    expect(testSync).toHaveBeenCalledWith(fakeContext, fakeUser, input);
  });

  it('should reject verify when uri is denied', async () => {
    vi.mocked(verifyIngestionUri).mockImplementation(() => {
      throw new Error('This URI is not allowed for ingestion.');
    });
    const input = {
      uri: 'https://example.denied.com',
      token: '',
      ssl_verify: false,
    } as never;

    await expect(connectorTestSync(fakeContext, fakeUser, input))
      .rejects.toThrow('This URI is not allowed for ingestion.');

    expect(testSync).not.toHaveBeenCalled();
  });
});

describe('connector.ts — fetchRemoteStreams deny-list coverage', () => {
  beforeEach(() => {
    vi.clearAllMocks();
    vi.mocked(verifyIngestionUri).mockImplementation(() => undefined);
  });

  it('should reject validate when uri is denied before any HTTP call', async () => {
    vi.mocked(verifyIngestionUri).mockImplementation(() => {
      throw new Error('This URI is not allowed for ingestion.');
    });

    await expect(fetchRemoteStreams(fakeContext, fakeUser, {
      uri: 'https://example.denied.com',
      token: '',
      ssl_verify: false,
    } as never)).rejects.toThrow('This URI is not allowed for ingestion.');

    expect(getHttpClient).not.toHaveBeenCalled();
  });
});

describe('connector ping refresh behavior', () => {
  beforeEach(() => {
    vi.clearAllMocks();
    invalidateConnectorHeartbeatCache();
    vi.mocked(getEntitiesMapFromCache).mockResolvedValue(new Map([['connector--1', {
      id: 'connector--1',
      name: 'Example Connector',
      connector_type: 'EXTERNAL_IMPORT',
      connector_scope: 'indicator,malware',
      connector_state: '{"last_run":0}',
      updated_at: new Date(0),
    }]]) as never);
    vi.mocked(ensureConnectorQueues).mockResolvedValue(undefined as never);
    vi.mocked(patchAttributeFromLoadedWithRefsInBatch).mockResolvedValue({
      element: {
        id: 'connector--1',
        name: 'Example Connector',
        connector_type: 'EXTERNAL_IMPORT',
        connector_scope: 'indicator,malware',
        connector_state: '{"last_run":1}',
        updated_at: new Date(),
      },
    } as never);
    vi.mocked(completeConnector).mockImplementation((connector) => connector as never);
  });

  it('updates heartbeat state without forcing a refresh or reloading the connector', async () => {
    const result = await pingConnector(fakeContext, fakeUser, 'connector--1', '{"last_run":1}', {
      buffering: false,
      queue_threshold: 500,
    } as never);

    expect(ensureConnectorQueues).toHaveBeenCalledWith('connector--1', 'Example Connector', 'EXTERNAL_IMPORT', ['indicator', 'malware']);
    expect(patchAttributeFromLoadedWithRefsInBatch).toHaveBeenCalledWith(
      fakeContext,
      fakeUser,
      expect.objectContaining({ id: 'connector--1' }),
      expect.objectContaining({ connector_state: '{"last_run":1}' }),
      { forceRefresh: false, mutationIntent: MutationIntent.Touch },
    );
    expect(getEntitiesMapFromCache).toHaveBeenCalledWith(fakeContext, fakeUser, 'Connector');
    expect(internalFindByIds).not.toHaveBeenCalled();
    expect(storeLoadById).not.toHaveBeenCalled();
    expect(completeConnector).toHaveBeenCalledWith(expect.objectContaining({ connector_state: '{"last_run":1}' }));
    expect(result).toMatchObject({ connector_state: '{"last_run":1}' });
  });

  it('skips stable heartbeat persistence until the durable touch interval expires', async () => {
    vi.useFakeTimers();
    vi.setSystemTime(new Date('2026-08-05T12:00:00.000Z'));
    try {
      vi.mocked(getEntitiesMapFromCache).mockResolvedValue(new Map([['connector--stable', {
        id: 'connector--stable',
        name: 'Stable Connector',
        connector_type: 'EXTERNAL_IMPORT',
        connector_scope: 'indicator',
        connector_state: 'null',
        connector_info: { queue_threshold: 500 },
        updated_at: new Date('2026-08-05T11:55:00.000Z'),
      }]]) as never);
      vi.mocked(patchAttributeFromLoadedWithRefsInBatch).mockResolvedValue({
        element: {
          id: 'connector--stable',
          name: 'Stable Connector',
          connector_type: 'EXTERNAL_IMPORT',
          connector_scope: 'indicator',
          connector_state: 'null',
          connector_info: { queue_threshold: 500 },
          updated_at: new Date('2026-08-05T12:00:00.000Z'),
        },
      } as never);

      await pingConnector(fakeContext, fakeUser, 'connector--stable', 'null', { queue_threshold: 500 } as never);
      await pingConnector(fakeContext, fakeUser, 'connector--stable', 'null', { queue_threshold: 500 } as never);

      expect(patchAttributeFromLoadedWithRefsInBatch).toHaveBeenCalledTimes(1);

      vi.setSystemTime(new Date('2026-08-05T12:04:01.000Z'));
      await pingConnector(fakeContext, fakeUser, 'connector--stable', 'null', { queue_threshold: 500 } as never);

      expect(patchAttributeFromLoadedWithRefsInBatch).toHaveBeenCalledTimes(2);
    } finally {
      vi.useRealTimers();
    }
  });

  it('treats an empty persisted state and a JSON null heartbeat state as the same value', async () => {
    vi.mocked(getEntitiesMapFromCache).mockResolvedValue(new Map([['connector--null-state', {
      id: 'connector--null-state',
      name: 'Null State Connector',
      connector_type: 'EXTERNAL_IMPORT',
      connector_scope: 'indicator',
      connector_state: null,
      connector_info: { queue_threshold: 500 },
      updated_at: new Date(),
    }]]) as never);

    await pingConnector(fakeContext, fakeUser, 'connector--null-state', 'null', { queue_threshold: 500 } as never);

    expect(patchAttributeFromLoadedWithRefsInBatch).not.toHaveBeenCalled();
  });

  it('drops the runtime overlay when the connector cache refreshes', async () => {
    vi.mocked(getEntitiesMapFromCache).mockResolvedValue(new Map([['connector--cache-refresh', {
      id: 'connector--cache-refresh',
      name: 'Cache Refresh Connector',
      connector_type: 'EXTERNAL_IMPORT',
      connector_scope: 'indicator',
      connector_state: '{"last_run":0}',
      connector_info: { queue_threshold: 500 },
      updated_at: new Date(),
    }]]) as never);

    await pingConnector(fakeContext, fakeUser, 'connector--cache-refresh', '{"last_run":0}', { queue_threshold: 500 } as never);

    vi.mocked(getEntitiesMapFromCache).mockResolvedValue(new Map([['connector--cache-refresh', {
      id: 'connector--cache-refresh',
      name: 'Cache Refresh Connector',
      connector_type: 'EXTERNAL_IMPORT',
      connector_scope: 'indicator',
      connector_state: '{"last_run":1}',
      connector_info: { queue_threshold: 500 },
      updated_at: new Date(),
    }]]) as never);

    await pingConnector(fakeContext, fakeUser, 'connector--cache-refresh', '{"last_run":0}', { queue_threshold: 500 } as never);

    expect(patchAttributeFromLoadedWithRefsInBatch).toHaveBeenCalledTimes(1);
    expect(patchAttributeFromLoadedWithRefsInBatch).toHaveBeenCalledWith(
      fakeContext,
      fakeUser,
      expect.objectContaining({ connector_state: '{"last_run":1}' }),
      expect.objectContaining({ connector_state: '{"last_run":0}' }),
      { forceRefresh: false, mutationIntent: MutationIntent.Touch },
    );
  });

  it('does not enter the write path once the heartbeat request is already aborted', async () => {
    const abortController = new AbortController();
    abortController.abort();
    vi.mocked(getEntitiesMapFromCache).mockResolvedValue(new Map([['connector--aborted', {
      id: 'connector--aborted',
      name: 'Aborted Connector',
      connector_type: 'EXTERNAL_IMPORT',
      connector_scope: 'indicator',
      connector_state: '{"last_run":0}',
      updated_at: new Date(0),
    }]]) as never);

    const result = await pingConnector({ requestAbortSignal: abortController.signal } as any, fakeUser, 'connector--aborted', '{"last_run":1}', {
      queue_threshold: 500,
    } as never);

    expect(patchAttributeFromLoadedWithRefsInBatch).not.toHaveBeenCalled();
    expect(result).toMatchObject({ id: 'connector--aborted' });
  });

  it('publishes a cache refresh when a heartbeat clears a remote state reset', async () => {
    vi.mocked(getEntitiesMapFromCache).mockResolvedValue(new Map([['connector--reset', {
      id: 'connector--reset',
      name: 'Reset Connector',
      connector_type: 'EXTERNAL_IMPORT',
      connector_scope: 'indicator',
      connector_state: '',
      connector_state_reset: true,
      updated_at: new Date(),
    }]]) as never);
    vi.mocked(patchAttributeFromLoadedWithRefsInBatch).mockResolvedValue({
      element: {
        id: 'connector--reset',
        name: 'Reset Connector',
        connector_type: 'EXTERNAL_IMPORT',
        connector_scope: 'indicator',
        connector_state: '',
        connector_state_reset: false,
        updated_at: new Date(),
      },
    } as never);

    await pingConnector(fakeContext, fakeUser, 'connector--reset', '{"last_run":1}', { queue_threshold: 500 } as never);

    expect(patchAttributeFromLoadedWithRefsInBatch).toHaveBeenCalledWith(
      fakeContext,
      fakeUser,
      expect.objectContaining({ connector_state_reset: true }),
      expect.objectContaining({ connector_state_reset: false }),
      { forceRefresh: false, mutationIntent: MutationIntent.Touch },
    );
    expect(notify).toHaveBeenCalledWith(expect.stringContaining('CONNECTOR_EDIT_TOPIC'), expect.objectContaining({ id: 'connector--reset' }), fakeUser);
  });
});
