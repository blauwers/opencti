import { beforeEach, describe, expect, it, vi } from 'vitest';
import { STIX_EXT_OCTI } from '../../../src/types/stix-2-1-extensions';

const mocks = vi.hoisted(() => ({
  elUpdateFilesWithEntityRestrictions: vi.fn(),
  executionContext: vi.fn(() => ({})),
  internalLoadById: vi.fn(),
  redisSetManagerEventState: vi.fn(),
  logApp: {
    debug: vi.fn(),
    error: vi.fn(),
    info: vi.fn(),
  },
}));

vi.mock('../../../src/config/conf', async (importOriginal) => {
  const actual = await importOriginal<typeof import('../../../src/config/conf')>();
  return {
    ...actual,
    default: {
      ...actual.default,
      get: vi.fn(),
    },
    ENABLED_FILE_INDEX_MANAGER: true,
    logApp: mocks.logApp,
  };
});

vi.mock('../../../src/database/stream/stream-handler', () => ({
  createStreamProcessor: vi.fn(),
}));

vi.mock('../../../src/database/redis', () => ({
  redisGetManagerEventState: vi.fn(),
  redisSetManagerEventState: mocks.redisSetManagerEventState,
}));

vi.mock('../../../src/lock/master-lock', () => ({
  lockResources: vi.fn(),
}));

vi.mock('../../../src/utils/access', () => ({
  executionContext: mocks.executionContext,
  SYSTEM_USER: { id: 'system' },
}));

vi.mock('../../../src/database/cache', () => ({
  getEntityFromCache: vi.fn(),
}));

vi.mock('../../../src/database/engine', () => ({
  isAttachmentProcessorEnabled: vi.fn(),
}));

vi.mock('../../../src/database/file-search', () => ({
  elIndexFiles: vi.fn(),
  elUpdateFilesWithEntityRestrictions: mocks.elUpdateFilesWithEntityRestrictions,
}));

vi.mock('../../../src/database/raw-file-storage', () => ({
  getFileContent: vi.fn(),
}));

vi.mock('../../../src/modules/managerConfiguration/managerConfiguration-domain', () => ({
  getManagerConfigurationFromCache: vi.fn(),
  updateManagerConfigurationLastRun: vi.fn(),
}));

vi.mock('../../../src/modules/internal/document/document-domain', () => ({
  allFilesForPaths: vi.fn(),
  getIndexFromDate: vi.fn(),
}));

vi.mock('../../../src/domain/file', () => ({
  buildOptionsFromFileManager: vi.fn(),
}));

vi.mock('../../../src/database/middleware-loader', () => ({
  internalLoadById: mocks.internalLoadById,
}));

vi.mock('../../../src/enterprise-edition/ee', () => ({
  isEnterpriseEditionFromSettings: vi.fn(),
}));

vi.mock('../../../src/manager/interruptible-timer', () => ({
  InterruptibleTimer: class {
    start = vi.fn();
  },
}));

const { handleStreamEvents, hasDataRestrictionsUpdate } = await import('../../../src/manager/fileIndexManager');

const buildUpdateEvent = (patch: Array<Record<string, unknown>>) => ({
  id: 'event-1',
  event: 'update',
  data: {
    type: 'update',
    version: '1',
    scope: 'external',
    origin: {},
    message: 'updated',
    data: {
      extensions: {
        [STIX_EXT_OCTI]: {
          id: 'entity-1',
          type: 'Report',
          files: [{ name: 'report.pdf' }],
        },
      },
    },
    context: {
      patch,
      reverse_patch: [],
      changes: [],
    },
  },
}) as any;

describe('file index manager restriction updates', () => {
  beforeEach(() => {
    vi.clearAllMocks();
    mocks.internalLoadById.mockResolvedValue({ internal_id: 'entity-1' });
    mocks.elUpdateFilesWithEntityRestrictions.mockResolvedValue(undefined);
    mocks.redisSetManagerEventState.mockResolvedValue(undefined);
  });

  it('returns false for unrelated entity patches', () => {
    expect(hasDataRestrictionsUpdate([
      { op: 'replace', path: '/name', value: 'Updated name' },
      { op: 'replace', path: '/description', value: 'Updated description' },
    ] as any)).toBe(false);
  });

  it('skips file restriction rewrites for unrelated entity patches', async () => {
    await handleStreamEvents([buildUpdateEvent([
      { op: 'replace', path: '/name', value: 'Updated name' },
    ])]);

    expect(mocks.internalLoadById).not.toHaveBeenCalled();
    expect(mocks.elUpdateFilesWithEntityRestrictions).not.toHaveBeenCalled();
    expect(mocks.redisSetManagerEventState).toHaveBeenCalledWith('file_index_manager', 'event-1');
  });

  it('rewrites file restrictions for granted-ref patches', async () => {
    await handleStreamEvents([buildUpdateEvent([
      { op: 'add', path: '/granted_refs/0', value: 'identity--1' },
    ])]);

    expect(mocks.internalLoadById).toHaveBeenCalledWith({}, { id: 'system' }, 'entity-1', { type: 'Report' });
    expect(mocks.elUpdateFilesWithEntityRestrictions).toHaveBeenCalledWith({ internal_id: 'entity-1' });
  });
});
