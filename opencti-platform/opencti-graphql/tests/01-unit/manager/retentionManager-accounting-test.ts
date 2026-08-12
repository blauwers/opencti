import moment from 'moment';
import { beforeEach, describe, expect, it, vi } from 'vitest';

const mocks = vi.hoisted(() => ({
  canDeleteElement: vi.fn(),
  deleteElementById: vi.fn(),
  elPaginate: vi.fn(),
  patchAttribute: vi.fn(),
  publishUserAction: vi.fn(),
  logApp: {
    debug: vi.fn(),
    error: vi.fn(),
    info: vi.fn(),
  },
}));

vi.mock('../../../src/modules/retentionRules/retentionRules-domain', () => ({
  listRules: vi.fn(),
}));

vi.mock('../../../src/config/conf', () => ({
  default: {
    get: vi.fn(),
  },
  booleanConf: vi.fn(() => false),
  logApp: mocks.logApp,
}));

vi.mock('../../../src/database/middleware', () => ({
  deleteElementById: mocks.deleteElementById,
  patchAttribute: mocks.patchAttribute,
}));

vi.mock('../../../src/utils/access', () => ({
  executionContext: vi.fn(),
  RETENTION_MANAGER_USER: { id: 'retention-manager' },
}));

vi.mock('../../../src/utils/format', () => ({
  now: vi.fn(() => '2026-08-12T12:00:00.000Z'),
  utcDate: vi.fn((value?: unknown) => moment.utc(value ?? '2026-08-12T12:00:00.000Z')),
}));

vi.mock('../../../src/database/utils', () => ({
  READ_INDEX_HISTORY: ['history'],
  READ_STIX_INDICES: ['stix'],
}));

vi.mock('../../../src/database/engine', () => ({
  elPaginate: mocks.elPaginate,
}));

vi.mock('../../../src/utils/filtering/filtering-resolution', () => ({
  convertFiltersToQueryOptions: vi.fn(async () => ({})),
}));

vi.mock('../../../src/manager/managerModule', () => ({
  registerManager: vi.fn(),
}));

vi.mock('../../../src/database/data-consistency', () => ({
  canDeleteElement: mocks.canDeleteElement,
}));

vi.mock('../../../src/database/file-storage', () => ({
  deleteFile: vi.fn(),
}));

vi.mock('../../../src/modules/internal/document/document-domain', () => ({
  DELETABLE_FILE_STATUSES: [],
  paginatedForPathWithEnrichment: vi.fn(),
}));

vi.mock('../../../src/config/errors', () => ({
  ALREADY_DELETED_ERROR: 'ALREADY_DELETED',
}));

vi.mock('../../../src/schema/internalObject', () => ({
  ENTITY_TYPE_ACTIVITY: 'Activity',
  ENTITY_TYPE_HISTORY: 'History',
}));

vi.mock('../../../src/listener/UserActionListener', () => ({
  publishUserAction: mocks.publishUserAction,
}));

vi.mock('../../../src/generated/graphql', () => ({
  RetentionRuleScope: {
    Activity: 'activity',
    File: 'file',
    History: 'history',
    Knowledge: 'knowledge',
    Workbench: 'workbench',
  },
  RetentionUnit: {
    Days: 'days',
  },
}));

const { executeProcessing } = await import('../../../src/manager/retentionManager');

const buildHistoryEdge = (id: string) => ({
  node: {
    id,
    internal_id: id,
    entity_type: 'History',
    timestamp: '2026-08-12T10:00:00.000Z',
    updated_at: '2026-08-12T10:00:00.000Z',
  },
});

describe('retention manager accounting', () => {
  beforeEach(() => {
    vi.clearAllMocks();
    mocks.canDeleteElement.mockResolvedValue(true);
    mocks.elPaginate.mockResolvedValue({
      pageInfo: { globalCount: 2 },
      edges: [buildHistoryEdge('history-1'), buildHistoryEdge('history-2')],
    });
    mocks.patchAttribute.mockResolvedValue(undefined);
    mocks.publishUserAction.mockResolvedValue(undefined);
  });

  it('counts and audits only successful deletions', async () => {
    mocks.deleteElementById
      .mockResolvedValueOnce(undefined)
      .mockRejectedValueOnce(new Error('delete failed'));

    await executeProcessing({} as any, {
      id: 'retention-rule-1',
      name: 'History cleanup',
      scope: 'history',
      max_retention: 1,
      retention_unit: 'days',
      active: true,
    } as any);

    expect(mocks.patchAttribute).toHaveBeenCalledWith(
      {},
      { id: 'retention-manager' },
      'retention-rule-1',
      'RetentionRule',
      expect.objectContaining({
        last_deleted_count: 1,
      }),
    );
    expect(mocks.publishUserAction).toHaveBeenCalledWith(expect.objectContaining({
      message: 'Retention rule `History cleanup` deleted `1` `history` entries',
      context_data: expect.objectContaining({
        input: {
          deleted_count: 1,
          deleted_entries: [{
            id: 'history-1',
            timestamp: '2026-08-12T10:00:00.000Z',
          }],
        },
      }),
    }));
    expect(mocks.logApp.debug).toHaveBeenCalledWith(expect.stringMatching(/Retention manager deleted 1 in \d+ ms/));
  });
});
