import { beforeEach, describe, expect, it, vi } from 'vitest';

const mocks = vi.hoisted(() => ({
  deleteWorksRaw: vi.fn(),
  elList: vi.fn(),
  elUpdateWithBufferedApply: vi.fn(),
  executeSingleBatchMutation: vi.fn(),
  redisGetConnectorStatus: vi.fn(),
  redisGetWorkCompletionState: vi.fn(),
  redisGetWorksCompletionState: vi.fn(),
}));

vi.mock('../../../src/database/redis', () => ({
  redisGetConnectorStatus: mocks.redisGetConnectorStatus,
  redisGetWorkCompletionState: mocks.redisGetWorkCompletionState,
  redisGetWorksCompletionState: mocks.redisGetWorksCompletionState,
}));

vi.mock('../../../src/database/engine', () => ({
  elList: mocks.elList,
  elUpdateWithBufferedApply: mocks.elUpdateWithBufferedApply,
}));

vi.mock('../../../src/domain/work', async (importOriginal) => {
  const actual = await importOriginal<typeof import('../../../src/domain/work')>();
  return {
    ...actual,
    deleteWorksRaw: mocks.deleteWorksRaw,
  };
});

vi.mock('../../../src/modules/batch/batch-executor', () => ({
  BatchMutationKind: {
    UpdateAttribute: 'update-attribute',
  },
  executeSingleBatchMutation: mocks.executeSingleBatchMutation,
}));

const { closeOldWorks } = await import('../../../src/manager/connectorManager');

const buildElements = (count: number) => Array.from({ length: count }, (_, index) => ({
  _index: 'test_history-000001',
  internal_id: `work-${index}`,
}));

const completeState = {
  expected: 1,
  total: 1,
  isProcessed: false,
  isMultiPartWork: false,
};

describe('connector manager closeOldWorks', () => {
  beforeEach(() => {
    vi.clearAllMocks();
    mocks.redisGetConnectorStatus.mockResolvedValue('work_connector-1_2026-08-12T00:00:00.000Z');
    mocks.elList.mockImplementation(async (_context, _user, _indices, opts) => {
      await opts.callback(buildElements(100));
    });
    mocks.redisGetWorksCompletionState.mockResolvedValue(Object.fromEntries(
      buildElements(100).map((element) => [element.internal_id, completeState]),
    ));
    mocks.redisGetWorkCompletionState.mockResolvedValue(completeState);
    mocks.elUpdateWithBufferedApply.mockResolvedValue(undefined);
    mocks.executeSingleBatchMutation.mockImplementation(async ({ executeWrite }) => executeWrite());
  });

  it('loads completion state once and reconciles the page through one batch boundary', async () => {
    await closeOldWorks({} as any, { internal_id: 'connector-1' } as any);

    expect(mocks.redisGetWorksCompletionState).toHaveBeenCalledTimes(1);
    expect(mocks.redisGetWorksCompletionState).toHaveBeenCalledWith(buildElements(100).map((element) => element.internal_id));
    expect(mocks.redisGetWorkCompletionState).not.toHaveBeenCalled();
    expect(mocks.executeSingleBatchMutation).toHaveBeenCalledTimes(1);
    expect(mocks.elUpdateWithBufferedApply).toHaveBeenCalledTimes(100);
  });

  it('falls back to scalar Redis reads when the pipeline read fails', async () => {
    mocks.redisGetWorksCompletionState.mockRejectedValueOnce(new Error('pipeline failed'));

    await closeOldWorks({} as any, { internal_id: 'connector-1' } as any);

    expect(mocks.redisGetWorksCompletionState).toHaveBeenCalledTimes(1);
    expect(mocks.redisGetWorkCompletionState).toHaveBeenCalledTimes(100);
    expect(mocks.executeSingleBatchMutation).toHaveBeenCalledTimes(1);
    expect(mocks.elUpdateWithBufferedApply).toHaveBeenCalledTimes(100);
  });

  it('falls back to scalar updates when the batch write fails', async () => {
    mocks.executeSingleBatchMutation.mockRejectedValueOnce(new Error('bulk update failed'));
    mocks.elUpdateWithBufferedApply
      .mockRejectedValueOnce(new Error('single update failed'))
      .mockResolvedValue(undefined);

    await closeOldWorks({} as any, { internal_id: 'connector-1' } as any);

    expect(mocks.executeSingleBatchMutation).toHaveBeenCalledTimes(1);
    expect(mocks.elUpdateWithBufferedApply).toHaveBeenCalledTimes(100);
  });
});
