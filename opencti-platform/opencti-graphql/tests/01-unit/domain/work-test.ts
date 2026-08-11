import { beforeEach, describe, expect, it, vi } from 'vitest';
import { BULK_TIMEOUT, elBulk, elFindByIds, elIndex, elLoadById, elUpdateWithBufferedApply } from '../../../src/database/engine';
import {
  redisGetWorksCompletionState,
  redisInitializeWork,
  redisInitializeWorks,
  redisMarkWorksAsProcessed,
} from '../../../src/database/redis';
import { createWorks, updateProcessedTimes, updateReceivedTimes } from '../../../src/domain/work';
import { INDEX_HISTORY, READ_INDEX_HISTORY } from '../../../src/database/utils';
import { ENTITY_TYPE_WORK } from '../../../src/schema/internalObject';

vi.mock('../../../src/database/engine', async (importOriginal) => {
  const actual = await importOriginal<typeof import('../../../src/database/engine')>();
  return {
    ...actual,
    elBulk: vi.fn(),
    elFindByIds: vi.fn(),
    elIndex: vi.fn(),
    elLoadById: vi.fn(),
    elUpdateWithBufferedApply: vi.fn(),
  };
});

vi.mock('../../../src/database/redis', async (importOriginal) => {
  const actual = await importOriginal<typeof import('../../../src/database/redis')>();
  return {
    ...actual,
    redisInitializeWork: vi.fn(),
    redisInitializeWorks: vi.fn(),
    redisGetWorksCompletionState: vi.fn(),
    redisMarkWorksAsProcessed: vi.fn(),
  };
});

describe('createWorks', () => {
  const context = {} as never;
  const user = { id: 'user--1' } as never;
  const connector = {
    connector_name: 'Hygiene',
    connector_type: 'INTERNAL_ENRICHMENT',
    internal_id: 'connector--1',
  };

  beforeEach(() => {
    vi.clearAllMocks();
    vi.mocked(elBulk).mockResolvedValue({ errors: false } as never);
    vi.mocked(elFindByIds).mockImplementation(async (_context, _user, ids) => {
      return (ids as string[]).map((id) => ({
        _index: INDEX_HISTORY,
        entity_type: ENTITY_TYPE_WORK,
        internal_id: id,
      })) as never;
    });
    vi.mocked(elLoadById).mockImplementation(async (_context, _user, id) => ({
      _index: INDEX_HISTORY,
      entity_type: ENTITY_TYPE_WORK,
      internal_id: id,
    }) as never);
    vi.mocked(redisInitializeWork).mockResolvedValue(undefined as never);
    vi.mocked(redisInitializeWorks).mockResolvedValue(undefined as never);
    vi.mocked(redisGetWorksCompletionState).mockResolvedValue({
      'work--1': {
        expected: 0,
        total: 0,
        isProcessed: false,
        isMultiPartWork: false,
      },
      'work--2': {
        expected: 2,
        total: 1,
        isProcessed: false,
        isMultiPartWork: false,
      },
    } as never);
    vi.mocked(redisMarkWorksAsProcessed).mockResolvedValue(undefined as never);
    vi.mocked(elUpdateWithBufferedApply).mockResolvedValue(undefined as never);
  });

  it('bulk indexes and initializes multiple work documents once', async () => {
    const works = await createWorks(context, user, [
      {
        connector,
        friendlyName: 'First work',
        sourceId: 'indicator--1',
        args: { preallocatedWork: { id: 'work--1', timestamp: '2026-08-10T00:00:00.000Z' } },
      },
      {
        connector,
        friendlyName: 'Second work',
        sourceId: 'indicator--2',
        args: {
          isMultiPartWork: true,
          preallocatedWork: { id: 'work--2', timestamp: '2026-08-10T00:00:01.000Z' },
        },
      },
    ]);

    expect(elIndex).not.toHaveBeenCalled();
    expect(elBulk).toHaveBeenCalledTimes(1);
    expect(elBulk).toHaveBeenCalledWith(context, expect.objectContaining({
      refresh: true,
      timeout: BULK_TIMEOUT,
    }));
    const body = vi.mocked(elBulk).mock.calls[0][1].body;
    expect(body[0]).toEqual({ index: { _index: INDEX_HISTORY, _id: 'work--1' } });
    expect(body[1]).toEqual(expect.objectContaining({
      connector_id: 'connector--1',
      entity_type: ENTITY_TYPE_WORK,
      event_source_id: 'indicator--1',
      internal_id: 'work--1',
      name: 'First work',
    }));
    expect(body[2]).toEqual({ index: { _index: INDEX_HISTORY, _id: 'work--2' } });
    expect(body[3]).toEqual(expect.objectContaining({
      connector_id: 'connector--1',
      entity_type: ENTITY_TYPE_WORK,
      event_source_id: 'indicator--2',
      internal_id: 'work--2',
      is_multipart: true,
      name: 'Second work',
    }));
    expect(elFindByIds).toHaveBeenCalledWith(context, user, ['work--1', 'work--2'], {
      indices: READ_INDEX_HISTORY,
      type: ENTITY_TYPE_WORK,
      withoutRels: false,
    });
    expect(redisInitializeWorks).toHaveBeenCalledWith([
      { workId: 'work--1', isMultiPartWork: false },
      { workId: 'work--2', isMultiPartWork: true },
    ]);
    expect(works.map((work: { id?: string }) => work?.id)).toEqual(['work--1', 'work--2']);
  });

  it('keeps a single work on the singular indexing and initialization path', async () => {
    const works = await createWorks(context, user, [{
      connector,
      friendlyName: 'Only work',
      sourceId: 'indicator--1',
      args: { preallocatedWork: { id: 'work--1', timestamp: '2026-08-10T00:00:00.000Z' } },
    }]);

    expect(elBulk).not.toHaveBeenCalled();
    expect(elIndex).toHaveBeenCalledWith(INDEX_HISTORY, expect.objectContaining({
      internal_id: 'work--1',
      name: 'Only work',
    }), { context });
    expect(redisInitializeWork).toHaveBeenCalledWith('work--1', false);
    expect(redisInitializeWorks).not.toHaveBeenCalled();
    expect(works.map((work: { id?: string }) => work?.id)).toEqual(['work--1']);
  });

  it('buffers one received transition per loaded work under one batch execution', async () => {
    await updateReceivedTimes(context, user, [
      {
        work: { _index: INDEX_HISTORY, id: 'work--1', internal_id: 'work--1' },
        message: 'Connector ready to process the operation',
      },
      {
        work: { _index: INDEX_HISTORY, id: 'work--2', internal_id: 'work--2' },
        message: 'Connector ready to process the operation',
      },
    ]);

    expect(elUpdateWithBufferedApply).toHaveBeenCalledTimes(2);
    expect(vi.mocked(elUpdateWithBufferedApply).mock.calls.map((call) => call[2])).toEqual(['work--1', 'work--2']);
    expect(vi.mocked(elUpdateWithBufferedApply).mock.calls[0][3]).toMatchObject({
      script: { params: { message: 'Connector ready to process the operation' } },
    });
    expect(vi.mocked(elUpdateWithBufferedApply).mock.calls[1][3]).toMatchObject({
      script: { params: { message: 'Connector ready to process the operation' } },
    });
  });

  it('does not rewrite completed work rows on received replay', async () => {
    await updateReceivedTimes(context, user, [
      {
        work: { _index: INDEX_HISTORY, id: 'work--1', internal_id: 'work--1', status: 'complete' },
        message: 'Connector ready to process the operation',
      },
      {
        work: { _index: INDEX_HISTORY, id: 'work--2', internal_id: 'work--2', status: 'progress' },
        message: 'Connector ready to process the operation',
      },
    ]);

    expect(elUpdateWithBufferedApply).toHaveBeenCalledTimes(1);
    expect(vi.mocked(elUpdateWithBufferedApply).mock.calls[0][2]).toBe('work--2');
    expect(vi.mocked(elUpdateWithBufferedApply).mock.calls[0][3]).toMatchObject({
      script: {
        source: expect.stringContaining('if (ctx._source.status != "complete" && ctx._source.received_time == null)'),
      },
    });
  });

  it('does not append another received message when a non-terminal work was already received', async () => {
    await updateReceivedTimes(context, user, [{
      work: {
        _index: INDEX_HISTORY,
        id: 'work--1',
        internal_id: 'work--1',
        status: 'progress',
        received_time: '2026-08-11T00:00:00.000Z',
      },
      message: 'Connector ready to process the operation',
    }]);

    expect(elUpdateWithBufferedApply).not.toHaveBeenCalled();
  });

  it('reads and marks Redis completion state once for one processed batch', async () => {
    await updateProcessedTimes(context, user, [
      {
        work: {
          _index: INDEX_HISTORY,
          id: 'work--1',
          internal_id: 'work--1',
          event_type: 'OTHER',
          status: 'progress',
        },
        message: 'updated',
        inError: false,
      },
      {
        work: {
          _index: INDEX_HISTORY,
          id: 'work--2',
          internal_id: 'work--2',
          event_type: 'OTHER',
          status: 'progress',
        },
        message: 'failed',
        inError: true,
      },
    ]);

    expect(redisGetWorksCompletionState).toHaveBeenCalledWith(['work--1', 'work--2']);
    expect(redisMarkWorksAsProcessed).toHaveBeenCalledWith(['work--1', 'work--2']);
    expect(elUpdateWithBufferedApply).toHaveBeenCalledTimes(2);
    expect(vi.mocked(elUpdateWithBufferedApply).mock.calls[0][3]).toMatchObject({
      script: {
        params: {
          message: 'updated',
          completed_number: 1,
        },
      },
    });
    expect(vi.mocked(elUpdateWithBufferedApply).mock.calls[1][3]).toMatchObject({
      script: {
        params: {
          message: 'failed',
        },
      },
    });
  });

  it('does not rewrite completed work rows on processed replay', async () => {
    await updateProcessedTimes(context, user, [
      {
        work: {
          _index: INDEX_HISTORY,
          id: 'work--1',
          internal_id: 'work--1',
          event_type: 'OTHER',
          status: 'complete',
        },
        message: 'already complete',
        inError: false,
      },
      {
        work: {
          _index: INDEX_HISTORY,
          id: 'work--2',
          internal_id: 'work--2',
          event_type: 'OTHER',
          status: 'progress',
        },
        message: 'updated',
        inError: false,
      },
    ]);

    expect(redisGetWorksCompletionState).toHaveBeenCalledWith(['work--2']);
    expect(redisMarkWorksAsProcessed).toHaveBeenCalledWith(['work--2']);
    expect(elUpdateWithBufferedApply).toHaveBeenCalledTimes(1);
    expect(vi.mocked(elUpdateWithBufferedApply).mock.calls[0][2]).toBe('work--2');
    expect(vi.mocked(elUpdateWithBufferedApply).mock.calls[0][3]).toMatchObject({
      script: {
        source: expect.stringContaining('if (ctx._source.status != "complete" && ctx._source.processed_time == null)'),
      },
    });
  });

  it('does not append another processed message when a non-terminal work was already processed', async () => {
    await updateProcessedTimes(context, user, [{
      work: {
        _index: INDEX_HISTORY,
        id: 'work--1',
        internal_id: 'work--1',
        event_type: 'OTHER',
        status: 'progress',
        processed_time: '2026-08-11T00:00:00.000Z',
      },
      message: 'already processed',
      inError: false,
    }]);

    expect(redisGetWorksCompletionState).not.toHaveBeenCalled();
    expect(redisMarkWorksAsProcessed).not.toHaveBeenCalled();
    expect(elUpdateWithBufferedApply).not.toHaveBeenCalled();
  });
});
