import { beforeEach, describe, expect, it, vi } from 'vitest';
import { INDEX_FILES } from '../../../src/database/utils';

const elFindByIds = vi.fn(async () => ({}));
const elRawBulk = vi.fn();

vi.mock('../../../src/database/engine', () => ({
  BULK_TIMEOUT: '1h',
  ES_MINIMUM_FIXED_PAGINATION: 1,
  MAX_BULK_BYTES: 2 * 1024 * 1024,
  MAX_BULK_OPERATIONS: 5000,
  buildDataRestrictions: vi.fn(),
  elFindByIds,
  elRawBulk,
  elRawCount: vi.fn(),
  elRawDeleteByQuery: vi.fn(),
  elRawSearch: vi.fn(),
}));

const { elIndexFiles } = await import('../../../src/database/file-search');

const buildFiles = (count: number) => Array.from({ length: count }, (_, index) => ({
  internal_id: `doc-${index}`,
  file_id: `file-${index}`,
  file_data: `content-${index}`,
  entity_id: '',
  name: `file-${index}.txt`,
  uploaded_at: new Date('2026-08-12T00:00:00.000Z'),
}));

const buildSuccessItems = (count: number) => Array.from({ length: count }, () => ({
  index: {
    status: 201,
  },
}));

describe('file search indexing', () => {
  beforeEach(() => {
    vi.clearAllMocks();
    elRawBulk.mockResolvedValue({
      errors: false,
      items: buildSuccessItems(100),
    });
  });

  it('coalesces valid file indexing into one bounded attachment bulk request', async () => {
    const files = buildFiles(100);

    await elIndexFiles({} as any, {} as any, files);

    expect(elFindByIds).toHaveBeenCalledTimes(1);
    expect(elRawBulk).toHaveBeenCalledTimes(1);
    expect(elRawBulk).toHaveBeenCalledWith({}, {
      body: expect.any(Array),
      pipeline: 'attachment',
      refresh: true,
      timeout: '1h',
    });
    const [, { body }] = elRawBulk.mock.calls[0];
    expect(body).toHaveLength(200);
    expect(body[0]).toEqual({ index: { _id: 'doc-0', _index: INDEX_FILES } });
    expect(body[1]).toEqual(expect.objectContaining({
      file_data: 'content-0',
      file_id: 'file-0',
      internal_id: 'doc-0',
    }));
  });

  it('splits attachment writes at the configured bulk operation bound', async () => {
    const files = buildFiles(5001);
    elRawBulk
      .mockResolvedValueOnce({
        errors: false,
        items: buildSuccessItems(5000),
      })
      .mockResolvedValueOnce({
        errors: false,
        items: buildSuccessItems(1),
      });

    await elIndexFiles({} as any, {} as any, files);

    expect(elRawBulk).toHaveBeenCalledTimes(2);
    expect(elRawBulk.mock.calls[0][1].body).toHaveLength(10000);
    expect(elRawBulk.mock.calls[1][1].body).toHaveLength(2);
  });

  it('splits attachment writes at the configured bulk byte bound', async () => {
    const files = buildFiles(2).map((file) => ({
      ...file,
      file_data: 'a'.repeat(1_100_000),
    }));
    elRawBulk
      .mockResolvedValueOnce({
        errors: false,
        items: buildSuccessItems(1),
      })
      .mockResolvedValueOnce({
        errors: false,
        items: buildSuccessItems(1),
      });

    await elIndexFiles({} as any, {} as any, files);

    expect(elRawBulk).toHaveBeenCalledTimes(2);
    expect(elRawBulk.mock.calls[0][1].body).toHaveLength(2);
    expect(elRawBulk.mock.calls[1][1].body).toHaveLength(2);
  });

  it('retries only attachment-pipeline failures in one fallback bulk without file data', async () => {
    const files = buildFiles(3);
    elRawBulk
      .mockResolvedValueOnce({
        errors: true,
        items: [
          { index: { status: 201 } },
          { index: { error: { type: 'mapper_parsing_exception' }, status: 400 } },
          { index: { status: 201 } },
        ],
      })
      .mockResolvedValueOnce({
        errors: false,
        items: [{ index: { status: 201 } }],
      });

    await elIndexFiles({} as any, {} as any, files);

    expect(elRawBulk).toHaveBeenCalledTimes(2);
    expect(elRawBulk.mock.calls[0][1]).toEqual(expect.objectContaining({
      pipeline: 'attachment',
      refresh: true,
    }));
    expect(elRawBulk.mock.calls[1][1]).toEqual(expect.objectContaining({
      refresh: true,
    }));
    expect(elRawBulk.mock.calls[1][1]).not.toHaveProperty('pipeline');
    const fallbackBody = elRawBulk.mock.calls[1][1].body;
    expect(fallbackBody).toHaveLength(2);
    expect(fallbackBody[0]).toEqual({ index: { _id: 'doc-1', _index: INDEX_FILES } });
    expect(fallbackBody[1]).toEqual(expect.objectContaining({
      file_id: 'file-1',
      internal_id: 'doc-1',
    }));
    expect(fallbackBody[1]).not.toHaveProperty('file_data');
  });

  it('falls back the whole group without file data after an attachment bulk transport failure', async () => {
    const files = buildFiles(3);
    elRawBulk
      .mockRejectedValueOnce(new Error('attachment transport failed'))
      .mockResolvedValueOnce({
        errors: false,
        items: buildSuccessItems(3),
      });

    await elIndexFiles({} as any, {} as any, files);

    expect(elRawBulk).toHaveBeenCalledTimes(2);
    expect(elRawBulk.mock.calls[1][1]).not.toHaveProperty('pipeline');
    const fallbackBody = elRawBulk.mock.calls[1][1].body;
    expect(fallbackBody).toHaveLength(6);
    expect(fallbackBody[1]).not.toHaveProperty('file_data');
    expect(fallbackBody[3]).not.toHaveProperty('file_data');
    expect(fallbackBody[5]).not.toHaveProperty('file_data');
  });
});
