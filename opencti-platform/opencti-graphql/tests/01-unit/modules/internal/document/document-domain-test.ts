import { beforeEach, describe, expect, it, vi } from 'vitest';
import { FROM_START_STR } from '../../../../../src/utils/format';

const elSearchFiles = vi.fn();

vi.mock('../../../../../src/database/file-search', () => ({
  elSearchFiles,
}));

const { getIndexFromDate } = await import('../../../../../src/modules/internal/document/document-domain');

describe('document index watermark', () => {
  beforeEach(() => {
    vi.clearAllMocks();
  });

  it('returns the newest indexed file upload timestamp from the raw file search path', async () => {
    elSearchFiles.mockResolvedValue([{ uploaded_at: new Date('2026-08-12T00:01:02.000Z') }]);

    await expect(getIndexFromDate({} as any)).resolves.toBe('2026-08-12T00:01:02.000Z');
    expect(elSearchFiles).toHaveBeenCalledWith(expect.anything(), expect.anything(), expect.objectContaining({
      connectionFormat: false,
      first: 1,
      highlight: false,
      orderBy: 'uploaded_at',
      orderMode: 'desc',
    }));
  });

  it('returns the start sentinel when no indexed file exists', async () => {
    elSearchFiles.mockResolvedValue([]);

    await expect(getIndexFromDate({} as any)).resolves.toBe(FROM_START_STR);
  });
});
