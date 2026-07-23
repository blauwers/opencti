import { beforeEach, describe, expect, it, vi } from 'vitest';
import { BULK_TIMEOUT, elBulk } from '../../../src/database/engine';
import { createdWorksIndexBatchLoader } from '../../../src/domain/work';
import { INDEX_HISTORY } from '../../../src/database/utils';

vi.mock('../../../src/database/engine', async (importOriginal) => {
  const actual = await importOriginal<typeof import('../../../src/database/engine')>();
  return {
    ...actual,
    elBulk: vi.fn().mockResolvedValue({}),
  };
});

describe('createdWorksIndexBatchLoader', () => {
  beforeEach(() => {
    vi.clearAllMocks();
  });

  it('batches same-tick work indexes into one refreshed bulk write', async () => {
    const context = {} as never;
    const loader = createdWorksIndexBatchLoader(context);
    const work1 = { internal_id: 'work--1', entity_type: 'Work', _index: 'ignored', name: 'One' };
    const work2 = { internal_id: 'work--2', entity_type: 'Work', _index: 'ignored', name: 'Two' };

    await Promise.all([loader.load(work1), loader.load(work2)]);

    expect(elBulk).toHaveBeenCalledTimes(1);
    expect(elBulk).toHaveBeenCalledWith(context, {
      refresh: true,
      timeout: BULK_TIMEOUT,
      body: [
        { index: { _index: INDEX_HISTORY, _id: 'work--1' } },
        { internal_id: 'work--1', entity_type: 'Work', name: 'One' },
        { index: { _index: INDEX_HISTORY, _id: 'work--2' } },
        { internal_id: 'work--2', entity_type: 'Work', name: 'Two' },
      ],
    });
  });
});
