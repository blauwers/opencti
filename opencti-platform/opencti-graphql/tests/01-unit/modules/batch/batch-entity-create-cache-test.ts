import { describe, expect, it, vi } from 'vitest';
import { BatchMutationKind, executeBatchMutations } from '../../../../src/modules/batch/batch-executor';
import {
  buildBatchEntityCreateCacheKey,
  executeBatchCoalescedEntityCreate,
} from '../../../../src/modules/batch/batch-entity-create-cache';

describe('batch entity create cache', () => {
  it('canonicalizes equivalent plain JSON inputs', () => {
    expect(buildBatchEntityCreateCacheKey(
      'External-Reference',
      { source_name: 'mitre-attack', external_id: 'T1000' },
      { waitUntil: 'MATERIALIZED' },
    )).toBe(buildBatchEntityCreateCacheKey(
      'External-Reference',
      { external_id: 'T1000', source_name: 'mitre-attack' },
      { waitUntil: 'MATERIALIZED' },
    ));
  });

  it('skips inputs that can carry upload or other non-JSON state', () => {
    expect(buildBatchEntityCreateCacheKey(
      'External-Reference',
      { file: Promise.resolve({ filename: 'sample.txt' }) },
      {},
    )).toBeUndefined();
  });

  it('coalesces identical in-flight creates only inside the active write boundary', async () => {
    const execute = vi.fn(async () => {
      await new Promise((resolve) => setTimeout(resolve, 0));
      return { element: { internal_id: 'external-reference--one' } };
    });
    const input = { source_name: 'mitre-attack', external_id: 'T1000' };

    const outsideFirst = executeBatchCoalescedEntityCreate('External-Reference', input, {}, execute);
    const outsideSecond = executeBatchCoalescedEntityCreate('External-Reference', input, {}, execute);
    await Promise.all([outsideFirst, outsideSecond]);
    expect(execute).toHaveBeenCalledTimes(2);

    await executeBatchMutations([{
      kind: BatchMutationKind.CreateEntity,
      executeWrite: async () => {
        const first = executeBatchCoalescedEntityCreate('External-Reference', input, {}, execute);
        const second = executeBatchCoalescedEntityCreate('External-Reference', input, {}, execute);
        expect(first).toBe(second);
        await Promise.all([first, second]);
      },
    }]);

    expect(execute).toHaveBeenCalledTimes(3);
  });

  it('evicts settled creates so later operations can observe newer batch-local state', async () => {
    const execute = vi.fn(async () => ({ element: { internal_id: 'external-reference--one' } }));
    const input = { source_name: 'mitre-attack', external_id: 'T1000' };

    await executeBatchMutations([{
      kind: BatchMutationKind.CreateEntity,
      executeWrite: async () => {
        await executeBatchCoalescedEntityCreate('External-Reference', input, {}, execute);
        await executeBatchCoalescedEntityCreate('External-Reference', input, {}, execute);
      },
    }]);

    expect(execute).toHaveBeenCalledTimes(2);
  });

  it('evicts rejected creates so a later attempt can retry', async () => {
    const execute = vi.fn()
      .mockRejectedValueOnce(new Error('temporary failure'))
      .mockResolvedValueOnce({ element: { internal_id: 'external-reference--one' } });
    const input = { source_name: 'mitre-attack', external_id: 'T1000' };

    await executeBatchMutations([{
      kind: BatchMutationKind.CreateEntity,
      executeWrite: async () => {
        await expect(executeBatchCoalescedEntityCreate('External-Reference', input, {}, execute)).rejects.toThrow('temporary failure');
        await expect(executeBatchCoalescedEntityCreate('External-Reference', input, {}, execute)).resolves.toEqual({
          element: { internal_id: 'external-reference--one' },
        });
      },
    }]);

    expect(execute).toHaveBeenCalledTimes(2);
  });
});
