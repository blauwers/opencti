import { afterAll, describe, expect, it } from 'vitest';
import { v4 as uuidv4 } from 'uuid';
import { elDeleteInstances, elUpdateElement } from '../../../src/database/engine';
import { deleteElementById } from '../../../src/database/middleware';
import { internalLoadById } from '../../../src/database/middleware-loader';
import { addMalware } from '../../../src/domain/malware';
import { BatchMutationKind, executeBatchMutations } from '../../../src/modules/batch/batch-executor';
import { ENTITY_TYPE_MALWARE } from '../../../src/schema/stixDomainObject';
import { ADMIN_USER, testContext } from '../../utils/testQuery';

describe('batch engine writes', () => {
  let malware: any;
  let deletedMalware: any;

  afterAll(async () => {
    if (malware) {
      await deleteElementById(testContext, ADMIN_USER, malware.internal_id, ENTITY_TYPE_MALWARE, { forceDelete: true });
    }
    if (deletedMalware) {
      const existing = await internalLoadById(testContext, ADMIN_USER, deletedMalware.internal_id);
      if (existing) {
        await deleteElementById(testContext, ADMIN_USER, deletedMalware.internal_id, ENTITY_TYPE_MALWARE, { forceDelete: true });
      }
    }
  });

  it('buffers update writes and exposes them to later mutations in the same batch', async () => {
    malware = await addMalware(testContext, ADMIN_USER, { name: `Batch update ${uuidv4()}` });

    await executeBatchMutations([
      {
        kind: BatchMutationKind.UpdateAttribute,
        executeWrite: async () => {
          await elUpdateElement(testContext, ADMIN_USER, {
            _index: malware?._index,
            _id: malware?._id,
            internal_id: malware?.internal_id,
            entity_type: malware?.entity_type,
            description: 'first buffered update',
          } as any);
          return null;
        },
      },
      {
        kind: BatchMutationKind.UpdateAttribute,
        executeWrite: async () => {
          const buffered = await internalLoadById(testContext, ADMIN_USER, malware?.internal_id) as any;
          expect(buffered?.description).toBe('first buffered update');
          await elUpdateElement(testContext, ADMIN_USER, {
            _index: malware?._index,
            _id: malware?._id,
            internal_id: malware?.internal_id,
            entity_type: malware?.entity_type,
            confidence: 88,
          } as any);
          return null;
        },
      },
    ]);

    const committed = await internalLoadById(testContext, ADMIN_USER, malware.internal_id) as any;
    expect(committed?.name).toBe(malware.name);
    expect(committed?.description).toBe('first buffered update');
    expect(committed?.confidence).toBe(88);
  });

  it('buffers deletes and hides deleted documents from later mutations in the same batch', async () => {
    deletedMalware = await addMalware(testContext, ADMIN_USER, { name: `Batch delete ${uuidv4()}` });

    await executeBatchMutations([
      {
        kind: BatchMutationKind.UpdateAttribute,
        executeWrite: async () => {
          await elDeleteInstances(testContext, [deletedMalware]);
          return null;
        },
      },
      {
        kind: BatchMutationKind.UpdateAttribute,
        executeWrite: async () => {
          const buffered = await internalLoadById(testContext, ADMIN_USER, deletedMalware.internal_id);
          expect(buffered).toBeUndefined();
          return null;
        },
      },
    ]);

    await expect(internalLoadById(testContext, ADMIN_USER, deletedMalware.internal_id)).resolves.toBeUndefined();
  });
});
