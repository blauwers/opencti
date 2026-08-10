import { describe, expect, it } from 'vitest';
import { v4 as uuidv4 } from 'uuid';
import { createEntity, MutationOutcome, type MutationResult, updateAttribute } from '../../../src/database/middleware';
import { labelDelete } from '../../../src/domain/label';
import { ENTITY_TYPE_LABEL } from '../../../src/schema/stixMetaObject';
import { ADMIN_USER, testContext } from '../../utils/testQuery';

describe('Middleware mutation outcome contract', () => {
  it('distinguishes create, update, and unchanged results without relying on event presence', async () => {
    const input = {
      color: '#123456',
      value: `mutation-outcome-${uuidv4()}`,
    };
    const created = await createEntity(testContext, ADMIN_USER, input, ENTITY_TYPE_LABEL, { complete: true }) as MutationResult<any>;

    try {
      expect(created.outcome).toBe(MutationOutcome.Created);
      expect(created.isCreation).toBe(true);

      const unchangedUpsert = await createEntity(testContext, ADMIN_USER, input, ENTITY_TYPE_LABEL, { complete: true }) as MutationResult<any>;
      expect(unchangedUpsert.outcome).toBe(MutationOutcome.Unchanged);
      expect(unchangedUpsert.isCreation).toBe(false);
      expect(unchangedUpsert.event).toBeNull();

      const updated = await updateAttribute(
        testContext,
        ADMIN_USER,
        created.element.internal_id,
        ENTITY_TYPE_LABEL,
        [{ key: 'color', value: ['#654321'] }],
      );
      expect(updated.outcome).toBe(MutationOutcome.Updated);
      expect(updated.isCreation).toBe(false);

      const unchangedUpdate = await updateAttribute(
        testContext,
        ADMIN_USER,
        created.element.internal_id,
        ENTITY_TYPE_LABEL,
        [{ key: 'color', value: ['#654321'] }],
      );
      expect(unchangedUpdate.outcome).toBe(MutationOutcome.Unchanged);
      expect(unchangedUpdate.isCreation).toBe(false);
      expect(unchangedUpdate.event).toBeNull();
    } finally {
      await labelDelete(testContext, ADMIN_USER, created.element.internal_id);
    }
  });
});
