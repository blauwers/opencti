import { describe, expect, it, vi } from 'vitest';
import { v4 as uuidv4 } from 'uuid';
import { createEntity, MutationIntent, MutationOutcome, MutationSuppressionClass, type MutationResult, updateAttribute } from '../../../src/database/middleware';
import { internalLoadById } from '../../../src/database/middleware-loader';
import { labelDelete } from '../../../src/domain/label';
import { ENTITY_TYPE_LABEL } from '../../../src/schema/stixMetaObject';
import { meterManager } from '../../../src/config/tracing';
import { BatchMutationKind } from '../../../src/modules/batch/batch-executor';
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

  it('suppresses default freshness-only writes while preserving explicit touch intent', async () => {
    const input = {
      color: '#123456',
      value: `mutation-touch-${uuidv4()}`,
    };
    const created = await createEntity(testContext, ADMIN_USER, input, ENTITY_TYPE_LABEL, { complete: true }) as MutationResult<any>;
    const mutationOutcomeSpy = vi.spyOn(meterManager, 'mutationOutcome').mockImplementation(() => undefined);
    const mutationSuppressionSpy = vi.spyOn(meterManager, 'mutationSuppression').mockImplementation(() => undefined);

    try {
      const before = await internalLoadById(testContext, ADMIN_USER, created.element.internal_id) as any;
      const defaultFreshness = new Date(Date.now() + 60_000).toISOString();

      mutationOutcomeSpy.mockClear();
      mutationSuppressionSpy.mockClear();
      const unchanged = await updateAttribute(
        testContext,
        ADMIN_USER,
        created.element.internal_id,
        ENTITY_TYPE_LABEL,
        [{ key: 'modified', value: [defaultFreshness] }],
      );
      const afterUnchanged = await internalLoadById(testContext, ADMIN_USER, created.element.internal_id) as any;

      expect(unchanged.outcome).toBe(MutationOutcome.Unchanged);
      expect(unchanged.event).toBeNull();
      expect(afterUnchanged.modified).toEqual(before.modified);
      expect(afterUnchanged.updated_at).toEqual(before.updated_at);
      expect(afterUnchanged.refreshed_at).toEqual(before.refreshed_at);
      expect(mutationOutcomeSpy).toHaveBeenCalledWith({
        mutation_kind: BatchMutationKind.UpdateAttribute,
        outcome: MutationOutcome.Unchanged,
      });
      expect(mutationSuppressionSpy.mock.calls.map(([attributes]) => attributes)).toEqual([
        { mutation_kind: BatchMutationKind.UpdateAttribute, suppression: MutationSuppressionClass.ElasticWrite },
        { mutation_kind: BatchMutationKind.UpdateAttribute, suppression: MutationSuppressionClass.StreamEvent },
        { mutation_kind: BatchMutationKind.UpdateAttribute, suppression: MutationSuppressionClass.AutoEnrichment },
      ]);

      const explicitTouch = new Date(Date.now() + 120_000).toISOString();
      mutationOutcomeSpy.mockClear();
      mutationSuppressionSpy.mockClear();
      const touched = await updateAttribute(
        testContext,
        ADMIN_USER,
        created.element.internal_id,
        ENTITY_TYPE_LABEL,
        [{ key: 'modified', value: [explicitTouch] }],
        { mutationIntent: MutationIntent.Touch },
      );
      const afterTouch = await internalLoadById(testContext, ADMIN_USER, created.element.internal_id) as any;

      expect(touched.outcome).toBe(MutationOutcome.Touched);
      expect(touched.event).toBeNull();
      expect(new Date(afterTouch.modified).toISOString()).toBe(explicitTouch);
      expect(mutationOutcomeSpy).toHaveBeenCalledWith({
        mutation_kind: BatchMutationKind.UpdateAttribute,
        outcome: MutationOutcome.Touched,
      });
      expect(mutationSuppressionSpy.mock.calls.map(([attributes]) => attributes)).toEqual([
        { mutation_kind: BatchMutationKind.UpdateAttribute, suppression: MutationSuppressionClass.StreamEvent },
        { mutation_kind: BatchMutationKind.UpdateAttribute, suppression: MutationSuppressionClass.AutoEnrichment },
      ]);
    } finally {
      mutationOutcomeSpy.mockRestore();
      mutationSuppressionSpy.mockRestore();
      await labelDelete(testContext, ADMIN_USER, created.element.internal_id);
    }
  });
});
