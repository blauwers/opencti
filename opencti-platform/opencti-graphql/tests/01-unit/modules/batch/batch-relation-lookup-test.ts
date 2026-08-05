import { describe, expect, it } from 'vitest';
import { BatchMutationKind, executeBatchMutations } from '../../../../src/modules/batch/batch-executor';
import { hasBatchCreatedRelationEndpoint, registerBatchCreatedEntity } from '../../../../src/modules/batch/batch-relation-lookup';

describe('batch relation lookup state', () => {
  it('tracks newly created entity identifiers only inside the active batch write boundary', async () => {
    const entity = {
      internal_id: 'indicator--internal',
      standard_id: 'indicator--standard',
      entity_type: 'Indicator',
    } as any;
    const input = {
      from: entity,
      to: {
        internal_id: 'identity--internal',
        standard_id: 'identity--standard',
        entity_type: 'Identity',
      },
    } as any;

    expect(hasBatchCreatedRelationEndpoint(input)).toBe(false);
    registerBatchCreatedEntity(entity);
    expect(hasBatchCreatedRelationEndpoint(input)).toBe(false);

    await executeBatchMutations([{
      kind: BatchMutationKind.CreateEntity,
      executeWrite: async () => {
        registerBatchCreatedEntity(entity);
        expect(hasBatchCreatedRelationEndpoint(input)).toBe(true);
      },
    }]);

    expect(hasBatchCreatedRelationEndpoint(input)).toBe(false);
  });

  it('matches any identifier carried by a newly created endpoint', async () => {
    const entity = {
      internal_id: 'indicator--internal',
      standard_id: 'indicator--standard',
      entity_type: 'Indicator',
      x_opencti_stix_ids: ['indicator--stix-alias'],
    } as any;

    await executeBatchMutations([{
      kind: BatchMutationKind.CreateEntity,
      executeWrite: async () => {
        registerBatchCreatedEntity(entity);
        expect(hasBatchCreatedRelationEndpoint({
          from: {
            internal_id: 'indicator--other-internal',
            standard_id: 'indicator--stix-alias',
            entity_type: 'Indicator',
          } as any,
        })).toBe(true);
      },
    }]);
  });
});
