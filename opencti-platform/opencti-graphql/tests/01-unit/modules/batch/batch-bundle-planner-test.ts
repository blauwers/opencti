import { describe, expect, it } from 'vitest';
import { planStixBundleObjects } from '../../../../src/modules/batch/batch-bundle-planner';
import { STIX_EXT_OCTI } from '../../../../src/types/stix-2-1-extensions';

describe('batch bundle planner', () => {
  it('builds backend execution phases from bundle-local references', () => {
    const identityId = 'identity--11111111-1111-4111-8111-111111111111';
    const indicatorId = 'indicator--22222222-2222-4222-8222-222222222222';
    const relationshipId = 'relationship--33333333-3333-4333-8333-333333333333';
    const plan = planStixBundleObjects([
      {
        type: 'relationship',
        id: relationshipId,
        relationship_type: 'indicates',
        source_ref: indicatorId,
        target_ref: identityId,
      },
      {
        type: 'indicator',
        id: indicatorId,
        created_by_ref: identityId,
      },
      {
        type: 'identity',
        id: identityId,
      },
    ]);

    expect(plan.plannedObjectCount).toBe(3);
    expect(plan.executionPhases).toEqual([
      { phase: 0, objectIds: [identityId] },
      { phase: 1, objectIds: [indicatorId] },
      { phase: 2, objectIds: [relationshipId] },
    ]);
    expect(plan.orderedObjectIds).toEqual([identityId, indicatorId, relationshipId]);
    expect(plan.objects).toEqual([
      expect.objectContaining({ id: relationshipId, dependencyIds: [indicatorId, identityId], executionPhase: 2 }),
      expect.objectContaining({ id: indicatorId, dependencyIds: [identityId], executionPhase: 1 }),
      expect.objectContaining({ id: identityId, dependencyIds: [], executionPhase: 0 }),
    ]);
  });

  it('resolves internal OpenCTI ids as aliases for bundle-local dependencies', () => {
    const identityId = 'identity--11111111-1111-4111-8111-111111111111';
    const internalId = '11111111-1111-4111-8111-111111111111';
    const indicatorId = 'indicator--22222222-2222-4222-8222-222222222222';
    const plan = planStixBundleObjects([
      {
        type: 'indicator',
        id: indicatorId,
        created_by_ref: internalId,
      },
      {
        type: 'identity',
        id: identityId,
        extensions: {
          [STIX_EXT_OCTI]: { id: internalId },
        },
      },
    ]);

    expect(plan.objects.find((object) => object.id === indicatorId)?.dependencyIds).toEqual([identityId]);
    expect(plan.executionPhases).toEqual([
      { phase: 0, objectIds: [identityId] },
      { phase: 1, objectIds: [indicatorId] },
    ]);
  });

  it('marks missing required relationship references incompatible when cleanup is requested', () => {
    const relationshipId = 'relationship--33333333-3333-4333-8333-333333333333';
    const plan = planStixBundleObjects([
      {
        type: 'relationship',
        id: relationshipId,
        relationship_type: 'indicates',
        source_ref: 'indicator--22222222-2222-4222-8222-222222222222',
        target_ref: 'malware--44444444-4444-4444-8444-444444444444',
      },
    ], { cleanupInconsistentBundle: true });

    expect(plan.plannedObjectCount).toBe(0);
    expect(plan.incompatibleObjectIds).toEqual([relationshipId]);
    expect(plan.executionPhases).toEqual([]);
  });

  it('drops unsupported STIX ids and relations that point to them', () => {
    const unsupportedId = 'x-mitre-detection-strategy--11111111-1111-4111-8111-111111111111';
    const attackPatternId = 'attack-pattern--22222222-2222-4222-8222-222222222222';
    const relationshipId = 'relationship--33333333-3333-4333-8333-333333333333';
    const plan = planStixBundleObjects([
      {
        type: 'x-mitre-detection-strategy',
        id: unsupportedId,
      },
      {
        type: 'attack-pattern',
        id: attackPatternId,
      },
      {
        type: 'relationship',
        id: relationshipId,
        relationship_type: 'detects',
        source_ref: unsupportedId,
        target_ref: attackPatternId,
      },
    ]);

    expect(plan.plannedObjectCount).toBe(1);
    expect(plan.incompatibleObjectIds).toEqual([unsupportedId, relationshipId]);
    expect(plan.executionPhases).toEqual([{ phase: 0, objectIds: [attackPatternId] }]);
  });

  it('records backend-owned reference cleanup patches without rewriting the source bundle', () => {
    const indicatorId = 'indicator--22222222-2222-4222-8222-222222222222';
    const source = {
      type: 'indicator',
      id: indicatorId,
      created_by_ref: 'identity--11111111-1111-4111-8111-111111111111',
      object_marking_refs: [
        'marking-definition--33333333-3333-4333-8333-333333333333',
        'marking-definition--33333333-3333-4333-8333-333333333333',
      ],
    };
    const plan = planStixBundleObjects([source], { cleanupInconsistentBundle: true });

    expect(source.created_by_ref).toBe('identity--11111111-1111-4111-8111-111111111111');
    expect(source.object_marking_refs).toHaveLength(2);
    expect(plan.objects[0]).toMatchObject({
      id: indicatorId,
      normalization: {
        referenceValues: {
          created_by_ref: null,
          object_marking_refs: [],
        },
      },
    });
  });

  it('records embedded reference deduplication as compact retained indexes', () => {
    const indicatorId = 'indicator--22222222-2222-4222-8222-222222222222';
    const plan = planStixBundleObjects([{
      type: 'indicator',
      id: indicatorId,
      external_references: [
        { source_name: 'feed', external_id: '1' },
        { source_name: 'feed', external_id: '1' },
        { url: 'https://example.test/a' },
      ],
      kill_chain_phases: [
        { kill_chain_name: 'chain', phase_name: 'phase' },
        { kill_chain_name: 'chain', phase_name: 'phase' },
      ],
    }]);

    expect(plan.objects[0]).toMatchObject({
      id: indicatorId,
      normalization: {
        externalReferenceIndexes: [0, 2],
        killChainPhaseIndexes: [0],
      },
    });
  });

  it('drops blank external reference identities and falls back to source plus external id', () => {
    const indicatorId = 'indicator--22222222-2222-4222-8222-222222222222';
    const plan = planStixBundleObjects([{
      type: 'indicator',
      id: indicatorId,
      external_references: [
        { source_name: 'Unknown', url: '' },
        { source_name: 'feed', external_id: '1', url: '' },
        { url: 'https://example.test/a' },
      ],
    }]);

    expect(plan.objects[0]).toMatchObject({
      id: indicatorId,
      normalization: {
        externalReferenceIndexes: [1, 2],
      },
    });
  });

  it('keeps missing external references outside the local dependency graph by default', () => {
    const indicatorId = 'indicator--22222222-2222-4222-8222-222222222222';
    const plan = planStixBundleObjects([
      {
        type: 'indicator',
        id: indicatorId,
        created_by_ref: 'identity--11111111-1111-4111-8111-111111111111',
      },
    ]);

    expect(plan.plannedObjectCount).toBe(1);
    expect(plan.objects[0]).toMatchObject({
      id: indicatorId,
      dependencyIds: [],
      executionPhase: 0,
    });
  });

  it('deduplicates repeated object ids while retaining one planned object', () => {
    const identityId = 'identity--11111111-1111-4111-8111-111111111111';
    const plan = planStixBundleObjects([
      { type: 'identity', id: identityId, name: 'first' },
      { type: 'identity', id: identityId, name: 'second' },
    ]);

    expect(plan.objectCount).toBe(2);
    expect(plan.plannedObjectCount).toBe(1);
    expect(plan.ignoredObjectCount).toBe(1);
    expect(plan.executionPhases).toEqual([{ phase: 0, objectIds: [identityId] }]);
  });

  it('breaks cyclic bundle-local references without producing an unbounded phase walk', () => {
    const firstId = 'report--11111111-1111-4111-8111-111111111111';
    const secondId = 'report--22222222-2222-4222-8222-222222222222';
    const plan = planStixBundleObjects([
      { type: 'report', id: firstId, object_refs: [secondId] },
      { type: 'report', id: secondId, object_refs: [firstId] },
    ]);

    expect(plan.plannedObjectCount).toBe(2);
    expect(plan.executionPhases).toEqual([
      { phase: 0, objectIds: [secondId] },
      { phase: 1, objectIds: [firstId] },
    ]);
    expect(plan.objects.find((object) => object.id === secondId)).toMatchObject({
      normalization: {
        referenceValues: {
          object_refs: [],
        },
      },
    });
  });
});
