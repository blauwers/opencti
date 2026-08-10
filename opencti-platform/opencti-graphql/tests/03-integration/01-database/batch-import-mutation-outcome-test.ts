import { describe, expect, it, vi } from 'vitest';
import { v4 as uuidv4 } from 'uuid';
import { meterManager } from '../../../src/config/tracing';
import { elRawSearch } from '../../../src/database/engine';
import { deleteElementById, MutationOutcome, MutationSuppressionClass } from '../../../src/database/middleware';
import { internalLoadById } from '../../../src/database/middleware-loader';
import createSchema from '../../../src/graphql/schema';
import { computeLoaders } from '../../../src/http/httpAuthenticatedContext';
import { BatchMutationKind, BatchSideEffectKind } from '../../../src/modules/batch/batch-executor';
import { buildBatchGraphqlResultToken, executeBatchGraphqlOperations } from '../../../src/modules/batch/batch-operation-executor';
import { RELATION_RELATED_TO } from '../../../src/schema/stixCoreRelationship';
import { ENTITY_TYPE_MALWARE } from '../../../src/schema/stixDomainObject';
import { ENTITY_TYPE_LABEL } from '../../../src/schema/stixMetaObject';
import { executionContext } from '../../../src/utils/access';
import { ADMIN_USER } from '../../utils/testQuery';

type PersistedElement = {
  _id?: string;
  _index: string;
  entity_type: string;
  internal_id: string;
};

const loadDocumentVersion = async (entity: PersistedElement) => {
  const data = await elRawSearch(executionContext('batch-import-version-read', ADMIN_USER), ADMIN_USER, entity.entity_type, {
    index: entity._index,
    size: 1,
    version: true,
    body: {
      query: {
        ids: {
          values: [entity._id ?? entity.internal_id],
        },
      },
    },
  });
  return data.hits.hits[0]?._version;
};

const countCalls = (
  calls: Array<[Record<string, string>]>,
  expected: Record<string, string>,
) => calls.filter(([attributes]) => Object.entries(expected).every(([key, value]) => attributes[key] === value)).length;

describe('Batch import mutation outcome proof', () => {
  it('suppresses equivalent bundle re-import writes and reports mixed re-import outcomes independently', async () => {
    const schema = createSchema();
    const batchContext = executionContext(`batch-import-proof-${uuidv4()}`, ADMIN_USER);
    batchContext.batch = computeLoaders(batchContext, ADMIN_USER);
    const labelStixId = `label--${uuidv4()}`;
    const firstMalwareStixId = `malware--${uuidv4()}`;
    const secondMalwareStixId = `malware--${uuidv4()}`;
    const relationshipStixId = `relationship--${uuidv4()}`;
    const created = '2026-08-10T00:00:00.000Z';
    const modified = '2026-08-10T00:00:00.000Z';
    const labelValue = `batch-import-proof-${uuidv4()}`;
    const firstMalwareName = `Batch import proof malware one ${uuidv4()}`;
    const secondMalwareName = `Batch import proof malware two ${uuidv4()}`;
    const labelToken = buildBatchGraphqlResultToken(0, ['labelAdd', 'id']);
    const firstMalwareToken = buildBatchGraphqlResultToken(1, ['malwareAdd', 'id']);
    const secondMalwareToken = buildBatchGraphqlResultToken(2, ['malwareAdd', 'id']);
    const buildOperations = (firstDescription: string, firstModified = modified) => [
      {
        query: 'mutation LabelAdd($input: LabelAddInput!) { labelAdd(input: $input) { id } }',
        variables: JSON.stringify({
          input: {
            color: '#123456',
            stix_id: labelStixId,
            update: true,
            value: labelValue,
          },
        }),
        objectId: labelStixId,
        executionGroup: 0,
        executionPhase: 0,
      },
      {
        query: 'mutation MalwareAdd($input: MalwareAddInput!) { malwareAdd(input: $input) { id } }',
        variables: JSON.stringify({
          input: {
            created,
            description: firstDescription,
            modified: firstModified,
            name: firstMalwareName,
            objectLabel: [labelToken],
            stix_id: firstMalwareStixId,
            update: true,
          },
        }),
        objectId: firstMalwareStixId,
        executionGroup: 1,
        executionPhase: 1,
      },
      {
        query: 'mutation MalwareAdd($input: MalwareAddInput!) { malwareAdd(input: $input) { id } }',
        variables: JSON.stringify({
          input: {
            created,
            description: 'stable second malware',
            modified,
            name: secondMalwareName,
            stix_id: secondMalwareStixId,
            update: true,
          },
        }),
        objectId: secondMalwareStixId,
        executionGroup: 2,
        executionPhase: 1,
      },
      {
        query: 'mutation StixCoreRelationshipAdd($input: StixCoreRelationshipAddInput!) { stixCoreRelationshipAdd(input: $input) { id } }',
        variables: JSON.stringify({
          input: {
            created,
            fromId: firstMalwareToken,
            modified,
            relationship_type: RELATION_RELATED_TO,
            stix_id: relationshipStixId,
            toId: secondMalwareToken,
            update: true,
          },
        }),
        objectId: relationshipStixId,
        executionGroup: 3,
        executionPhase: 2,
      },
    ];
    const bundlePlan = {
      version: 1,
      executionPhases: [
        { phase: 0, objectIds: [labelStixId] },
        { phase: 1, objectIds: [firstMalwareStixId, secondMalwareStixId] },
        { phase: 2, objectIds: [relationshipStixId] },
      ],
    };
    const mutationOutcomeSpy = vi.spyOn(meterManager, 'mutationOutcome').mockImplementation(() => undefined);
    const mutationSuppressionSpy = vi.spyOn(meterManager, 'mutationSuppression').mockImplementation(() => undefined);
    let label: PersistedElement | undefined;
    let firstMalware: PersistedElement | undefined;
    let secondMalware: PersistedElement | undefined;
    let relationship: PersistedElement | undefined;

    try {
      const firstExecution = await executeBatchGraphqlOperations(schema, batchContext, buildOperations('stable first malware'), { bundlePlan });
      label = await internalLoadById(batchContext, ADMIN_USER, (firstExecution.results[0] as any).labelAdd.id) as PersistedElement;
      firstMalware = await internalLoadById(batchContext, ADMIN_USER, (firstExecution.results[1] as any).malwareAdd.id) as PersistedElement;
      secondMalware = await internalLoadById(batchContext, ADMIN_USER, (firstExecution.results[2] as any).malwareAdd.id) as PersistedElement;
      relationship = await internalLoadById(batchContext, ADMIN_USER, (firstExecution.results[3] as any).stixCoreRelationshipAdd.id) as PersistedElement;
      const versionsAfterCreate = await Promise.all([label, firstMalware, secondMalware, relationship].map((entity) => loadDocumentVersion(entity)));

      mutationOutcomeSpy.mockClear();
      mutationSuppressionSpy.mockClear();
      const equivalentExecution = await executeBatchGraphqlOperations(schema, batchContext, buildOperations('stable first malware'), { bundlePlan });
      const versionsAfterEquivalent = await Promise.all([label, firstMalware, secondMalware, relationship].map((entity) => loadDocumentVersion(entity)));

      expect(equivalentExecution.sideEffectKinds).not.toContain(BatchSideEffectKind.StreamPublication);
      expect(equivalentExecution.sideEffectKinds).not.toContain(BatchSideEffectKind.AutoEnrichment);
      expect(versionsAfterEquivalent).toEqual(versionsAfterCreate);
      expect(mutationOutcomeSpy).toHaveBeenCalledTimes(4);
      expect(countCalls(mutationOutcomeSpy.mock.calls as Array<[Record<string, string>]>, {
        mutation_kind: BatchMutationKind.CreateEntity,
        outcome: MutationOutcome.Unchanged,
      })).toBe(3);
      expect(countCalls(mutationOutcomeSpy.mock.calls as Array<[Record<string, string>]>, {
        mutation_kind: BatchMutationKind.CreateRelation,
        outcome: MutationOutcome.Unchanged,
      })).toBe(1);
      expect(countCalls(mutationSuppressionSpy.mock.calls as Array<[Record<string, string>]>, {
        mutation_kind: BatchMutationKind.CreateEntity,
        suppression: MutationSuppressionClass.ElasticWrite,
      })).toBe(3);
      expect(countCalls(mutationSuppressionSpy.mock.calls as Array<[Record<string, string>]>, {
        mutation_kind: BatchMutationKind.CreateRelation,
        suppression: MutationSuppressionClass.ElasticWrite,
      })).toBe(1);

      mutationOutcomeSpy.mockClear();
      mutationSuppressionSpy.mockClear();
      const mixedExecution = await executeBatchGraphqlOperations(
        schema,
        batchContext,
        buildOperations('changed first malware', '2026-08-10T00:01:00.000Z'),
        { bundlePlan },
      );
      const versionsAfterMixed = await Promise.all([label, firstMalware, secondMalware, relationship].map((entity) => loadDocumentVersion(entity)));

      expect(mixedExecution.sideEffectKinds).toContain(BatchSideEffectKind.StreamPublication);
      expect(versionsAfterMixed[0]).toBe(versionsAfterEquivalent[0]);
      expect(versionsAfterMixed[1]).toBe(versionsAfterEquivalent[1] + 1);
      expect(versionsAfterMixed[2]).toBe(versionsAfterEquivalent[2]);
      expect(versionsAfterMixed[3]).toBe(versionsAfterEquivalent[3]);
      expect(mutationOutcomeSpy).toHaveBeenCalledTimes(4);
      expect(countCalls(mutationOutcomeSpy.mock.calls as Array<[Record<string, string>]>, {
        mutation_kind: BatchMutationKind.CreateEntity,
        outcome: MutationOutcome.Updated,
      })).toBe(1);
      expect(countCalls(mutationOutcomeSpy.mock.calls as Array<[Record<string, string>]>, {
        mutation_kind: BatchMutationKind.CreateEntity,
        outcome: MutationOutcome.Unchanged,
      })).toBe(2);
      expect(countCalls(mutationOutcomeSpy.mock.calls as Array<[Record<string, string>]>, {
        mutation_kind: BatchMutationKind.CreateRelation,
        outcome: MutationOutcome.Unchanged,
      })).toBe(1);
    } finally {
      mutationOutcomeSpy.mockRestore();
      mutationSuppressionSpy.mockRestore();
      if (relationship) {
        await deleteElementById(batchContext, ADMIN_USER, relationship.internal_id, relationship.entity_type, { forceDelete: true });
      }
      if (firstMalware) {
        await deleteElementById(batchContext, ADMIN_USER, firstMalware.internal_id, ENTITY_TYPE_MALWARE, { forceDelete: true });
      }
      if (secondMalware) {
        await deleteElementById(batchContext, ADMIN_USER, secondMalware.internal_id, ENTITY_TYPE_MALWARE, { forceDelete: true });
      }
      if (label) {
        await deleteElementById(batchContext, ADMIN_USER, label.internal_id, ENTITY_TYPE_LABEL, { forceDelete: true });
      }
    }
  });
});
