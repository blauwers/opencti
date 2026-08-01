import { afterAll, describe, expect, it } from 'vitest';
import { v4 as uuidv4 } from 'uuid';
import { copyLiveElementToDraft, elDeleteElements, elDeleteInstances, elLoadById, elRemoveDraftIdFromElements, elReplace, elUpdate, elUpdateElement, elUpdateEntityConnections, elUpdateRelationConnections } from '../../../src/database/engine';
import { deleteElementById } from '../../../src/database/middleware';
import { fullEntitiesList, fullRelationsList, internalLoadById } from '../../../src/database/middleware-loader';
import { addMalware } from '../../../src/domain/malware';
import { addStixCoreRelationship } from '../../../src/domain/stixCoreRelationship';
import { BatchMutationKind, executeBatchMutations } from '../../../src/modules/batch/batch-executor';
import { INDEX_DELETED_OBJECTS, INDEX_DRAFT_OBJECTS } from '../../../src/database/utils';
import { confirmDelete } from '../../../src/modules/deleteOperation/deleteOperation-domain';
import { ENTITY_TYPE_DELETE_OPERATION } from '../../../src/modules/deleteOperation/deleteOperation-types';
import { updatePirInformationOnEntity } from '../../../src/modules/pir/pir-utils';
import { FilterMode, FilterOperator } from '../../../src/generated/graphql';
import { buildRefRelationKey } from '../../../src/schema/general';
import { RELATION_RELATED_TO } from '../../../src/schema/stixCoreRelationship';
import { ENTITY_TYPE_MALWARE } from '../../../src/schema/stixDomainObject';
import { ADMIN_USER, testContext } from '../../utils/testQuery';

describe('batch engine writes', () => {
  let malware: any;
  let deletedMalware: any;
  const cleanupMalwares: any[] = [];
  const cleanupSoftDeletedMalwareIds: string[] = [];

  const findDeleteOperationsForMainEntity = (mainEntityId: string) => {
    return fullEntitiesList<any>(testContext, ADMIN_USER, [ENTITY_TYPE_DELETE_OPERATION], {
      filters: {
        mode: FilterMode.And,
        filters: [{ key: ['main_entity_id'], values: [mainEntityId], operator: FilterOperator.Eq }],
        filterGroups: [],
      },
    });
  };

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
    for (let index = 0; index < cleanupMalwares.length; index += 1) {
      const cleanupMalware = cleanupMalwares[index];
      const existing = await internalLoadById(testContext, ADMIN_USER, cleanupMalware.internal_id);
      if (existing) {
        await deleteElementById(testContext, ADMIN_USER, cleanupMalware.internal_id, ENTITY_TYPE_MALWARE, { forceDelete: true });
      }
    }
    for (let index = 0; index < cleanupSoftDeletedMalwareIds.length; index += 1) {
      const deleteOperations = await findDeleteOperationsForMainEntity(cleanupSoftDeletedMalwareIds[index]);
      for (let operationIndex = 0; operationIndex < deleteOperations.length; operationIndex += 1) {
        await confirmDelete(testContext, ADMIN_USER, deleteOperations[operationIndex].internal_id);
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

  it('buffers direct document and replace updates inside the batch boundary', async () => {
    const rawUpdateMalware = await addMalware(testContext, ADMIN_USER, { name: `Batch raw update ${uuidv4()}` });
    cleanupMalwares.push(rawUpdateMalware);

    await executeBatchMutations([
      {
        kind: BatchMutationKind.UpdateAttribute,
        executeWrite: async () => {
          await elUpdate(testContext, rawUpdateMalware._index, rawUpdateMalware._id ?? rawUpdateMalware.internal_id, {
            doc: { description: 'raw doc update' },
          });
          return null;
        },
      },
      {
        kind: BatchMutationKind.UpdateAttribute,
        executeWrite: async () => {
          const buffered = await internalLoadById(testContext, ADMIN_USER, rawUpdateMalware.internal_id) as any;
          expect(buffered.description).toBe('raw doc update');
          await elReplace(testContext, rawUpdateMalware._index, rawUpdateMalware._id ?? rawUpdateMalware.internal_id, {
            doc: { confidence: 64 },
          });
          return null;
        },
      },
      {
        kind: BatchMutationKind.UpdateAttribute,
        executeWrite: async () => {
          const buffered = await internalLoadById(testContext, ADMIN_USER, rawUpdateMalware.internal_id) as any;
          expect(buffered.description).toBe('raw doc update');
          expect(buffered.confidence).toBe(64);
          return null;
        },
      },
    ]);

    const committed = await internalLoadById(testContext, ADMIN_USER, rawUpdateMalware.internal_id) as any;
    expect(committed.description).toBe('raw doc update');
    expect(committed.confidence).toBe(64);
  });

  it('buffers connection rewrites and exposes them to later mutations in the same batch', async () => {
    const source = await addMalware(testContext, ADMIN_USER, { name: `Batch source ${uuidv4()}` });
    const originalTarget = await addMalware(testContext, ADMIN_USER, { name: `Batch original ${uuidv4()}` });
    const replacementTarget = await addMalware(testContext, ADMIN_USER, { name: `Batch replacement ${uuidv4()}` });
    cleanupMalwares.push(source, originalTarget, replacementTarget);
    const relation = await addStixCoreRelationship(testContext, ADMIN_USER, {
      relationship_type: RELATION_RELATED_TO,
      fromId: source.id,
      toId: originalTarget.id,
    });
    await elReplace(testContext, relation._index, relation._id ?? relation.internal_id, {
      doc: {
        connections: [
          {
            internal_id: source.internal_id,
            role: `${RELATION_RELATED_TO}_from`,
            types: [...source.parent_types, source.entity_type],
            name: source.name,
          },
          {
            internal_id: originalTarget.internal_id,
            role: `${RELATION_RELATED_TO}_to`,
            types: [...originalTarget.parent_types, originalTarget.entity_type],
            name: originalTarget.name,
          },
        ],
      },
    });
    const relationKey = buildRefRelationKey(RELATION_RELATED_TO);

    await executeBatchMutations([
      {
        kind: BatchMutationKind.UpdateAttribute,
        executeWrite: async () => {
          await elUpdateRelationConnections(testContext, [{
            _index: relation._index,
            id: relation.internal_id,
            toReplace: originalTarget.internal_id,
            data: { internal_id: replacementTarget.internal_id, name: replacementTarget.name },
          }]);
          await elUpdateEntityConnections(testContext, [{
            _index: source._index,
            id: source.internal_id,
            toReplace: originalTarget.internal_id,
            relationType: RELATION_RELATED_TO,
            data: { internal_id: replacementTarget.internal_id },
          }]);
          return null;
        },
      },
      {
        kind: BatchMutationKind.UpdateAttribute,
        executeWrite: async () => {
          const bufferedRelation = await internalLoadById(testContext, ADMIN_USER, relation.internal_id) as any;
          const bufferedSource = await internalLoadById(testContext, ADMIN_USER, source.internal_id) as any;
          expect(bufferedRelation.toId).toBe(replacementTarget.internal_id);
          expect(bufferedRelation.toName).toBe(replacementTarget.name);
          expect(bufferedSource[relationKey]).toContain(replacementTarget.internal_id);
          expect(bufferedSource[relationKey]).not.toContain(originalTarget.internal_id);
          return null;
        },
      },
    ]);

    const committedRelation = await internalLoadById(testContext, ADMIN_USER, relation.internal_id) as any;
    const committedSource = await internalLoadById(testContext, ADMIN_USER, source.internal_id) as any;
    expect(committedRelation.toId).toBe(replacementTarget.internal_id);
    expect(committedRelation.toName).toBe(replacementTarget.name);
    expect(committedSource[relationKey]).toContain(replacementTarget.internal_id);
    expect(committedSource[relationKey]).not.toContain(originalTarget.internal_id);
  });

  it('buffers relation cleanup updates before deleting linked elements', async () => {
    const source = await addMalware(testContext, ADMIN_USER, { name: `Batch cleanup source ${uuidv4()}` });
    const target = await addMalware(testContext, ADMIN_USER, { name: `Batch cleanup target ${uuidv4()}` });
    cleanupMalwares.push(source, target);
    await addStixCoreRelationship(testContext, ADMIN_USER, {
      relationship_type: RELATION_RELATED_TO,
      fromId: source.id,
      toId: target.id,
    });
    const relationKey = buildRefRelationKey(RELATION_RELATED_TO);

    await executeBatchMutations([
      {
        kind: BatchMutationKind.UpdateAttribute,
        executeWrite: async () => {
          await elDeleteElements(testContext, ADMIN_USER, [target], { forceDelete: true });
          return null;
        },
      },
      {
        kind: BatchMutationKind.UpdateAttribute,
        executeWrite: async () => {
          const bufferedSource = await internalLoadById(testContext, ADMIN_USER, source.internal_id) as any;
          expect(bufferedSource[relationKey]).not.toContain(target.internal_id);
          return null;
        },
      },
    ]);

    const committedSource = await internalLoadById(testContext, ADMIN_USER, source.internal_id) as any;
    expect(committedSource[relationKey]).not.toContain(target.internal_id);
    await expect(internalLoadById(testContext, ADMIN_USER, target.internal_id)).resolves.toBeUndefined();
  });

  it('keeps nested entity and relation creation in one backend batch scope', async () => {
    let source: any;
    let target: any;
    let relation: any;

    await executeBatchMutations([
      {
        kind: BatchMutationKind.CreateEntity,
        executeWrite: async () => {
          source = await addMalware(testContext, ADMIN_USER, { name: `Batch nested source ${uuidv4()}` });
          cleanupMalwares.push(source);
          return source;
        },
      },
      {
        kind: BatchMutationKind.CreateEntity,
        executeWrite: async () => {
          target = await addMalware(testContext, ADMIN_USER, { name: `Batch nested target ${uuidv4()}` });
          cleanupMalwares.push(target);
          return target;
        },
      },
      {
        kind: BatchMutationKind.CreateRelation,
        executeWrite: async () => {
          relation = await addStixCoreRelationship(testContext, ADMIN_USER, {
            relationship_type: RELATION_RELATED_TO,
            fromId: source.id,
            toId: target.id,
          });
          return relation;
        },
      },
    ]);

    const committedRelation = await internalLoadById(testContext, ADMIN_USER, relation.internal_id) as any;
    expect(committedRelation.fromId).toBe(source.internal_id);
    expect(committedRelation.toId).toBe(target.internal_id);
  });

  it('deduplicates repeated relation creation inside one backend batch scope', async () => {
    const source = await addMalware(testContext, ADMIN_USER, { name: `Batch duplicate source ${uuidv4()}` });
    const target = await addMalware(testContext, ADMIN_USER, { name: `Batch duplicate target ${uuidv4()}` });
    cleanupMalwares.push(source, target);
    let firstRelation: any;
    let secondRelation: any;

    await executeBatchMutations([
      {
        kind: BatchMutationKind.CreateRelation,
        executeWrite: async () => {
          firstRelation = await addStixCoreRelationship(testContext, ADMIN_USER, {
            relationship_type: RELATION_RELATED_TO,
            fromId: source.id,
            toId: target.id,
          });
          return firstRelation;
        },
      },
      {
        kind: BatchMutationKind.CreateRelation,
        executeWrite: async () => {
          secondRelation = await addStixCoreRelationship(testContext, ADMIN_USER, {
            relationship_type: RELATION_RELATED_TO,
            fromId: source.id,
            toId: target.id,
          });
          return secondRelation;
        },
      },
    ]);

    expect(secondRelation.internal_id).toBe(firstRelation.internal_id);
    const committedRelations = await fullRelationsList(testContext, ADMIN_USER, RELATION_RELATED_TO, {
      fromId: source.internal_id,
      toId: target.internal_id,
    });
    expect(committedRelations).toHaveLength(1);
  });

  it('deletes relations created earlier in the same backend batch scope', async () => {
    const source = await addMalware(testContext, ADMIN_USER, { name: `Batch transient source ${uuidv4()}` });
    const target = await addMalware(testContext, ADMIN_USER, { name: `Batch transient target ${uuidv4()}` });
    cleanupMalwares.push(source, target);
    let relation: any;

    await executeBatchMutations([
      {
        kind: BatchMutationKind.CreateRelation,
        executeWrite: async () => {
          relation = await addStixCoreRelationship(testContext, ADMIN_USER, {
            relationship_type: RELATION_RELATED_TO,
            fromId: source.id,
            toId: target.id,
          });
          return relation;
        },
      },
      {
        kind: BatchMutationKind.UpdateAttribute,
        executeWrite: async () => {
          await elDeleteElements(testContext, ADMIN_USER, [target], { forceDelete: true });
          return null;
        },
      },
    ]);

    await expect(internalLoadById(testContext, ADMIN_USER, target.internal_id)).resolves.toBeUndefined();
    await expect(internalLoadById(testContext, ADMIN_USER, relation.internal_id)).resolves.toBeUndefined();
  });

  it('keeps soft-delete reindex copies inside the batch boundary until commit', async () => {
    const softDeleteMalware = await addMalware(testContext, ADMIN_USER, { name: `Batch soft delete ${uuidv4()}` });
    cleanupMalwares.push(softDeleteMalware);

    await expect(executeBatchMutations([
      {
        kind: BatchMutationKind.UpdateAttribute,
        executeWrite: async () => {
          await elDeleteElements(testContext, ADMIN_USER, [softDeleteMalware], { forceDelete: false });
          return null;
        },
      },
      {
        kind: BatchMutationKind.UpdateAttribute,
        executeWrite: async () => {
          const bufferedTrashCopy = await elLoadById(testContext, ADMIN_USER, softDeleteMalware.internal_id, { indices: [INDEX_DELETED_OBJECTS] });
          expect(bufferedTrashCopy?.internal_id).toBe(softDeleteMalware.internal_id);
          throw new Error('abort soft delete batch');
        },
      },
    ])).rejects.toThrow('abort soft delete batch');

    await expect(internalLoadById(testContext, ADMIN_USER, softDeleteMalware.internal_id)).resolves.toBeDefined();
    await expect(elLoadById(testContext, ADMIN_USER, softDeleteMalware.internal_id, { indices: [INDEX_DELETED_OBJECTS] })).resolves.toBeUndefined();
  });

  it('flushes soft-delete reindex copies through the batch boundary on commit', async () => {
    const softDeleteMalware = await addMalware(testContext, ADMIN_USER, { name: `Batch committed soft delete ${uuidv4()}` });
    cleanupSoftDeletedMalwareIds.push(softDeleteMalware.internal_id);

    await executeBatchMutations([
      {
        kind: BatchMutationKind.UpdateAttribute,
        executeWrite: async () => {
          await elDeleteElements(testContext, ADMIN_USER, [softDeleteMalware], { forceDelete: false });
          return null;
        },
      },
    ]);

    await expect(internalLoadById(testContext, ADMIN_USER, softDeleteMalware.internal_id)).resolves.toBeUndefined();
    await expect(elLoadById(testContext, ADMIN_USER, softDeleteMalware.internal_id, { indices: [INDEX_DELETED_OBJECTS] })).resolves.toBeDefined();
    await expect(findDeleteOperationsForMainEntity(softDeleteMalware.internal_id)).resolves.toHaveLength(1);
  });

  it('keeps draft copies and live draft markers inside the batch boundary until commit', async () => {
    const draftCopyMalware = await addMalware(testContext, ADMIN_USER, { name: `Batch draft copy ${uuidv4()}` });
    cleanupMalwares.push(draftCopyMalware);
    const draftId = `draft--${uuidv4()}`;
    const draftContext = { ...testContext, draft_context: draftId };

    await expect(executeBatchMutations([
      {
        kind: BatchMutationKind.UpdateAttribute,
        executeWrite: async () => {
          await copyLiveElementToDraft(draftContext, ADMIN_USER, draftCopyMalware);
          return null;
        },
      },
      {
        kind: BatchMutationKind.UpdateAttribute,
        executeWrite: async () => {
          const bufferedDraftCopy = await elLoadById(draftContext, ADMIN_USER, draftCopyMalware.internal_id);
          const bufferedLive = await internalLoadById(testContext, ADMIN_USER, draftCopyMalware.internal_id) as any;
          expect(bufferedDraftCopy?._index).toContain(INDEX_DRAFT_OBJECTS);
          expect(bufferedDraftCopy?.internal_id).toBe(draftCopyMalware.internal_id);
          expect(bufferedLive?.draft_ids).toContain(draftId);
          throw new Error('abort draft copy batch');
        },
      },
    ])).rejects.toThrow('abort draft copy batch');

    const committedLive = await internalLoadById(testContext, ADMIN_USER, draftCopyMalware.internal_id) as any;
    expect(committedLive?.draft_ids ?? []).not.toContain(draftId);
    await expect(elLoadById(testContext, ADMIN_USER, draftCopyMalware.internal_id, { indices: [INDEX_DRAFT_OBJECTS] })).resolves.toBeUndefined();
  });

  it('keeps revert-style draft marker removal inside the batch boundary until commit', async () => {
    const revertDraftMalware = await addMalware(testContext, ADMIN_USER, { name: `Batch revert draft ${uuidv4()}` });
    cleanupMalwares.push(revertDraftMalware);
    const draftId = `draft--${uuidv4()}`;
    const draftContext = { ...testContext, draft_context: draftId };
    await elReplace(testContext, revertDraftMalware._index, revertDraftMalware._id ?? revertDraftMalware.internal_id, {
      doc: { draft_ids: [draftId] },
    });

    await expect(executeBatchMutations([
      {
        kind: BatchMutationKind.UpdateAttribute,
        executeWrite: async () => {
          await elRemoveDraftIdFromElements(draftContext, ADMIN_USER, draftId, [revertDraftMalware.internal_id]);
          return null;
        },
      },
      {
        kind: BatchMutationKind.UpdateAttribute,
        executeWrite: async () => {
          const bufferedLive = await internalLoadById(testContext, ADMIN_USER, revertDraftMalware.internal_id) as any;
          expect(bufferedLive?.draft_ids ?? []).not.toContain(draftId);
          throw new Error('abort draft marker removal batch');
        },
      },
    ])).rejects.toThrow('abort draft marker removal batch');

    const committedLive = await internalLoadById(testContext, ADMIN_USER, revertDraftMalware.internal_id) as any;
    expect(committedLive?.draft_ids).toContain(draftId);
    await elReplace(testContext, revertDraftMalware._index, revertDraftMalware._id ?? revertDraftMalware.internal_id, {
      doc: { draft_ids: [] },
    });
  });

  it('keeps PIR information script updates inside the batch boundary until commit', async () => {
    const pirMalware = await addMalware(testContext, ADMIN_USER, { name: `Batch PIR update ${uuidv4()}` });
    cleanupMalwares.push(pirMalware);
    const pirId = `pir--${uuidv4()}`;

    await expect(executeBatchMutations([
      {
        kind: BatchMutationKind.UpdateAttribute,
        executeWrite: async () => {
          await updatePirInformationOnEntity(testContext, ADMIN_USER, pirMalware.internal_id, pirId, 78);
          return null;
        },
      },
      {
        kind: BatchMutationKind.UpdateAttribute,
        executeWrite: async () => {
          const buffered = await internalLoadById(testContext, ADMIN_USER, pirMalware.internal_id) as any;
          expect(buffered?.pir_information).toEqual(expect.arrayContaining([
            expect.objectContaining({ pir_id: pirId, pir_score: 78 }),
          ]));
          throw new Error('abort PIR update batch');
        },
      },
    ])).rejects.toThrow('abort PIR update batch');

    const committed = await internalLoadById(testContext, ADMIN_USER, pirMalware.internal_id) as any;
    expect(committed?.pir_information ?? []).not.toEqual(expect.arrayContaining([
      expect.objectContaining({ pir_id: pirId }),
    ]));
  });
});
