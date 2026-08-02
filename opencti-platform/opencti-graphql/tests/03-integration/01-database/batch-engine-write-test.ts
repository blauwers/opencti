import { afterAll, describe, expect, it } from 'vitest';
import { Readable } from 'stream';
import { v4 as uuidv4 } from 'uuid';
import { copyLiveElementToDraft, elDeleteElements, elDeleteInstances, elIndex, elLoadById, elRemoveDraftIdFromElements, elReplace, elUpdate, elUpdateElement, elUpdateEntityConnections, elUpdateRelationConnections } from '../../../src/database/engine';
import { deleteElementById, mergeEntities, storeLoadByIdWithRefs } from '../../../src/database/middleware';
import { fullEntitiesList, fullRelationsList, internalLoadById } from '../../../src/database/middleware-loader';
import { elIndexFiles } from '../../../src/database/file-search';
import { fileToReadStream, uploadToStorage } from '../../../src/database/file-storage';
import { getFileContent } from '../../../src/database/raw-file-storage';
import { findById as findDocumentById, IMPORT_STORAGE_PATH } from '../../../src/modules/internal/document/document-domain';
import { addMalware } from '../../../src/domain/malware';
import { addDraftContext, createWork, loadWorkById, pingWork, updateReceivedTime } from '../../../src/domain/work';
import { addStixCyberObservable, stixCyberObservableDelete } from '../../../src/domain/stixCyberObservable';
import { addStixCoreRelationship } from '../../../src/domain/stixCoreRelationship';
import { BatchMutationKind, BatchSideEffectKind, executeBatchMutations } from '../../../src/modules/batch/batch-executor';
import { INDEX_DELETED_OBJECTS, INDEX_DRAFT_OBJECTS, INDEX_FILES, INDEX_HISTORY } from '../../../src/database/utils';
import { confirmDelete } from '../../../src/modules/deleteOperation/deleteOperation-domain';
import { ENTITY_TYPE_DELETE_OPERATION } from '../../../src/modules/deleteOperation/deleteOperation-types';
import { updatePirInformationOnEntity } from '../../../src/modules/pir/pir-utils';
import { addVocabulary, deleteVocabulary, editVocabulary } from '../../../src/modules/vocabulary/vocabulary-domain';
import { ENTITY_TYPE_VOCABULARY } from '../../../src/modules/vocabulary/vocabulary-types';
import { FilterMode, FilterOperator, VocabularyCategory } from '../../../src/generated/graphql';
import { buildRefRelationKey } from '../../../src/schema/general';
import { RELATION_RELATED_TO } from '../../../src/schema/stixCoreRelationship';
import { ENTITY_TYPE_MALWARE } from '../../../src/schema/stixDomainObject';
import { ENTITY_TYPE_WORK } from '../../../src/schema/internalObject';
import { ADMIN_USER, testContext } from '../../utils/testQuery';
import { redisDeleteWorks, redisFetchLatestDeletions, redisGetWork } from '../../../src/database/redis';
import { computeLoaders } from '../../../src/http/httpAuthenticatedContext';
import { executionContext } from '../../../src/utils/access';

describe('batch engine writes', () => {
  let malware: any;
  let deletedMalware: any;
  const cleanupMalwares: any[] = [];
  const cleanupObservables: any[] = [];
  const cleanupSoftDeletedMalwareIds: string[] = [];
  const cleanupVocabularies: any[] = [];
  const cleanupWorkIds: string[] = [];

  const findDeleteOperationsForMainEntity = (mainEntityId: string) => {
    return fullEntitiesList<any>(testContext, ADMIN_USER, [ENTITY_TYPE_DELETE_OPERATION], {
      filters: {
        mode: FilterMode.And,
        filters: [{ key: ['main_entity_id'], values: [mainEntityId], operator: FilterOperator.Eq }],
        filterGroups: [],
      },
    });
  };

  const indexFileForEntity = async (entity: any) => {
    const fileName = `batch-file-${uuidv4()}.txt`;
    const file = fileToReadStream('./tests/data/', 'file-storage-helper-test.txt', fileName, 'text/plain');
    const filePath = `${IMPORT_STORAGE_PATH}/${entity.entity_type}/${entity.internal_id}`;
    const { upload } = await uploadToStorage(testContext, ADMIN_USER, filePath, file, { entity });
    const fileContent = await getFileContent(upload.id, 'base64');
    const documentId = `batch-file-index-${uuidv4()}`;
    await elIndexFiles(testContext, ADMIN_USER, [{
      internal_id: documentId,
      file_id: upload.id,
      file_data: fileContent,
      entity_id: entity.internal_id,
      name: fileName,
      uploaded_at: new Date(),
    }]);
    return { documentId, fileId: upload.id };
  };

  const createArtifactWithFile = (fileName: string) => {
    return addStixCyberObservable(testContext, ADMIN_USER, {
      type: 'Artifact',
      Artifact: {
        payload_bin: '',
        file: {
          createReadStream: () => Readable.from(`batch artifact ${fileName}`),
          filename: fileName,
          mimetype: 'text/plain',
        },
        hashes: [],
        mime_type: 'text/plain',
      },
    });
  };

  const buildDirectWork = () => ({
    internal_id: `work--${uuidv4()}`,
    entity_type: ENTITY_TYPE_WORK,
    name: `Batch direct work ${uuidv4()}`,
    timestamp: Date.now(),
    updated_at: new Date().toISOString(),
    status: 'wait',
    messages: [],
    errors: [],
  });

  const buildWorkConnector = () => ({
    internal_id: `connector--${uuidv4()}`,
    connector_type: 'EXTERNAL_IMPORT',
    connector_name: 'Batch work connector',
    name: 'Batch work connector',
  });

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
    for (let index = 0; index < cleanupObservables.length; index += 1) {
      const cleanupObservable = cleanupObservables[index];
      const existing = await internalLoadById(testContext, ADMIN_USER, cleanupObservable.internal_id);
      if (existing) {
        await stixCyberObservableDelete(testContext, ADMIN_USER, cleanupObservable.internal_id);
      }
    }
    for (let index = 0; index < cleanupSoftDeletedMalwareIds.length; index += 1) {
      const deleteOperations = await findDeleteOperationsForMainEntity(cleanupSoftDeletedMalwareIds[index]);
      for (let operationIndex = 0; operationIndex < deleteOperations.length; operationIndex += 1) {
        await confirmDelete(testContext, ADMIN_USER, deleteOperations[operationIndex].internal_id);
      }
    }
    for (let index = 0; index < cleanupVocabularies.length; index += 1) {
      const cleanupVocabulary = cleanupVocabularies[index];
      const existing = await internalLoadById(testContext, ADMIN_USER, cleanupVocabulary.internal_id);
      if (existing) {
        await deleteElementById(testContext, ADMIN_USER, cleanupVocabulary.internal_id, ENTITY_TYPE_VOCABULARY, { forceDelete: true });
      }
    }
    for (let index = 0; index < cleanupWorkIds.length; index += 1) {
      const cleanupWork = await loadWorkById(testContext, ADMIN_USER, cleanupWorkIds[index]);
      if (cleanupWork) {
        await elDeleteInstances(testContext, [cleanupWork]);
      }
    }
    await redisDeleteWorks(cleanupWorkIds);
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

  it('commits mixed create update and delete writes through one batch boundary', async () => {
    const mixedDeletedMalware = await addMalware(testContext, ADMIN_USER, { name: `Batch mixed delete ${uuidv4()}` });
    cleanupMalwares.push(mixedDeletedMalware);
    let mixedCreatedMalware: any;

    await executeBatchMutations([
      {
        kind: BatchMutationKind.CreateEntity,
        executeWrite: async () => {
          mixedCreatedMalware = await addMalware(testContext, ADMIN_USER, { name: `Batch mixed create ${uuidv4()}` });
          cleanupMalwares.push(mixedCreatedMalware);
          return null;
        },
      },
      {
        kind: BatchMutationKind.UpdateAttribute,
        executeWrite: async () => {
          await elUpdateElement(testContext, ADMIN_USER, {
            ...mixedCreatedMalware,
            description: 'mixed buffered update',
          });
          return null;
        },
      },
      {
        kind: BatchMutationKind.UpdateAttribute,
        executeWrite: async () => {
          await elDeleteInstances(testContext, [mixedDeletedMalware]);
          const bufferedCreated = await internalLoadById(testContext, ADMIN_USER, mixedCreatedMalware.internal_id) as any;
          const bufferedDeleted = await internalLoadById(testContext, ADMIN_USER, mixedDeletedMalware.internal_id);
          expect(bufferedCreated?.description).toBe('mixed buffered update');
          expect(bufferedDeleted).toBeUndefined();
          return null;
        },
      },
    ]);

    const committedCreated = await internalLoadById(testContext, ADMIN_USER, mixedCreatedMalware.internal_id) as any;
    expect(committedCreated?.description).toBe('mixed buffered update');
    await expect(internalLoadById(testContext, ADMIN_USER, mixedDeletedMalware.internal_id)).resolves.toBeUndefined();
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

  it('keeps direct index writes inside the batch boundary until commit', async () => {
    const directWork = buildDirectWork();

    await expect(executeBatchMutations([
      {
        kind: BatchMutationKind.CreateEntity,
        executeWrite: async () => {
          await elIndex(INDEX_HISTORY, directWork, { context: testContext });
          return null;
        },
      },
      {
        kind: BatchMutationKind.UpdateAttribute,
        executeWrite: async () => {
          const buffered = await loadWorkById(testContext, ADMIN_USER, directWork.internal_id) as any;
          expect(buffered?.name).toBe(directWork.name);
          throw new Error('abort direct index batch');
        },
      },
    ])).rejects.toThrow('abort direct index batch');

    await expect(loadWorkById(testContext, ADMIN_USER, directWork.internal_id)).resolves.toBeUndefined();
  });

  it('materializes direct index writes after batch commit', async () => {
    const directWork = buildDirectWork();
    cleanupWorkIds.push(directWork.internal_id);

    await executeBatchMutations([
      {
        kind: BatchMutationKind.CreateEntity,
        executeWrite: async () => {
          await elIndex(INDEX_HISTORY, directWork, { context: testContext });
          return null;
        },
      },
    ]);

    const committed = await loadWorkById(testContext, ADMIN_USER, directWork.internal_id) as any;
    expect(committed?.name).toBe(directWork.name);
  });

  it('keeps work Redis initialization outside an aborted batch', async () => {
    let workId: string | undefined;

    await expect(executeBatchMutations([
      {
        kind: BatchMutationKind.CreateEntity,
        executeWrite: async () => {
          const work = await createWork(testContext, ADMIN_USER, buildWorkConnector(), `Batch aborted work ${uuidv4()}`, 'batch-source') as any;
          workId = work.id;
          const bufferedRedisWork = await redisGetWork(work.id);
          expect(bufferedRedisWork?.is_initialized).toBeUndefined();
          return null;
        },
      },
      {
        kind: BatchMutationKind.UpdateAttribute,
        executeWrite: async () => {
          throw new Error('abort work lifecycle batch');
        },
      },
    ])).rejects.toThrow('abort work lifecycle batch');

    expect(workId).toBeDefined();
    const abortedRedisWork = await redisGetWork(workId as string);
    expect(abortedRedisWork?.is_initialized).toBeUndefined();
    await expect(loadWorkById(testContext, ADMIN_USER, workId as string)).resolves.toBeUndefined();
  });

  it('initializes work Redis state after batch commit', async () => {
    const execution = await executeBatchMutations([
      {
        kind: BatchMutationKind.CreateEntity,
        executeWrite: async () => {
          return createWork(testContext, ADMIN_USER, buildWorkConnector(), `Batch committed work ${uuidv4()}`, 'batch-source');
        },
      },
    ]);
    const committedWork = execution.results[0] as any;
    cleanupWorkIds.push(committedWork.id);

    expect(execution.sideEffectKinds).toContain(BatchSideEffectKind.WorkLifecycle);
    const committedRedisWork = await redisGetWork(committedWork.id);
    expect(committedRedisWork?.is_initialized).toBe('true');
  });

  it('keeps work script updates inside an aborted batch', async () => {
    const work = await createWork(testContext, ADMIN_USER, buildWorkConnector(), `Batch script abort ${uuidv4()}`, 'batch-source') as any;
    cleanupWorkIds.push(work.id);
    const draftContext = `draft--${uuidv4()}`;
    const message = `batch script message ${uuidv4()}`;

    await expect(executeBatchMutations([
      {
        kind: BatchMutationKind.UpdateAttribute,
        executeWrite: async () => {
          await pingWork(testContext, ADMIN_USER, work.id);
          await addDraftContext(testContext, ADMIN_USER, work.id, draftContext);
          await updateReceivedTime(testContext, ADMIN_USER, work.id, message);
          return null;
        },
      },
      {
        kind: BatchMutationKind.UpdateAttribute,
        executeWrite: async () => {
          const buffered = await loadWorkById(testContext, ADMIN_USER, work.id) as any;
          expect(buffered.status).toBe('progress');
          expect(buffered.draft_context).toBe(draftContext);
          expect(buffered.messages).toContainEqual(expect.objectContaining({ message }));
          throw new Error('abort work script batch');
        },
      },
    ])).rejects.toThrow('abort work script batch');

    const committed = await loadWorkById(testContext, ADMIN_USER, work.id) as any;
    expect(committed.status).toBe('wait');
    expect(committed.draft_context).toBeUndefined();
    expect(committed.messages).toEqual([]);
  });

  it('materializes work script updates after batch commit', async () => {
    const work = await createWork(testContext, ADMIN_USER, buildWorkConnector(), `Batch script commit ${uuidv4()}`, 'batch-source') as any;
    cleanupWorkIds.push(work.id);
    const draftContext = `draft--${uuidv4()}`;
    const message = `batch committed script message ${uuidv4()}`;

    await executeBatchMutations([
      {
        kind: BatchMutationKind.UpdateAttribute,
        executeWrite: async () => {
          await pingWork(testContext, ADMIN_USER, work.id);
          await addDraftContext(testContext, ADMIN_USER, work.id, draftContext);
          await updateReceivedTime(testContext, ADMIN_USER, work.id, message);
          return null;
        },
      },
    ]);

    const committed = await loadWorkById(testContext, ADMIN_USER, work.id) as any;
    expect(committed.status).toBe('progress');
    expect(committed.draft_context).toBe(draftContext);
    expect(committed.messages).toContainEqual(expect.objectContaining({ message }));
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
          expect(bufferedSource[RELATION_RELATED_TO]).toContain(replacementTarget.internal_id);
          expect(bufferedSource[RELATION_RELATED_TO]).not.toContain(originalTarget.internal_id);
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
    expect(committedSource[RELATION_RELATED_TO]).toContain(replacementTarget.internal_id);
    expect(committedSource[RELATION_RELATED_TO]).not.toContain(originalTarget.internal_id);
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
          expect(bufferedSource[RELATION_RELATED_TO]).not.toContain(target.internal_id);
          return null;
        },
      },
    ]);

    const committedSource = await internalLoadById(testContext, ADMIN_USER, source.internal_id) as any;
    expect(committedSource[relationKey]).not.toContain(target.internal_id);
    expect(committedSource[RELATION_RELATED_TO]).not.toContain(target.internal_id);
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
    const committedSource = await internalLoadById(testContext, ADMIN_USER, source.internal_id) as any;
    const committedTarget = await internalLoadById(testContext, ADMIN_USER, target.internal_id) as any;
    const relationKey = buildRefRelationKey(RELATION_RELATED_TO);
    expect(committedRelation.fromId).toBe(source.internal_id);
    expect(committedRelation.toId).toBe(target.internal_id);
    expect(committedSource[relationKey]).toContain(target.internal_id);
    expect(committedTarget[relationKey]).toContain(source.internal_id);
  });

  it('loads hydrated entities created earlier in the same batch through the request batch loader', async () => {
    const batchContext = executionContext('batch-hydrated-read', ADMIN_USER);
    batchContext.batch = computeLoaders(batchContext, ADMIN_USER);
    let created: any;

    await executeBatchMutations([
      {
        kind: BatchMutationKind.CreateEntity,
        executeWrite: async () => {
          created = await addMalware(batchContext, ADMIN_USER, { name: `Batch hydrated read ${uuidv4()}` });
          cleanupMalwares.push(created);
          return created;
        },
      },
      {
        kind: BatchMutationKind.UpdateAttribute,
        executeWrite: async () => {
          const [byInternalId, byStandardId] = await Promise.all([
            storeLoadByIdWithRefs(batchContext, ADMIN_USER, created.internal_id, { type: ENTITY_TYPE_MALWARE }),
            storeLoadByIdWithRefs(batchContext, ADMIN_USER, created.standard_id, { type: ENTITY_TYPE_MALWARE }),
          ]);
          expect(byInternalId?.internal_id).toBe(created.internal_id);
          expect(byStandardId?.internal_id).toBe(created.internal_id);
          return null;
        },
      },
    ]);
  });

  it('invalidates cached hydrated reads when a buffered write changes the entity', async () => {
    const batchContext = executionContext('batch-hydrated-cache-invalidation', ADMIN_USER);
    batchContext.batch = computeLoaders(batchContext, ADMIN_USER);
    let created: any;

    await executeBatchMutations([
      {
        kind: BatchMutationKind.CreateEntity,
        executeWrite: async () => {
          created = await addMalware(batchContext, ADMIN_USER, { name: `Batch hydrated cache ${uuidv4()}` });
          cleanupMalwares.push(created);
          return created;
        },
      },
      {
        kind: BatchMutationKind.UpdateAttribute,
        executeWrite: async () => {
          const first = await storeLoadByIdWithRefs(batchContext, ADMIN_USER, created.standard_id, { type: ENTITY_TYPE_MALWARE }) as any;
          const second = await storeLoadByIdWithRefs(batchContext, ADMIN_USER, created.standard_id, { type: ENTITY_TYPE_MALWARE }) as any;
          expect(first?.description).toBeUndefined();
          expect(second?.description).toBeUndefined();
          return null;
        },
      },
      {
        kind: BatchMutationKind.UpdateAttribute,
        executeWrite: async () => {
          await elUpdateElement(batchContext, ADMIN_USER, { ...created, description: 'buffered cache invalidation' });
          return null;
        },
      },
      {
        kind: BatchMutationKind.UpdateAttribute,
        executeWrite: async () => {
          const updated = await storeLoadByIdWithRefs(batchContext, ADMIN_USER, created.standard_id, { type: ENTITY_TYPE_MALWARE }) as any;
          expect(updated?.description).toBe('buffered cache invalidation');
          return null;
        },
      },
    ]);
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
    await expect(redisFetchLatestDeletions()).resolves.not.toContain(softDeleteMalware.internal_id);
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

  it('keeps soft-delete file search flags outside an aborted batch', async () => {
    const softDeleteMalware = await addMalware(testContext, ADMIN_USER, { name: `Batch file soft delete ${uuidv4()}` });
    cleanupMalwares.push(softDeleteMalware);
    const { documentId } = await indexFileForEntity(softDeleteMalware);

    await expect(executeBatchMutations([
      {
        kind: BatchMutationKind.UpdateAttribute,
        executeWrite: async () => {
          await deleteElementById(testContext, ADMIN_USER, softDeleteMalware.internal_id, ENTITY_TYPE_MALWARE);
          return null;
        },
      },
      {
        kind: BatchMutationKind.UpdateAttribute,
        executeWrite: async () => {
          const provisionalFile = await elLoadById(testContext, ADMIN_USER, documentId, { indices: INDEX_FILES }) as any;
          expect(provisionalFile?.removed ?? false).toBe(false);
          throw new Error('abort file soft delete batch');
        },
      },
    ])).rejects.toThrow('abort file soft delete batch');

    const committedFile = await elLoadById(testContext, ADMIN_USER, documentId, { indices: INDEX_FILES }) as any;
    expect(committedFile?.removed ?? false).toBe(false);
    await expect(redisFetchLatestDeletions()).resolves.not.toContain(softDeleteMalware.internal_id);
  });

  it('keeps permanent file cleanup outside an aborted batch', async () => {
    const forceDeleteMalware = await addMalware(testContext, ADMIN_USER, { name: `Batch file force delete ${uuidv4()}` });
    cleanupMalwares.push(forceDeleteMalware);
    const { fileId } = await indexFileForEntity(forceDeleteMalware);

    await expect(executeBatchMutations([
      {
        kind: BatchMutationKind.UpdateAttribute,
        executeWrite: async () => {
          await deleteElementById(testContext, ADMIN_USER, forceDeleteMalware.internal_id, ENTITY_TYPE_MALWARE, { forceDelete: true });
          return null;
        },
      },
      {
        kind: BatchMutationKind.UpdateAttribute,
        executeWrite: async () => {
          await expect(findDocumentById(testContext, ADMIN_USER, fileId)).resolves.toBeDefined();
          throw new Error('abort file force delete batch');
        },
      },
    ])).rejects.toThrow('abort file force delete batch');

    await expect(findDocumentById(testContext, ADMIN_USER, fileId)).resolves.toBeDefined();
  });

  it('keeps confirmed-delete file cleanup outside an aborted batch', async () => {
    const softDeleteMalware = await addMalware(testContext, ADMIN_USER, { name: `Batch file confirm delete ${uuidv4()}` });
    cleanupSoftDeletedMalwareIds.push(softDeleteMalware.internal_id);
    const { fileId } = await indexFileForEntity(softDeleteMalware);
    await deleteElementById(testContext, ADMIN_USER, softDeleteMalware.internal_id, ENTITY_TYPE_MALWARE);
    await expect(redisFetchLatestDeletions()).resolves.toContain(softDeleteMalware.internal_id);
    const [deleteOperation] = await findDeleteOperationsForMainEntity(softDeleteMalware.internal_id);

    await expect(executeBatchMutations([
      {
        kind: BatchMutationKind.UpdateAttribute,
        executeWrite: async () => {
          await confirmDelete(testContext, ADMIN_USER, deleteOperation.internal_id);
          return null;
        },
      },
      {
        kind: BatchMutationKind.UpdateAttribute,
        executeWrite: async () => {
          await expect(findDocumentById(testContext, ADMIN_USER, fileId)).resolves.toBeDefined();
          throw new Error('abort file confirm delete batch');
        },
      },
    ])).rejects.toThrow('abort file confirm delete batch');

    await expect(findDocumentById(testContext, ADMIN_USER, fileId)).resolves.toBeDefined();
    await expect(findDeleteOperationsForMainEntity(softDeleteMalware.internal_id)).resolves.toHaveLength(1);
  });

  it('keeps merge file moves outside an aborted batch', async () => {
    const targetArtifact = await createArtifactWithFile(`batch-target-${uuidv4()}.txt`);
    const sourceArtifact = await createArtifactWithFile(`batch-source-${uuidv4()}.txt`);
    cleanupObservables.push(targetArtifact, sourceArtifact);
    const sourceFileId = sourceArtifact.x_opencti_files[0].id;
    const targetFileId = sourceFileId.replace(sourceArtifact.internal_id, targetArtifact.internal_id);

    await expect(executeBatchMutations([
      {
        kind: BatchMutationKind.UpdateAttribute,
        executeWrite: async () => {
          await mergeEntities(testContext, ADMIN_USER, targetArtifact.internal_id, [sourceArtifact.internal_id]);
          return null;
        },
      },
      {
        kind: BatchMutationKind.UpdateAttribute,
        executeWrite: async () => {
          await expect(findDocumentById(testContext, ADMIN_USER, sourceFileId)).resolves.toBeDefined();
          await expect(findDocumentById(testContext, ADMIN_USER, targetFileId)).resolves.toBeUndefined();
          throw new Error('abort merge file move batch');
        },
      },
    ])).rejects.toThrow('abort merge file move batch');

    await expect(findDocumentById(testContext, ADMIN_USER, sourceFileId)).resolves.toBeDefined();
    await expect(findDocumentById(testContext, ADMIN_USER, targetFileId)).resolves.toBeUndefined();
    await expect(redisFetchLatestDeletions()).resolves.not.toContain(sourceArtifact.internal_id);
  });

  it('materializes merge file moves after batch commit', async () => {
    const targetArtifact = await createArtifactWithFile(`batch-commit-target-${uuidv4()}.txt`);
    const sourceArtifact = await createArtifactWithFile(`batch-commit-source-${uuidv4()}.txt`);
    cleanupObservables.push(targetArtifact, sourceArtifact);
    const sourceFileId = sourceArtifact.x_opencti_files[0].id;
    const targetFileId = sourceFileId.replace(sourceArtifact.internal_id, targetArtifact.internal_id);

    await executeBatchMutations([
      {
        kind: BatchMutationKind.UpdateAttribute,
        executeWrite: async () => {
          await mergeEntities(testContext, ADMIN_USER, targetArtifact.internal_id, [sourceArtifact.internal_id]);
          return null;
        },
      },
    ]);

    await expect(findDocumentById(testContext, ADMIN_USER, sourceFileId)).resolves.toBeUndefined();
    await expect(findDocumentById(testContext, ADMIN_USER, targetFileId)).resolves.toBeDefined();
    const mergedArtifact = await internalLoadById(testContext, ADMIN_USER, targetArtifact.internal_id) as any;
    expect(mergedArtifact?.x_opencti_files.map((file: any) => file.id)).toContain(targetFileId);
    await expect(redisFetchLatestDeletions()).resolves.toContain(sourceArtifact.internal_id);
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

  it('keeps vocabulary rename fan-out updates inside the batch boundary until commit', async () => {
    const oldName = `batch-old-${uuidv4()}`;
    const newName = `batch-new-${uuidv4()}`;
    const vocabulary = await addVocabulary(testContext, ADMIN_USER, {
      name: oldName,
      description: '',
      category: VocabularyCategory.MalwareTypeOv,
    });
    cleanupVocabularies.push(vocabulary);
    const vocabularyMalware = await addMalware(testContext, ADMIN_USER, {
      name: `Batch vocabulary rename ${uuidv4()}`,
      malware_types: [oldName],
    });
    cleanupMalwares.push(vocabularyMalware);

    await expect(executeBatchMutations([
      {
        kind: BatchMutationKind.UpdateAttribute,
        executeWrite: async () => {
          await editVocabulary(testContext, ADMIN_USER, vocabulary.internal_id, [{ key: 'name', value: [newName] }], {});
          return null;
        },
      },
      {
        kind: BatchMutationKind.UpdateAttribute,
        executeWrite: async () => {
          const buffered = await internalLoadById(testContext, ADMIN_USER, vocabularyMalware.internal_id) as any;
          expect(buffered?.malware_types).toContain(newName);
          expect(buffered?.malware_types).not.toContain(oldName);
          throw new Error('abort vocabulary rename batch');
        },
      },
    ])).rejects.toThrow('abort vocabulary rename batch');

    const committed = await internalLoadById(testContext, ADMIN_USER, vocabularyMalware.internal_id) as any;
    expect(committed?.malware_types).toContain(oldName);
    expect(committed?.malware_types).not.toContain(newName);
  });

  it('keeps vocabulary delete fan-out updates inside the batch boundary until commit', async () => {
    const removedName = `batch-remove-${uuidv4()}`;
    const keptName = `batch-keep-${uuidv4()}`;
    const removedVocabulary = await addVocabulary(testContext, ADMIN_USER, {
      name: removedName,
      description: '',
      category: VocabularyCategory.MalwareTypeOv,
    });
    const keptVocabulary = await addVocabulary(testContext, ADMIN_USER, {
      name: keptName,
      description: '',
      category: VocabularyCategory.MalwareTypeOv,
    });
    cleanupVocabularies.push(removedVocabulary, keptVocabulary);
    const vocabularyMalware = await addMalware(testContext, ADMIN_USER, {
      name: `Batch vocabulary delete ${uuidv4()}`,
      malware_types: [removedName, keptName],
    });
    cleanupMalwares.push(vocabularyMalware);

    await expect(executeBatchMutations([
      {
        kind: BatchMutationKind.UpdateAttribute,
        executeWrite: async () => {
          await deleteVocabulary(testContext, ADMIN_USER, removedVocabulary.internal_id);
          return null;
        },
      },
      {
        kind: BatchMutationKind.UpdateAttribute,
        executeWrite: async () => {
          const buffered = await internalLoadById(testContext, ADMIN_USER, vocabularyMalware.internal_id) as any;
          expect(buffered?.malware_types).toEqual([keptName]);
          throw new Error('abort vocabulary delete batch');
        },
      },
    ])).rejects.toThrow('abort vocabulary delete batch');

    const committed = await internalLoadById(testContext, ADMIN_USER, vocabularyMalware.internal_id) as any;
    expect(committed?.malware_types).toEqual(expect.arrayContaining([removedName, keptName]));
  });

  it('applies vocabulary fan-out updates to entities created earlier in the same batch', async () => {
    const oldName = `batch-created-old-${uuidv4()}`;
    const newName = `batch-created-new-${uuidv4()}`;
    const vocabulary = await addVocabulary(testContext, ADMIN_USER, {
      name: oldName,
      description: '',
      category: VocabularyCategory.MalwareTypeOv,
    });
    cleanupVocabularies.push(vocabulary);
    let createdMalwareId: string | undefined;

    await expect(executeBatchMutations([
      {
        kind: BatchMutationKind.CreateEntity,
        executeWrite: async () => {
          const createdMalware = await addMalware(testContext, ADMIN_USER, {
            name: `Batch created vocabulary rename ${uuidv4()}`,
            malware_types: [oldName],
          });
          createdMalwareId = createdMalware.internal_id;
          return null;
        },
      },
      {
        kind: BatchMutationKind.UpdateAttribute,
        executeWrite: async () => {
          await editVocabulary(testContext, ADMIN_USER, vocabulary.internal_id, [{ key: 'name', value: [newName] }], {});
          return null;
        },
      },
      {
        kind: BatchMutationKind.UpdateAttribute,
        executeWrite: async () => {
          const buffered = await internalLoadById(testContext, ADMIN_USER, createdMalwareId) as any;
          expect(buffered?.malware_types).toEqual([newName]);
          throw new Error('abort created vocabulary rename batch');
        },
      },
    ])).rejects.toThrow('abort created vocabulary rename batch');

    const committed = await internalLoadById(testContext, ADMIN_USER, createdMalwareId);
    expect(committed).toBeUndefined();
  });
});
