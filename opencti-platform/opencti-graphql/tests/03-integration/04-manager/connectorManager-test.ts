import { describe, it, expect } from 'vitest';
import { v4 as uuid } from 'uuid';
import moment from 'moment/moment';
import { createWork, findById as findWorkById, updateExpectationsNumber, updateProcessedTime, updateReceivedTime, worksForConnector } from '../../../src/domain/work';
import { registerConnector } from '../../../src/domain/connector';
import { ADMIN_USER, testContext } from '../../utils/testQuery';
import type { RegisterConnectorInput } from '../../../src/generated/graphql';
import { ConnectorType } from '../../../src/generated/graphql';
import { elIndex } from '../../../src/database/engine';
import { ENTITY_TYPE_WORK } from '../../../src/schema/internalObject';
import { INDEX_HISTORY, RABBIT_QUEUE_PREFIX } from '../../../src/database/utils';
import { closeOldWorks, deleteCompletedWorks } from '../../../src/manager/connectorManager';
import type { BasicStoreEntityConnector } from '../../../src/types/connector';
import type { Work } from '../../../src/types/work';
import { unregisterConnector, metrics } from '../../../src/database/rabbitmq';
import { redisGetWorkCompletionState, redisUpdateWorkFigures } from '../../../src/database/redis';

describe('Old work of connector cleanup test', () => {
  let testConnector: BasicStoreEntityConnector;
  const testConnectorId = uuid();

  const createConnectorForTest = async () => {
    const connectorData: RegisterConnectorInput = {
      id: testConnectorId,
      name: 'test-connector-manager-fake-connector',
      type: ConnectorType.ExternalImport,
    };
    testConnector = await registerConnector(testContext, ADMIN_USER, connectorData);
    expect(testConnector.id).toBeDefined();
  };

  const createWorkForTest = async (name: string, dateForWork: Date, status: string) => {
    // cheat and create a work in the past in elastic
    const dateForWorkStr = dateForWork.toISOString();
    const workId = `work_${testConnector.id}_${dateForWorkStr}`;

    const eightDaysAgoWork: Partial<Work> = {
      _index: '',
      completed_number: 10,
      completed_time: dateForWorkStr,
      connector_id: testConnector.id,
      entity_type: ENTITY_TYPE_WORK,
      errors: [],
      event_source_id: testConnector.id,
      event_type: '',
      id: workId,
      import_expected_number: 0,
      internal_id: workId,
      messages: [],
      name,
      processed_time: dateForWorkStr,
      received_time: dateForWorkStr,
      status,
      timestamp: dateForWorkStr,
      updated_at: dateForWork,
      user_id: '',
    };

    await elIndex(INDEX_HISTORY, eightDaysAgoWork as Work);
    const workCreated = await findWorkById(testContext, ADMIN_USER, workId) as unknown as Work;
    expect(workCreated.id).toBeDefined();
    expect(workCreated.status).toBe(status);
    expect(workCreated.completed_time).toBeDefined();
    return workCreated;
  };

  it('should cleanup old finished works but not new ones', async () => {
    // GIVEN a connector that has 2 works in database
    // one older than connector_manager.works_day_range (default: 7 days) and one more recent
    await createConnectorForTest();

    await createWorkForTest('Work 8 days old and complete', moment().subtract('8', 'days').toDate(), 'complete');
    await createWorkForTest('Work 9 days old and not complete', moment().subtract('9', 'days').toDate(), 'wait');
    await createWorkForTest('Work 2 days old and complete', moment().subtract('2', 'days').toDate(), 'complete');

    const allWorkBeforeCleanup = await worksForConnector(testContext, ADMIN_USER, testConnector.id) as Work[];
    expect(allWorkBeforeCleanup.length).toBe(3);
    expect(allWorkBeforeCleanup.some((workItem: Work) => workItem.name === 'Work 2 days old and complete')).toBeTruthy();
    expect(allWorkBeforeCleanup.some((workItem: Work) => workItem.name === 'Work 9 days old and not complete')).toBeTruthy();
    expect(allWorkBeforeCleanup.some((workItem: Work) => workItem.name === 'Work 8 days old and complete')).toBeTruthy();

    // WHEN the cleanup is done by manager
    await deleteCompletedWorks(testContext, testConnector);

    // THEN old complete one should be deleted and others still there
    const allWorkAfterCleanup: Work[] = await worksForConnector(testContext, ADMIN_USER, testConnector.id) as Work[];
    expect(allWorkAfterCleanup.length).toBe(2);

    expect(allWorkAfterCleanup.some((workItem: Work) => workItem.name === 'Work 2 days old and complete')).toBeTruthy();
    expect(allWorkAfterCleanup.some((workItem: Work) => workItem.name === 'Work 9 days old and not complete')).toBeTruthy();
    expect(allWorkAfterCleanup.some((workItem: Work) => workItem.name === 'Work 8 days old and complete')).toBeFalsy();
  });

  it('should not force complete an older live work when a newer work reports progress', async () => {
    const liveTimestamp = moment().subtract(2, 'minutes').toISOString();
    const newerTimestamp = moment().subtract(1, 'minute').toISOString();
    const liveWork = await createWork(testContext, ADMIN_USER, testConnector, 'Live concurrent work', testConnector.id, {
      preallocatedWork: {
        id: `work_${testConnector.id}_${liveTimestamp}_${uuid()}`,
        timestamp: liveTimestamp,
      },
    }) as unknown as Work;
    await updateReceivedTime(testContext, ADMIN_USER, liveWork.id, 'Connector ready to process the operation');

    const newerWork = await createWork(testContext, ADMIN_USER, testConnector, 'Newer concurrent work', testConnector.id, {
      preallocatedWork: {
        id: `work_${testConnector.id}_${newerTimestamp}_${uuid()}`,
        timestamp: newerTimestamp,
      },
    }) as unknown as Work;
    await redisUpdateWorkFigures(newerWork.id);

    await closeOldWorks(testContext, testConnector);

    const liveWorkAfterCleanup = await findWorkById(testContext, ADMIN_USER, liveWork.id) as unknown as Work;
    expect(liveWorkAfterCleanup.status).toBe('progress');
    expect(liveWorkAfterCleanup.completed_time).toBeNull();
  });

  it('should reconcile an older work when Redis proves completion', async () => {
    const settledTimestamp = moment().subtract(4, 'minutes').toISOString();
    const newerTimestamp = moment().subtract(3, 'minutes').toISOString();
    const settledWork = await createWork(testContext, ADMIN_USER, testConnector, 'Settled work with stale projection', testConnector.id, {
      preallocatedWork: {
        id: `work_${testConnector.id}_${settledTimestamp}_${uuid()}`,
        timestamp: settledTimestamp,
      },
    }) as unknown as Work;
    await updateReceivedTime(testContext, ADMIN_USER, settledWork.id, 'Connector ready to process the operation');
    await updateExpectationsNumber(testContext, ADMIN_USER, settledWork.id, 1);
    await redisUpdateWorkFigures(settledWork.id);

    const newerWork = await createWork(testContext, ADMIN_USER, testConnector, 'Newer work after settled projection', testConnector.id, {
      preallocatedWork: {
        id: `work_${testConnector.id}_${newerTimestamp}_${uuid()}`,
        timestamp: newerTimestamp,
      },
    }) as unknown as Work;
    await redisUpdateWorkFigures(newerWork.id);

    await closeOldWorks(testContext, testConnector);

    const settledWorkAfterCleanup = await findWorkById(testContext, ADMIN_USER, settledWork.id) as unknown as Work;
    expect(settledWorkAfterCleanup.status).toBe('complete');
    expect(settledWorkAfterCleanup.completed_number).toBe(1);
  });

  it('should retain a connector completion signal for zero-output work', async () => {
    const zeroOutputWork = await createWork(testContext, ADMIN_USER, testConnector, 'Zero output work', testConnector.id) as unknown as Work;
    await updateReceivedTime(testContext, ADMIN_USER, zeroOutputWork.id, 'Connector ready to process the operation');
    await updateProcessedTime(testContext, ADMIN_USER, zeroOutputWork.id, 'No changes produced by connector');

    const completionState = await redisGetWorkCompletionState(zeroOutputWork.id);
    expect(completionState).toEqual({
      expected: 0,
      total: 0,
      isProcessed: true,
      isMultiPartWork: false,
    });
  });

  it('should delete connector', async () => {
    const unregister = await unregisterConnector(testConnectorId);
    expect(unregister.listen).not.toBeNull();
    expect(unregister.listen.messageCount).toEqual(0);
    expect(unregister.push).not.toBeNull();
    expect(unregister.push.messageCount).toEqual(0);
    const data = await metrics(testContext, ADMIN_USER);
    const aggregationMap = new Map(data.queues.map((queue: any) => [queue.name, queue]));
    expect(aggregationMap.get(`${RABBIT_QUEUE_PREFIX}listen_${testConnectorId}`)).toBeUndefined();
    expect(aggregationMap.get(`${RABBIT_QUEUE_PREFIX}push_${testConnectorId}`)).toBeUndefined();
  });
});
