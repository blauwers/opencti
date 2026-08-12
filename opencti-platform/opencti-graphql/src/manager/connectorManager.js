import { clearIntervalAsync, setIntervalAsync } from 'set-interval-async/fixed';
import { redisGetConnectorStatus, redisGetWorkCompletionState, redisGetWorksCompletionState } from '../database/redis';
import { lockResources } from '../lock/master-lock';
import conf, { booleanConf, logApp } from '../config/conf';
import { TYPE_LOCK_ERROR } from '../config/errors';
import { connectors } from '../database/repository';
import { elList, elUpdateWithBufferedApply } from '../database/engine';
import { executionContext, SYSTEM_USER } from '../utils/access';
import { READ_INDEX_HISTORY } from '../database/utils';
import { now } from '../utils/format';
import { canReconcileWorkCompletionFromRedis, deleteWorksRaw } from '../domain/work';
import { BatchMutationKind, executeSingleBatchMutation } from '../modules/batch/batch-executor';

// Manage work created by connectors
// Update status to complete when needed
// Cleanup "batch_size" work in Elastic and Redis when complete "after works_day_range" days
const SCHEDULE_TIME = conf.get('connector_manager:interval') || 60000;
const CONNECTOR_MANAGER_KEY = conf.get('connector_manager:lock_key') || 'connector_manager_lock';
const CONNECTOR_WORK_RANGE = conf.get('connector_manager:works_day_range') || 7;
const BATCH_SIZE = conf.get('connector_manager:batch_size') || 10000;
let running = false;

const buildWorkCompletionReconciliation = (element, completionState) => {
  const params = { completed_time: now(), completed_number: completionState.total };
  const sourceScript = `ctx._source['status'] = "complete";
      ctx._source['completed_time'] = params.completed_time;
      ctx._source['completed_number'] = params.completed_number;`;
  return { element, completionState, params, sourceScript };
};

const applyWorkCompletionReconciliation = async (context, reconciliation) => {
  const { element, params, sourceScript } = reconciliation;
  await elUpdateWithBufferedApply(context, element._index, element.internal_id, {
    script: {
      source: sourceScript,
      lang: 'painless',
      params,
    },
  }, (existing) => ({
    ...existing,
    status: 'complete',
    completed_time: params.completed_time,
    completed_number: params.completed_number,
  }));
};

const logWorkCompletionReconciled = ({ element, completionState }) => {
  logApp.info('Work completion reconciled from Redis state', {
    workId: element.internal_id,
    completedNumber: completionState.total,
  });
};

const loadWorkCompletionStates = async (elements) => {
  const workIds = elements.map((element) => element.internal_id);
  try {
    return await redisGetWorksCompletionState(workIds);
  } catch (e) {
    logApp.warn('[OPENCTI-MODULE] Connector manager bulk work completion read failed, falling back to scalar reads', { cause: e });
    const completionStates = {};
    for (let index = 0; index < elements.length; index += 1) {
      const element = elements[index];
      try {
        completionStates[element.internal_id] = await redisGetWorkCompletionState(element.internal_id);
      } catch (error) {
        logApp.error('[OPENCTI-MODULE] Connector manager error processing work closing', { cause: error });
      }
    }
    return completionStates;
  }
};

const reconcileWorkCompletions = async (context, reconciliations) => {
  if (reconciliations.length === 0) {
    return;
  }
  try {
    await executeSingleBatchMutation({
      kind: BatchMutationKind.UpdateAttribute,
      executeWrite: async () => {
        for (let index = 0; index < reconciliations.length; index += 1) {
          await applyWorkCompletionReconciliation(context, reconciliations[index]);
        }
        return reconciliations.map(({ element }) => element.internal_id);
      },
    });
    reconciliations.forEach(logWorkCompletionReconciled);
  } catch (e) {
    logApp.warn('[OPENCTI-MODULE] Connector manager batch work closing failed, falling back to scalar updates', { cause: e });
    for (let index = 0; index < reconciliations.length; index += 1) {
      const reconciliation = reconciliations[index];
      try {
        await applyWorkCompletionReconciliation(context, reconciliation);
        logWorkCompletionReconciled(reconciliation);
      } catch (error) {
        logApp.error('[OPENCTI-MODULE] Connector manager error processing work closing', { cause: error });
      }
    }
  }
};

export const closeOldWorks = async (context, connector) => {
  // Get current status from Redis
  const status = await redisGetConnectorStatus(connector.internal_id);
  // If status is here we can try to close all old open works
  if (status) {
    const [,, timestamp] = status.split('_');
    // Get all works created before the current one and put a complete status on it.
    const filters = {
      mode: 'and',
      filters: [
        { key: 'connector_id', values: [connector.internal_id] },
        { key: 'status', values: ['wait', 'progress'] },
        { key: 'timestamp', values: [timestamp], operator: 'lt' },
      ],
      filterGroups: [],
    };
    const queryCallback = async (elements) => {
      const completionStates = await loadWorkCompletionStates(elements);
      const reconciliations = elements.flatMap((element) => {
        const completionState = completionStates[element.internal_id];
        return completionState && canReconcileWorkCompletionFromRedis(completionState)
          ? [buildWorkCompletionReconciliation(element, completionState)]
          : [];
      });
      await reconcileWorkCompletions(context, reconciliations);
    };
    await elList(context, SYSTEM_USER, [READ_INDEX_HISTORY], {
      filters,
      noFiltersChecking: true,
      types: ['Work'],
      orderBy: 'timestamp',
      baseData: true,
      baseFields: ['internal_id', 'timestamp'],
      maxSize: BATCH_SIZE,
      callback: queryCallback,
    });
  }
};

export const deleteCompletedWorks = async (context, connector) => {
  const filters = {
    mode: 'and',
    filters: [
      { key: 'connector_id', values: [connector.internal_id] },
      { key: 'status', values: ['complete'] },
      { key: 'completed_time', values: [`now-${CONNECTOR_WORK_RANGE}d/d`], operator: 'lte' },
    ],
    filterGroups: [],
  };
  const queryCallback = async (elements) => {
    const message = `[WORKS] Deleting ${elements.length} works for ${connector.name}`;
    logApp.info(message);
    await deleteWorksRaw(context, elements);
  };
  await elList(context, SYSTEM_USER, [READ_INDEX_HISTORY], {
    filters,
    types: ['Work'],
    orderBy: 'timestamp',
    noFiltersChecking: true,
    baseData: true,
    baseFields: ['internal_id'],
    maxSize: BATCH_SIZE,
    callback: queryCallback,
  });
};

const connectorHandler = async () => {
  let lock;
  try {
    // Lock the manager
    lock = await lockResources([CONNECTOR_MANAGER_KEY], { retryCount: 0 });
    running = true;
    const context = executionContext('connector_manager');
    // Execute the cleaning
    const platformConnectors = await connectors(context, SYSTEM_USER);
    for (let index = 0; index < platformConnectors.length; index += 1) {
      lock.signal.throwIfAborted();
      const platformConnector = platformConnectors[index];
      // Force close all needed works
      await closeOldWorks(context, platformConnector);
      // Cleanup too old complete works
      await deleteCompletedWorks(context, platformConnector);
    }
  } catch (e) {
    if (e.name === TYPE_LOCK_ERROR) {
      logApp.debug('[OPENCTI-MODULE] Connector manager already started by another API');
    } else {
      logApp.error('[OPENCTI-MODULE] Connector manager handling error', { cause: e, manager: 'CONNECTOR_MANAGER' });
    }
  } finally {
    running = false;
    logApp.debug('[OPENCTI-MODULE] Connector manager done');
    if (lock) await lock.unlock();
  }
};

const initConnectorManager = () => {
  let scheduler;
  return {
    start: async () => {
      scheduler = setIntervalAsync(async () => {
        await connectorHandler();
      }, SCHEDULE_TIME);
    },
    status: async () => {
      return {
        id: 'CONNECTOR_MANAGER',
        enable: booleanConf('connector_manager:enabled', false),
        running,
      };
    },
    shutdown: async () => {
      const startTime = Date.now();
      logApp.info('[OPENCTI-MODULE] Stopping connector manager');
      if (scheduler) {
        return clearIntervalAsync(scheduler);
      }
      logApp.info(`[OPENCTI-MODULE] Connector manager stopped in ${Date.now() - startTime} ms`);
      return true;
    },
  };
};
const connectorManager = initConnectorManager();

export default connectorManager;
