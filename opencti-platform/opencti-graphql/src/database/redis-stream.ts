import { Cluster, Redis } from 'ioredis';
import * as R from 'ramda';
import conf, { logApp, REDIS_PREFIX } from '../config/conf';
import type { ActivityStreamEvent, BaseEvent, DataEvent, SseEvent, StreamNotifEvent } from '../types/event';
import {
  ACTIVITY_STREAM_NAME,
  type FetchEventRangeOption,
  LIVE_STREAM_NAME,
  NOTIFICATION_STREAM_NAME,
  type RawStreamClient,
  type SizedNotifEvent,
  type StreamProcessor,
  type StreamProcessorOption,
} from './stream/stream-utils';
import { createRedisClient, getClientBase, getClientXRANGE } from './redis';
import { isEmptyField, wait, waitInSec } from './utils';
import { utcDate } from '../utils/format';
import { DatabaseError, UnsupportedError } from '../config/errors';
import { asyncMap } from '../utils/data-processing';
import { roundRate } from '../utils/consumer-metrics';

// region opencti data stream
const REDIS_LIVE_STREAM_NAME = `${REDIS_PREFIX}${LIVE_STREAM_NAME}`;
const REDIS_NOTIFICATION_STREAM_NAME = `${REDIS_PREFIX}${NOTIFICATION_STREAM_NAME}`;
const REDIS_ACTIVITY_STREAM_NAME = `${REDIS_PREFIX}${ACTIVITY_STREAM_NAME}`;
const streamTrimming = conf.get('redis:trimming') || 0;
const MAX_REDIS_STREAM_ENTRY_ID_BYTES = 41;
export const REDIS_STREAM_PUBLICATION_PROOF_MAX_ENTRIES = 1024;
export const REDIS_STREAM_PUBLICATION_PROOF_MAX_SERIALIZED_BYTES = 512 * 1024;

export enum RedisStreamPublicationProofAppendResult {
  Malformed = -1,
  Conflict = 0,
  Appended = 1,
  Existing = 2,
  EntryLimitExceeded = 3,
  SerializedByteLimitExceeded = 4,
}

export interface RawAppendOrReturnLiveStreamPublicationProofInput {
  deliveryId: string;
  publicationId: string;
  eventFingerprint: string;
  publishedAt: string;
  proofVersion: number;
  maxEntries: number;
  maxSerializedBytes: number;
  event: BaseEvent;
}

export interface RawAppendOrReturnLiveStreamPublicationProofResult {
  result: RedisStreamPublicationProofAppendResult;
  rawProof: string | null;
}

const redisHashSlotTag = (key: string): string => {
  const openingBraceIndex = key.indexOf('{');
  if (openingBraceIndex >= 0) {
    const closingBraceIndex = key.indexOf('}', openingBraceIndex + 1);
    if (closingBraceIndex > openingBraceIndex + 1) {
      return key.slice(openingBraceIndex + 1, closingBraceIndex);
    }
  }
  return key;
};

// EVAL touches the live stream and proof hash together, so both keys must share one Redis Cluster slot.
const buildLiveStreamCoLocatedKey = (suffix: string): string => {
  return `{${redisHashSlotTag(REDIS_LIVE_STREAM_NAME)}}:${suffix}`;
};

export const buildRedisStreamPublicationProofContainerKey = (deliveryId: string): string => {
  return buildLiveStreamCoLocatedKey(`batch_stream_publication_proof:${deliveryId}`);
};

const APPEND_OR_RETURN_LIVE_STREAM_PUBLICATION_PROOF_SCRIPT = `
  local function is_sha256_hex(value)
    return type(value) == 'string'
      and string.len(value) == 64
      and string.match(value, '^[a-f0-9]+$') ~= nil
  end

  local function is_valid_proof(field, proof, proof_version)
    if type(proof) ~= 'table' then
      return false
    end
    local field_count = 0
    for key, _ in pairs(proof) do
      if key ~= 'publication_id'
        and key ~= 'event_fingerprint'
        and key ~= 'stream_entry_id'
        and key ~= 'published_at'
        and key ~= 'proof_version' then
        return false
      end
      field_count = field_count + 1
    end
    return field_count == 5
      and type(proof.publication_id) == 'string'
      and proof.publication_id == field
      and type(proof.event_fingerprint) == 'string'
      and type(proof.stream_entry_id) == 'string'
      and string.len(proof.stream_entry_id) > 0
      and type(proof.published_at) == 'string'
      and string.len(proof.published_at) > 0
      and tostring(proof.proof_version) == proof_version
  end

  local proof_version = tonumber(ARGV[4])
  local max_entries = tonumber(ARGV[5])
  local max_serialized_bytes = tonumber(ARGV[6])
  local max_stream_entry_id_bytes = tonumber(ARGV[7])
  if not is_sha256_hex(ARGV[1])
    or not is_sha256_hex(ARGV[2])
    or string.len(ARGV[3]) == 0
    or not proof_version
    or proof_version < 1
    or proof_version % 1 ~= 0
    or tostring(proof_version) ~= ARGV[4]
    or not max_entries
    or max_entries < 1
    or max_entries % 1 ~= 0
    or not max_serialized_bytes
    or max_serialized_bytes < 1
    or max_serialized_bytes % 1 ~= 0
    or not max_stream_entry_id_bytes
    or max_stream_entry_id_bytes < 1
    or max_stream_entry_id_bytes % 1 ~= 0 then
    return {-1}
  end

  local current = redis.call('HGET', KEYS[2], ARGV[1])
  if current then
    local decoded, proof = pcall(cjson.decode, current)
    if not decoded or not is_valid_proof(ARGV[1], proof, ARGV[4]) then
      return {-1}
    end
    if proof.event_fingerprint ~= ARGV[2] then
      return {0}
    end
    return {2, current}
  end

  if redis.call('HLEN', KEYS[2]) >= max_entries then
    return {3}
  end

  local current_fields = redis.call('HGETALL', KEYS[2])
  local current_serialized_bytes = 0
  for index = 1, #current_fields, 2 do
    local field = current_fields[index]
    local raw_proof = current_fields[index + 1]
    local decoded, proof = pcall(cjson.decode, raw_proof)
    if not decoded or not is_valid_proof(field, proof, ARGV[4]) then
      return {-1}
    end
    current_serialized_bytes = current_serialized_bytes + string.len(field) + string.len(raw_proof)
  end

  local prospective_proof = cjson.encode({
    publication_id = ARGV[1],
    event_fingerprint = ARGV[2],
    stream_entry_id = string.rep('0', max_stream_entry_id_bytes),
    published_at = ARGV[3],
    proof_version = proof_version,
  })
  if current_serialized_bytes + string.len(ARGV[1]) + string.len(prospective_proof) > max_serialized_bytes then
    return {4}
  end

  local stream_entry_id
  if ARGV[8] == '1' then
    stream_entry_id = redis.call('XADD', KEYS[1], 'MAXLEN', '~', ARGV[9], '*', unpack(ARGV, 10))
  else
    stream_entry_id = redis.call('XADD', KEYS[1], '*', unpack(ARGV, 10))
  end
  local proof = cjson.encode({
    publication_id = ARGV[1],
    event_fingerprint = ARGV[2],
    stream_entry_id = stream_entry_id,
    published_at = ARGV[3],
    proof_version = proof_version,
  })
  redis.call('HSET', KEYS[2], ARGV[1], proof)
  return {1, proof}
`;

const convertStreamName = (streamName = LIVE_STREAM_NAME) => {
  switch (streamName) {
    case ACTIVITY_STREAM_NAME:
      return REDIS_ACTIVITY_STREAM_NAME;
    case NOTIFICATION_STREAM_NAME:
      return REDIS_NOTIFICATION_STREAM_NAME;
    case LIVE_STREAM_NAME:
      return REDIS_LIVE_STREAM_NAME;
    default:
      throw UnsupportedError('Cannot recognize stream name', streamName);
  }
};

const mapJSToStream = (event: any) => {
  const cmdArgs: Array<string> = [];
  Object.keys(event).forEach((key) => {
    const value = event[key];
    if (value !== undefined) {
      cmdArgs.push(key);
      cmdArgs.push(JSON.stringify(value));
    }
  });
  return cmdArgs;
};
const mapStreamToJS = ([id, data]: any): SseEvent<any> => {
  const count = data.length / 2;
  const obj: any = {};
  for (let i = 0; i < count; i += 1) {
    obj[data[2 * i]] = JSON.parse(data[2 * i + 1]);
  }
  return { id, event: obj.type, data: obj };
};

const rawPushToStream = async <T extends BaseEvent> (event: T) => {
  const redisClient = getClientBase();
  const eventStreamData = mapJSToStream(event);
  if (streamTrimming) {
    await redisClient.call('XADD', REDIS_LIVE_STREAM_NAME, 'MAXLEN', '~', streamTrimming, '*', ...eventStreamData);
  } else {
    await redisClient.call('XADD', REDIS_LIVE_STREAM_NAME, '*', ...eventStreamData);
  }
};

export const rawAppendOrReturnLiveStreamPublicationProof = async (
  input: RawAppendOrReturnLiveStreamPublicationProofInput,
): Promise<RawAppendOrReturnLiveStreamPublicationProofResult> => {
  const eventStreamData = mapJSToStream(input.event);
  const maxEntries = Math.min(input.maxEntries, REDIS_STREAM_PUBLICATION_PROOF_MAX_ENTRIES);
  const maxSerializedBytes = Math.min(input.maxSerializedBytes, REDIS_STREAM_PUBLICATION_PROOF_MAX_SERIALIZED_BYTES);
  const result = await getClientBase().call(
    'EVAL',
    APPEND_OR_RETURN_LIVE_STREAM_PUBLICATION_PROOF_SCRIPT,
    2,
    REDIS_LIVE_STREAM_NAME,
    buildRedisStreamPublicationProofContainerKey(input.deliveryId),
    input.publicationId,
    input.eventFingerprint,
    input.publishedAt,
    `${input.proofVersion}`,
    `${maxEntries}`,
    `${maxSerializedBytes}`,
    `${MAX_REDIS_STREAM_ENTRY_ID_BYTES}`,
    streamTrimming ? '1' : '0',
    `${streamTrimming}`,
    ...eventStreamData,
  ) as unknown;
  if (!Array.isArray(result) || result.length === 0) {
    throw DatabaseError('Redis stream publication proof append returned an invalid response', {
      delivery_id: input.deliveryId,
      publication_id: input.publicationId,
    });
  }
  return {
    result: Number(result[0]) as RedisStreamPublicationProofAppendResult,
    rawProof: typeof result[1] === 'string' ? result[1] : null,
  };
};

export const rawReadLiveStreamPublicationProof = async (
  deliveryId: string,
  publicationId: string,
): Promise<string | null> => {
  const result = await getClientBase().call(
    'HGET',
    buildRedisStreamPublicationProofContainerKey(deliveryId),
    publicationId,
  );
  return typeof result === 'string' ? result : null;
};
const processStreamResult = async (results: Array<any>, callback: any, withInternal: boolean | undefined) => {
  const transform = (r: any) => mapStreamToJS(r);
  const filter = (s: any) => (withInternal ? true : (s.data.scope ?? 'external') === 'external');
  const events = await asyncMap(results, transform, filter);
  const lastEventId = events.length > 0 ? R.last(events)?.id : `${new Date().valueOf()}-0`;
  await callback(events, lastEventId);
  return lastEventId;
};
const rawFetchStreamInfo = async (streamName = LIVE_STREAM_NAME) => {
  const redisStreamName = convertStreamName(streamName);
  const res: any = await getClientBase().xinfo('STREAM', redisStreamName);
  const info: any = R.fromPairs(R.splitEvery(2, res) as any);
  const firstId = info['first-entry'][0];
  const firstEventDate = utcDate(parseInt(firstId.split('-')[0], 10)).toISOString();
  const lastId = info['last-entry'][0];
  const lastEventDate = utcDate(parseInt(lastId.split('-')[0], 10)).toISOString();
  return { lastEventId: lastId, firstEventId: firstId, firstEventDate, lastEventDate, streamSize: info.length };
};

const STREAM_BATCH_TIME = 5000;
const MAX_RANGE_MESSAGES = 100;

const rawCreateStreamProcessor = <T extends BaseEvent> (
  provider: string,
  callback: (events: Array<SseEvent<T>>, lastEventId: string) => Promise<void>,
  opts: StreamProcessorOption = {},
): StreamProcessor => {
  let client: Cluster | Redis;
  let startEventId: string;
  let processingLoopPromise: Promise<void>;
  let streamListening = true;
  const { streamName = LIVE_STREAM_NAME } = opts;
  const redisStreamName = convertStreamName(streamName);

  const processStep = async () => {
    // since previous call is async (and blocking) we should check if we are still running before processing the message
    if (!streamListening) {
      return false;
    }
    try {
      // Consume the data stream
      const streamResult = await client.call(
        'XREAD',
        'COUNT',
        MAX_RANGE_MESSAGES,
        'BLOCK',
        STREAM_BATCH_TIME,
        'STREAMS',
        redisStreamName,
        startEventId,
      ) as any[];
      // Process the event results
      if (streamResult && streamResult.length > 0) {
        const [, results] = streamResult[0];
        const lastElementId = await processStreamResult(results, callback, opts.withInternal);
        startEventId = lastElementId || startEventId;
      } else {
        await processStreamResult([], callback, opts.withInternal);
      }
      const bufferTime = opts.bufferTime ?? 50;
      if (bufferTime > 0 && streamListening) {
        await wait(bufferTime);
      }
    } catch (err) {
      // During shutdown, connection errors are expected (client is disconnected to cancel blocking XREAD)
      if (!streamListening) {
        return false;
      }
      logApp.error('Redis stream consume fail', { cause: err, provider });
      if (opts.autoReconnect) {
        await waitInSec(5);
      } else {
        return false;
      }
    }
    return streamListening;
  };
  const processingLoop = async () => {
    while (streamListening) {
      if (!(await processStep())) {
        streamListening = false;
        break;
      }
    }
  };
  return {
    info: async () => rawFetchStreamInfo(streamName),
    running: () => streamListening,
    start: async (start = 'live') => {
      if (streamListening) {
        let fromStart = start;
        if (isEmptyField(fromStart)) {
          fromStart = 'live';
        }
        startEventId = fromStart === 'live' ? '$' : fromStart;
        logApp.info('[STREAM] Starting stream processor', { provider, startEventId });
        processingLoopPromise = (async () => {
          client = await createRedisClient(provider, opts.autoReconnect); // Create client for this processing loop
          try {
            await processingLoop();
          } finally {
            logApp.info('[STREAM] Stream processor terminated, closing Redis client');
            client.disconnect();
          }
        })();
      }
    },
    shutdown: async () => {
      logApp.info('[STREAM] Shutdown stream processor', { provider });
      streamListening = false;
      // Disconnect the Redis client to immediately cancel any blocking XREAD
      if (client) {
        client.disconnect();
      }
      if (processingLoopPromise) {
        await processingLoopPromise;
      }
      logApp.info('[STREAM] Stream processor current promise terminated', { provider });
    },
  };
};
// endregion

// region fetch stream event range
const rawFetchStreamEventsRangeFromEventId = async (
  startEventId: string,
  callback: (events: Array<SseEvent<DataEvent>>, lastEventId: string) => void,
  opts: FetchEventRangeOption = {},
) => {
  const { streamBatchSize = MAX_RANGE_MESSAGES, streamName = LIVE_STREAM_NAME, withInternal } = opts;
  const redisStreamName = convertStreamName(streamName);
  let effectiveStartEventId = startEventId;
  const redisClient = getClientXRANGE();
  try {
    // Consume streamBatchSize number of stream events from startEventId (excluded)
    const streamResult = await redisClient.call(
      'XRANGE',
      redisStreamName,
      `(${startEventId}`, // ( prefix to exclude startEventId
      '+',
      'COUNT',
      streamBatchSize,
    ) as any[];
    // Process the event results
    if (streamResult && streamResult.length > 0) {
      const lastStreamResultId = R.last(streamResult)[0]; // id of last event fetched (internal or external)
      await processStreamResult(streamResult, callback, withInternal); // process the stream events of the range
      if (lastStreamResultId) {
        effectiveStartEventId = lastStreamResultId;
      }
    } else {
      await processStreamResult([], callback, withInternal);
    }
  } catch (err) {
    logApp.error('Redis stream consume fail', { cause: err });
  }
  return { lastEventId: effectiveStartEventId };
};

// region opencti notification stream
const notificationTrimming = conf.get('redis:notification_trimming') || 50000;
// Number of notification stream entries fetched per XRANGE batch when reading a range (digest computation).
// Reading the whole range in a single pass can exhaust the memory heap when the stream holds a very large number
// of events, so we paginate and let the caller filter/transform each batch incrementally
// (see handleDigestNotifications) instead of materializing the whole range at once.
const notificationRangeBatchSize = conf.get('redis:notification_range_batch_size') || 1000;
const rawStoreNotificationEvent = async <T extends StreamNotifEvent> (event: T) => {
  const eventStreamData = mapJSToStream(event);
  await getClientBase().call('XADD', REDIS_NOTIFICATION_STREAM_NAME, 'MAXLEN', '~', notificationTrimming, '*', ...eventStreamData);
};
// Byte size of a raw XRANGE entry [id, [field, value, ...]]: sum of the already-serialized stored
// strings. Cheaper and more faithful than re-stringifying the parsed object to budget memory.
const rawEntryByteSize = (rawFields: any[]): number => {
  let size = 0;
  for (let i = 0; i < rawFields.length; i += 1) {
    size += Buffer.byteLength(rawFields[i]);
  }
  return size;
};
const rawFetchRangeNotifications = async <T extends StreamNotifEvent> (
  start: Date,
  end: Date,
  // Called for each batch of 'live' notification events (with their stored byte size) in the range.
  // Return false to stop the iteration early.
  callback: (events: Array<SizedNotifEvent<T>>) => Promise<boolean | void> | boolean | void,
): Promise<void> => {
  const client = getClientBase();
  const endId = `${end.getTime()}`;
  let fromId = `${start.getTime()}`;
  let isFirstBatch = true;
  for (;;) {
    // The '(' prefix excludes the cursor entry already processed at the end of the previous batch.
    // cursor is in the form "timestamp - eventCursor"
    const startId = isFirstBatch ? fromId : `(${fromId}`;
    const streamResult = await client.call('XRANGE', REDIS_NOTIFICATION_STREAM_NAME, startId, endId, 'COUNT', notificationRangeBatchSize) as any[];
    if (!streamResult || streamResult.length === 0) {
      break;
    }
    const events: Array<SizedNotifEvent<T>> = [];
    for (let i = 0; i < streamResult.length; i += 1) {
      const parsed = mapStreamToJS(streamResult[i]);
      if (parsed.event === 'live') {
        events.push({ event: parsed.data as T, byteSize: rawEntryByteSize(streamResult[i][1]) });
      }
    }
    if (events.length > 0) {
      const shouldContinue = await callback(events);
      if (shouldContinue === false) {
        break;
      }
    }
    // A short batch means the range is exhausted, no need for an extra empty XRANGE call.
    if (streamResult.length < notificationRangeBatchSize) {
      break;
    }
    fromId = R.last(streamResult)[0];
    isFirstBatch = false;
  }
};
// endregion

// region opencti audit stream
const auditTrimming = conf.get('redis:activity_trimming') || 50000;
const rawStoreActivityEvent = async (event: ActivityStreamEvent) => {
  const eventStreamData = mapJSToStream(event);
  await getClientBase().call('XADD', REDIS_ACTIVITY_STREAM_NAME, 'MAXLEN', '~', auditTrimming, '*', ...eventStreamData);
};
// endregion

// region stream production rate tracking
const RATE_SAMPLE_INTERVAL_MS = 10000; // Cache production rate for 10 seconds
let lastSampleTime: number = 0;
let lastSampleLastId: string = '';
let lastSampleStreamSize: number = 0;
let cachedProductionRate: number = 0;

export const getStreamProductionRate = async (): Promise<number> => {
  const now = Date.now();
  if (now - lastSampleTime < RATE_SAMPLE_INTERVAL_MS && lastSampleTime > 0) {
    return roundRate(cachedProductionRate);
  }
  try {
    const info = await rawFetchStreamInfo();
    if (lastSampleTime > 0 && lastSampleLastId) {
      const timeDelta = (now - lastSampleTime) / 1000;
      if (timeDelta > 0) {
        const lastIdTime = parseInt(info.lastEventId.split('-')[0], 10);
        const prevIdTime = parseInt(lastSampleLastId.split('-')[0], 10);
        const eventTimeDelta = (lastIdTime - prevIdTime) / 1000;
        // Use size delta as primary metric when available
        const sizeDelta = info.streamSize - lastSampleStreamSize;
        if (sizeDelta > 0 && eventTimeDelta > 0) {
          // Stream grew: rate = new events / time elapsed in event timestamps
          cachedProductionRate = sizeDelta / eventTimeDelta;
        } else if (eventTimeDelta > 0) {
          // Stream at max size (trimming active): estimate from timestamp progression
          // When trimming is active, the stream size stays roughly constant
          // so we use the time progression of event IDs
          cachedProductionRate = Math.max(0, sizeDelta / timeDelta);
        } else {
          cachedProductionRate = 0;
        }
      }
    }
    lastSampleTime = now;
    lastSampleLastId = info.lastEventId;
    lastSampleStreamSize = info.streamSize;
  } catch (err) {
    logApp.error('Failed to compute stream production rate', { cause: err });
  }
  return roundRate(cachedProductionRate);
};
// endregion

export const rawRedisStreamClient: RawStreamClient = {
  initializeStreams: async () => {},
  rawPushToStream,
  rawFetchStreamInfo,
  rawCreateStreamProcessor,
  rawFetchStreamEventsRangeFromEventId,
  rawStoreNotificationEvent,
  rawFetchRangeNotifications,
  rawStoreActivityEvent,
};
