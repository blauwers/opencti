import { afterEach, describe, expect, it } from 'vitest';
import { v4 as uuid } from 'uuid';
import { REDIS_PREFIX } from '../../../src/config/conf';
import { getClientBase } from '../../../src/database/redis';
import {
  buildRedisStreamPublicationProofContainerKey,
  rawAppendOrReturnLiveStreamPublicationProof,
  rawReadLiveStreamPublicationProof,
  RedisStreamPublicationProofAppendResult,
  type RawAppendOrReturnLiveStreamPublicationProofInput,
} from '../../../src/database/redis-stream';
import { LIVE_STREAM_NAME } from '../../../src/database/stream/stream-utils';
import { STIX_EXT_OCTI } from '../../../src/types/stix-2-1-extensions';

const liveStreamKey = `${REDIS_PREFIX}${LIVE_STREAM_NAME}`;
const trackedProofContainerKeys = new Set<string>();
const trackedStreamEntryIds = new Set<string>();

const buildInput = (
  deliveryId: string,
  publicationId: string,
  eventFingerprint: string,
  dataId: string,
): RawAppendOrReturnLiveStreamPublicationProofInput => ({
  deliveryId,
  publicationId,
  eventFingerprint,
  publishedAt: '2026-08-08T00:00:00.000Z',
  proofVersion: 1,
  maxEntries: 4,
  maxSerializedBytes: 4096,
  event: {
    data: {
      confidence: 100,
      created: '2026-08-08T00:00:00.000Z',
      extensions: {
        [STIX_EXT_OCTI]: {
          created_at: '2026-08-08T00:00:00.000Z',
          extension_type: 'property-extension',
          granted_refs: [],
          granted_refs_ids: [],
          id: dataId,
          is_inferred: false,
          type: 'Indicator',
          updated_at: '2026-08-08T00:00:00.000Z',
        },
      },
      id: dataId,
      lang: 'en',
      labels: [],
      modified: '2026-08-08T00:00:00.000Z',
      pattern: '[ipv4-addr:value = \'198.51.100.1\']',
      pattern_type: 'stix',
      revoked: false,
      spec_version: '2.1',
      type: 'indicator',
      valid_from: '2026-08-08T00:00:00.000Z',
    },
    message: 'Create indicator',
    noHistory: true,
    origin: {},
    scope: 'external',
    type: 'create',
    version: '4',
  } as any,
});

const trackResult = (deliveryId: string, rawProof: string | null) => {
  trackedProofContainerKeys.add(buildRedisStreamPublicationProofContainerKey(deliveryId));
  if (!rawProof) {
    return;
  }
  const proof = JSON.parse(rawProof) as { stream_entry_id?: unknown };
  if (typeof proof.stream_entry_id === 'string') {
    trackedStreamEntryIds.add(proof.stream_entry_id);
  }
};

const getLiveStreamLength = async (): Promise<number> => {
  return Number(await getClientBase().call('XLEN', liveStreamKey));
};

afterEach(async () => {
  if (trackedStreamEntryIds.size > 0) {
    await getClientBase().call('XDEL', liveStreamKey, ...trackedStreamEntryIds);
  }
  if (trackedProofContainerKeys.size > 0) {
    await getClientBase().call('DEL', ...trackedProofContainerKeys);
  }
  trackedProofContainerKeys.clear();
  trackedStreamEntryIds.clear();
});

describe('redis stream publication proof script', () => {
  it('appends once, returns the same proof on retry, rejects changed fingerprints, and reads back proof metadata only', async () => {
    const deliveryId = `batch-delivery--redis-proof-${uuid()}`;
    const publicationId = 'a'.repeat(64);
    const input = buildInput(deliveryId, publicationId, 'b'.repeat(64), 'indicator--redis-proof');
    const beforeLength = await getLiveStreamLength();

    const first = await rawAppendOrReturnLiveStreamPublicationProof(input);
    trackResult(deliveryId, first.rawProof);
    const retry = await rawAppendOrReturnLiveStreamPublicationProof({
      ...input,
      publishedAt: '2026-08-08T00:00:01.000Z',
    });
    trackResult(deliveryId, retry.rawProof);
    const conflict = await rawAppendOrReturnLiveStreamPublicationProof({
      ...input,
      eventFingerprint: 'c'.repeat(64),
    });
    const readback = await rawReadLiveStreamPublicationProof(deliveryId, publicationId);
    const afterLength = await getLiveStreamLength();

    expect(first.result).toBe(RedisStreamPublicationProofAppendResult.Appended);
    expect(retry.result).toBe(RedisStreamPublicationProofAppendResult.Existing);
    expect(retry.rawProof).toBe(first.rawProof);
    expect(conflict).toStrictEqual({
      result: RedisStreamPublicationProofAppendResult.Conflict,
      rawProof: null,
    });
    expect(readback).toBe(first.rawProof);
    expect(afterLength - beforeLength).toBe(1);
    expect(JSON.parse(first.rawProof as string)).toStrictEqual({
      event_fingerprint: 'b'.repeat(64),
      proof_version: 1,
      publication_id: publicationId,
      published_at: '2026-08-08T00:00:00.000Z',
      stream_entry_id: expect.stringMatching(/^\d+-\d+$/),
    });
  });

  it('fails closed before append when the proof entry or serialized byte caps are exceeded', async () => {
    const entryCapDeliveryId = `batch-delivery--redis-proof-entry-cap-${uuid()}`;
    const firstEntryInput = {
      ...buildInput(entryCapDeliveryId, 'd'.repeat(64), 'e'.repeat(64), 'indicator--redis-proof-entry-cap-1'),
      maxEntries: 1,
    };
    const beforeEntryCapLength = await getLiveStreamLength();
    const first = await rawAppendOrReturnLiveStreamPublicationProof(firstEntryInput);
    trackResult(entryCapDeliveryId, first.rawProof);
    const entryCapResult = await rawAppendOrReturnLiveStreamPublicationProof({
      ...buildInput(entryCapDeliveryId, 'f'.repeat(64), '0'.repeat(64), 'indicator--redis-proof-entry-cap-2'),
      maxEntries: 1,
    });
    const afterEntryCapLength = await getLiveStreamLength();

    const byteCapDeliveryId = `batch-delivery--redis-proof-byte-cap-${uuid()}`;
    const beforeByteCapLength = await getLiveStreamLength();
    const byteCapResult = await rawAppendOrReturnLiveStreamPublicationProof({
      ...buildInput(byteCapDeliveryId, '1'.repeat(64), '2'.repeat(64), 'indicator--redis-proof-byte-cap'),
      maxSerializedBytes: 1,
    });
    trackedProofContainerKeys.add(buildRedisStreamPublicationProofContainerKey(byteCapDeliveryId));
    const afterByteCapLength = await getLiveStreamLength();

    const malformedDeliveryId = `batch-delivery--redis-proof-malformed-${uuid()}`;
    const beforeMalformedLength = await getLiveStreamLength();
    const malformedResult = await rawAppendOrReturnLiveStreamPublicationProof({
      ...buildInput(malformedDeliveryId, 'not-a-canonical-publication-id', '3'.repeat(64), 'indicator--redis-proof-malformed'),
    });
    trackedProofContainerKeys.add(buildRedisStreamPublicationProofContainerKey(malformedDeliveryId));
    const afterMalformedLength = await getLiveStreamLength();

    expect(first.result).toBe(RedisStreamPublicationProofAppendResult.Appended);
    expect(entryCapResult).toStrictEqual({
      result: RedisStreamPublicationProofAppendResult.EntryLimitExceeded,
      rawProof: null,
    });
    expect(afterEntryCapLength - beforeEntryCapLength).toBe(1);
    expect(byteCapResult).toStrictEqual({
      result: RedisStreamPublicationProofAppendResult.SerializedByteLimitExceeded,
      rawProof: null,
    });
    expect(afterByteCapLength - beforeByteCapLength).toBe(0);
    expect(malformedResult).toStrictEqual({
      result: RedisStreamPublicationProofAppendResult.Malformed,
      rawProof: null,
    });
    expect(afterMalformedLength - beforeMalformedLength).toBe(0);
  });
});
