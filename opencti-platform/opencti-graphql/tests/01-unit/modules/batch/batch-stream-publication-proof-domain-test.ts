import { beforeEach, describe, expect, it, vi } from 'vitest';
import {
  rawAppendOrReturnLiveStreamPublicationProof,
  rawReadLiveStreamPublicationProof,
  RedisStreamPublicationProofAppendResult,
  type RawAppendOrReturnLiveStreamPublicationProofInput,
} from '../../../../src/database/redis-stream';
import {
  appendOrReturnBatchStreamPublicationProof,
  BATCH_STREAM_PUBLICATION_PROOF_MAX_ENTRIES,
  BATCH_STREAM_PUBLICATION_PROOF_MAX_SERIALIZED_BYTES,
  buildBatchStreamPublicationEventFingerprint,
  buildBatchStreamPublicationId,
  buildBatchStreamPublicationKey,
  readBatchStreamPublicationProof,
  type ProofableStreamDataEvent,
} from '../../../../src/modules/batch/batch-stream-publication-proof-domain';
import { BatchAdmissionErrorCode, type BatchStreamPublicationProof } from '../../../../src/modules/batch/batch-types';

vi.mock('../../../../src/database/redis-stream', async (importOriginal) => {
  const actual = await importOriginal<typeof import('../../../../src/database/redis-stream')>();
  return {
    ...actual,
    rawAppendOrReturnLiveStreamPublicationProof: vi.fn(),
    rawReadLiveStreamPublicationProof: vi.fn(),
  };
});

const deliveryId = 'batch-delivery--stream-publication-proof-test';
const event = {
  data: {
    id: 'indicator--stream-publication-proof-test',
    name: 'example',
    type: 'indicator',
  },
  event_id: 'event-id-1',
  message: 'Create indicator',
  origin: {
    applicant_id: 'connector--stream-publication-proof-test',
  },
  scope: 'external',
  type: 'create',
  version: '4',
} as unknown as ProofableStreamDataEvent;

describe('batch stream publication proof domain', () => {
  const storedProofs = new Map<string, string>();
  const streamEntries: string[] = [];

  const buildStorageKey = (proofDeliveryId: string, publicationId: string) => `${proofDeliveryId}:${publicationId}`;

  const appendOrReturn = async (
    input: RawAppendOrReturnLiveStreamPublicationProofInput,
  ) => {
    const key = buildStorageKey(input.deliveryId, input.publicationId);
    const existingRawProof = storedProofs.get(key);
    if (existingRawProof) {
      const existingProof = JSON.parse(existingRawProof) as BatchStreamPublicationProof;
      if (existingProof.event_fingerprint !== input.eventFingerprint) {
        return {
          result: RedisStreamPublicationProofAppendResult.Conflict,
          rawProof: null,
        };
      }
      return {
        result: RedisStreamPublicationProofAppendResult.Existing,
        rawProof: existingRawProof,
      };
    }
    const proofEntries = Array.from(storedProofs.entries())
      .filter(([storedKey]) => storedKey.startsWith(`${input.deliveryId}:`));
    if (proofEntries.length >= input.maxEntries) {
      return {
        result: RedisStreamPublicationProofAppendResult.EntryLimitExceeded,
        rawProof: null,
      };
    }
    const streamEntryId = `${streamEntries.length + 1}-0`;
    const proof: BatchStreamPublicationProof = {
      publication_id: input.publicationId,
      event_fingerprint: input.eventFingerprint,
      stream_entry_id: streamEntryId,
      published_at: input.publishedAt,
      proof_version: input.proofVersion,
    };
    const rawProof = JSON.stringify(proof);
    const serializedBytes = proofEntries.reduce(
      (total, [publicationId, existing]) => total + Buffer.byteLength(publicationId) + Buffer.byteLength(existing),
      Buffer.byteLength(input.publicationId) + Buffer.byteLength(rawProof),
    );
    if (serializedBytes > input.maxSerializedBytes) {
      return {
        result: RedisStreamPublicationProofAppendResult.SerializedByteLimitExceeded,
        rawProof: null,
      };
    }
    streamEntries.push(streamEntryId);
    storedProofs.set(key, rawProof);
    return {
      result: RedisStreamPublicationProofAppendResult.Appended,
      rawProof,
    };
  };

  beforeEach(() => {
    vi.clearAllMocks();
    storedProofs.clear();
    streamEntries.length = 0;
    vi.mocked(rawAppendOrReturnLiveStreamPublicationProof).mockImplementation(appendOrReturn);
    vi.mocked(rawReadLiveStreamPublicationProof).mockImplementation(async (proofDeliveryId, publicationId) => {
      return storedProofs.get(buildStorageKey(proofDeliveryId, publicationId)) ?? null;
    });
  });

  it('builds deterministic coalesced publication identity and canonical event fingerprints', () => {
    const equivalentEvent = {
      version: '4',
      type: 'create',
      scope: 'external',
      message: 'Create indicator',
      origin: {
        applicant_id: 'connector--stream-publication-proof-test',
      },
      event_id: 'event-id-1',
      data: {
        type: 'indicator',
        name: 'example',
        id: 'indicator--stream-publication-proof-test',
      },
    } as unknown as ProofableStreamDataEvent;
    const publicationKey = buildBatchStreamPublicationKey(event);

    expect(publicationKey).toBe('external:indicator--stream-publication-proof-test');
    expect(buildBatchStreamPublicationId(deliveryId, 0, publicationKey))
      .toBe(buildBatchStreamPublicationId(deliveryId, 0, publicationKey));
    expect(buildBatchStreamPublicationId(deliveryId, 1, publicationKey))
      .not.toBe(buildBatchStreamPublicationId(deliveryId, 0, publicationKey));
    expect(buildBatchStreamPublicationEventFingerprint(equivalentEvent))
      .toBe(buildBatchStreamPublicationEventFingerprint(event));
    expect(buildBatchStreamPublicationEventFingerprint({
      ...event,
      data: {
        ...event.data,
        name: 'changed',
      },
    } as unknown as ProofableStreamDataEvent)).not.toBe(buildBatchStreamPublicationEventFingerprint(event));
  });

  it('rejects non-coalesced event shapes before a proof can be attempted', () => {
    expect(() => buildBatchStreamPublicationKey({
      ...event,
      type: 'merge',
    } as unknown as ProofableStreamDataEvent)).toThrowError('Batch stream publication proof only supports coalesced create, update, and delete events');
    expect(() => buildBatchStreamPublicationId(deliveryId, -1, buildBatchStreamPublicationKey(event)))
      .toThrowError('Invalid batch stream publication sequence');
    expect(() => buildBatchStreamPublicationId(deliveryId, 0, `external:${'\u00e9'.repeat(260)}`))
      .toThrowError('Invalid batch stream publication proof identity');
  });

  it('returns one stable proof for same-input retries and reads it back without another append', async () => {
    const publicationId = buildBatchStreamPublicationId(deliveryId, 0, buildBatchStreamPublicationKey(event));
    const eventFingerprint = buildBatchStreamPublicationEventFingerprint(event);
    const input = { deliveryId, event, eventFingerprint, publicationId };

    const first = await appendOrReturnBatchStreamPublicationProof(input, {
      publishedAt: '2026-08-08T00:00:00.000Z',
    });
    const retry = await appendOrReturnBatchStreamPublicationProof(input, {
      publishedAt: '2026-08-08T00:00:01.000Z',
    });

    expect(first).toStrictEqual(retry);
    expect(first.stream_entry_id).toBe('1-0');
    expect(streamEntries).toStrictEqual(['1-0']);
    expect(await readBatchStreamPublicationProof(deliveryId, publicationId)).toStrictEqual(first);
  });

  it('fails closed when one publication identity is reused for changed event content', async () => {
    const publicationId = buildBatchStreamPublicationId(deliveryId, 0, buildBatchStreamPublicationKey(event));
    await appendOrReturnBatchStreamPublicationProof({
      deliveryId,
      event,
      eventFingerprint: buildBatchStreamPublicationEventFingerprint(event),
      publicationId,
    }, {
      publishedAt: '2026-08-08T00:00:00.000Z',
    });
    const changedEvent = {
      ...event,
      data: {
        ...event.data,
        name: 'changed',
      },
    } as unknown as ProofableStreamDataEvent;

    await expect(appendOrReturnBatchStreamPublicationProof({
      deliveryId,
      event: changedEvent,
      eventFingerprint: buildBatchStreamPublicationEventFingerprint(changedEvent),
      publicationId,
    })).rejects.toMatchObject({
      extensions: {
        data: {
          batch_error_code: BatchAdmissionErrorCode.StreamPublicationProofConflict,
        },
      },
    });
    expect(streamEntries).toStrictEqual(['1-0']);
  });

  it('rejects a caller-supplied fingerprint that does not match the event before Redis is touched', async () => {
    const publicationId = buildBatchStreamPublicationId(deliveryId, 0, buildBatchStreamPublicationKey(event));

    await expect(appendOrReturnBatchStreamPublicationProof({
      deliveryId,
      event: {
        ...event,
        data: {
          ...event.data,
          name: 'changed',
        },
      } as unknown as ProofableStreamDataEvent,
      eventFingerprint: buildBatchStreamPublicationEventFingerprint(event),
      publicationId,
    })).rejects.toThrowError('Batch stream publication proof event fingerprint does not match the event payload');
    expect(rawAppendOrReturnLiveStreamPublicationProof).not.toHaveBeenCalled();
  });

  it('fails closed on malformed proof payloads and hard cap overruns', async () => {
    const publicationId = buildBatchStreamPublicationId(deliveryId, 0, buildBatchStreamPublicationKey(event));
    const eventFingerprint = buildBatchStreamPublicationEventFingerprint(event);
    storedProofs.set(buildStorageKey(deliveryId, publicationId), JSON.stringify({
      event_fingerprint: eventFingerprint,
      publication_id: publicationId,
      proof_version: 1,
      raw_event_body: event,
      stream_entry_id: '1-0',
      published_at: '2026-08-08T00:00:00.000Z',
    }));

    await expect(readBatchStreamPublicationProof(deliveryId, publicationId))
      .rejects.toThrowError('Batch stream publication proof payload is malformed');
    storedProofs.clear();

    await appendOrReturnBatchStreamPublicationProof({
      deliveryId,
      event,
      eventFingerprint,
      publicationId,
    }, {
      maxEntries: 1,
      publishedAt: '2026-08-08T00:00:00.000Z',
    });
    const secondEvent = {
      ...event,
      data: {
        ...event.data,
        id: 'indicator--stream-publication-proof-test-2',
      },
    } as unknown as ProofableStreamDataEvent;
    await expect(appendOrReturnBatchStreamPublicationProof({
      deliveryId,
      event: secondEvent,
      eventFingerprint: buildBatchStreamPublicationEventFingerprint(secondEvent),
      publicationId: buildBatchStreamPublicationId(deliveryId, 1, buildBatchStreamPublicationKey(secondEvent)),
    }, {
      maxEntries: 1,
    })).rejects.toThrowError('Batch stream publication proof entry cap exceeded');

    await expect(appendOrReturnBatchStreamPublicationProof({
      deliveryId: `${deliveryId}-bytes`,
      event,
      eventFingerprint,
      publicationId,
    }, {
      maxSerializedBytes: 1,
    })).rejects.toThrowError('Batch stream publication proof serialized byte cap exceeded');
    await expect(appendOrReturnBatchStreamPublicationProof({
      deliveryId,
      event,
      eventFingerprint,
      publicationId,
    }, {
      maxEntries: BATCH_STREAM_PUBLICATION_PROOF_MAX_ENTRIES + 1,
    })).rejects.toThrowError('Batch stream publication proof limit exceeds the hard cap');
    await expect(appendOrReturnBatchStreamPublicationProof({
      deliveryId,
      event,
      eventFingerprint,
      publicationId,
    }, {
      maxSerializedBytes: BATCH_STREAM_PUBLICATION_PROOF_MAX_SERIALIZED_BYTES + 1,
    })).rejects.toThrowError('Batch stream publication proof limit exceeds the hard cap');
  });
});
