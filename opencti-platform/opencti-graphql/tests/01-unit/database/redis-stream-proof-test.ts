import { afterEach, describe, expect, it, vi } from 'vitest';

const mockClient = { call: vi.fn() };
vi.mock('../../../src/database/redis', () => ({
  getClientBase: vi.fn(() => mockClient),
  getClientXRANGE: vi.fn(() => mockClient),
  createRedisClient: vi.fn(),
}));

import {
  buildRedisStreamPublicationProofContainerKey,
  rawAppendOrReturnLiveStreamPublicationProof,
  rawReadLiveStreamPublicationProof,
  REDIS_STREAM_PUBLICATION_PROOF_MAX_ENTRIES,
  REDIS_STREAM_PUBLICATION_PROOF_MAX_SERIALIZED_BYTES,
  RedisStreamPublicationProofAppendResult,
} from '../../../src/database/redis-stream';

describe('redis stream publication proof primitive', () => {
  afterEach(() => {
    vi.clearAllMocks();
  });

  it('uses one stream-slot co-located proof container and returns the script proof payload', async () => {
    const rawProof = JSON.stringify({
      publication_id: 'a'.repeat(64),
      event_fingerprint: 'b'.repeat(64),
      stream_entry_id: '1-0',
      published_at: '2026-08-08T00:00:00.000Z',
      proof_version: 1,
    });
    mockClient.call.mockResolvedValue([RedisStreamPublicationProofAppendResult.Appended, rawProof]);

    const result = await rawAppendOrReturnLiveStreamPublicationProof({
      deliveryId: 'batch-delivery--proof-test',
      publicationId: 'a'.repeat(64),
      eventFingerprint: 'b'.repeat(64),
      publishedAt: '2026-08-08T00:00:00.000Z',
      proofVersion: 1,
      maxEntries: 1024,
      maxSerializedBytes: 524288,
      event: {
        data: { id: 'indicator--proof-test' },
        scope: 'external',
        type: 'create',
        version: '4',
      } as any,
    });

    expect(buildRedisStreamPublicationProofContainerKey('batch-delivery--proof-test'))
      .toBe('{test:stream.opencti}:batch_stream_publication_proof:batch-delivery--proof-test');
    expect(result).toStrictEqual({
      result: RedisStreamPublicationProofAppendResult.Appended,
      rawProof,
    });
    expect(mockClient.call).toHaveBeenCalledTimes(1);
    const call = mockClient.call.mock.calls[0];
    expect(call[0]).toBe('EVAL');
    expect(call[2]).toBe(2);
    expect(call[3]).toBe('test:stream.opencti');
    expect(call[4]).toBe('{test:stream.opencti}:batch_stream_publication_proof:batch-delivery--proof-test');
    expect(call).toContain(JSON.stringify('create'));
    expect(call).toContain(JSON.stringify({ id: 'indicator--proof-test' }));
  });

  it('reads one proof field from the same bounded container', async () => {
    mockClient.call.mockResolvedValue('{"publication_id":"proof"}');

    await expect(rawReadLiveStreamPublicationProof('batch-delivery--proof-test', 'a'.repeat(64)))
      .resolves.toBe('{"publication_id":"proof"}');
    expect(mockClient.call).toHaveBeenCalledWith(
      'HGET',
      '{test:stream.opencti}:batch_stream_publication_proof:batch-delivery--proof-test',
      'a'.repeat(64),
    );
  });

  it('clamps raw proof limits to the primitive hard caps before invoking Redis', async () => {
    mockClient.call.mockResolvedValue([RedisStreamPublicationProofAppendResult.Appended, '{}']);

    await rawAppendOrReturnLiveStreamPublicationProof({
      deliveryId: 'batch-delivery--proof-test',
      publicationId: 'a'.repeat(64),
      eventFingerprint: 'b'.repeat(64),
      publishedAt: '2026-08-08T00:00:00.000Z',
      proofVersion: 1,
      maxEntries: REDIS_STREAM_PUBLICATION_PROOF_MAX_ENTRIES + 1,
      maxSerializedBytes: REDIS_STREAM_PUBLICATION_PROOF_MAX_SERIALIZED_BYTES + 1,
      event: {
        data: { id: 'indicator--proof-test' },
        scope: 'external',
        type: 'create',
        version: '4',
      } as any,
    });

    const call = mockClient.call.mock.calls[0];
    expect(call[9]).toBe(`${REDIS_STREAM_PUBLICATION_PROOF_MAX_ENTRIES}`);
    expect(call[10]).toBe(`${REDIS_STREAM_PUBLICATION_PROOF_MAX_SERIALIZED_BYTES}`);
  });
});
