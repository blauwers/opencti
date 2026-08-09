import jsonCanonicalize from 'canonicalize';
import { FunctionalError } from '../../config/errors';
import {
  rawAppendOrReturnLiveStreamPublicationProof,
  rawReadLiveStreamPublicationProof,
  REDIS_STREAM_PUBLICATION_PROOF_MAX_ENTRIES,
  REDIS_STREAM_PUBLICATION_PROOF_MAX_SERIALIZED_BYTES,
  RedisStreamPublicationProofAppendResult,
} from '../../database/redis-stream';
import type { StreamDataEvent } from '../../types/event';
import { now } from '../../utils/format';
import { hashSHA256 } from '../../utils/hash';
import { BatchAdmissionErrorCode, type BatchStreamPublicationProof } from './batch-types';

const BATCH_STREAM_PUBLICATION_ID_SCOPE = 'batch-stream-publication:';
const BATCH_STREAM_PUBLICATION_PROOF_VERSION = 1;
const BATCH_STREAM_PUBLICATION_DELIVERY_ID_MAX_LENGTH = 128;
const BATCH_STREAM_PUBLICATION_KEY_MAX_BYTES = 512;
const SHA256_HEX_PATTERN = /^[a-f0-9]{64}$/;
const STREAM_ENTRY_ID_PATTERN = /^\d+-\d+$/;
const BATCH_STREAM_PUBLICATION_PROOF_FIELDS = new Set([
  'publication_id',
  'event_fingerprint',
  'stream_entry_id',
  'published_at',
  'proof_version',
]);

export const BATCH_STREAM_PUBLICATION_PROOF_MAX_ENTRIES = REDIS_STREAM_PUBLICATION_PROOF_MAX_ENTRIES;
export const BATCH_STREAM_PUBLICATION_PROOF_MAX_SERIALIZED_BYTES = REDIS_STREAM_PUBLICATION_PROOF_MAX_SERIALIZED_BYTES;

export type ProofableStreamDataEvent = StreamDataEvent & {
  event_id?: string;
};

export interface AppendOrReturnBatchStreamPublicationProofInput {
  deliveryId: string;
  publicationId: string;
  eventFingerprint: string;
  event: ProofableStreamDataEvent;
}

export interface AppendOrReturnBatchStreamPublicationProofOptions {
  maxEntries?: number;
  maxSerializedBytes?: number;
  publishedAt?: string;
}

const streamPublicationProofConflict = (message: string, data: Record<string, unknown> = {}) => {
  return FunctionalError(message, {
    batch_error_code: BatchAdmissionErrorCode.StreamPublicationProofConflict,
    ...data,
  });
};

const canonicalizeOrThrow = (value: unknown, message: string): string => {
  const canonicalValue = jsonCanonicalize(value);
  if (typeof canonicalValue !== 'string') {
    throw streamPublicationProofConflict(message);
  }
  return canonicalValue;
};

const isNonEmptyString = (value: unknown): value is string => typeof value === 'string' && value.length > 0;

const assertBoundedNonEmptyString = (value: string, field: string, maxBytes: number) => {
  if (!isNonEmptyString(value) || Buffer.byteLength(value) > maxBytes) {
    throw streamPublicationProofConflict('Invalid batch stream publication proof identity', {
      field,
      value,
    });
  }
};

const assertSha256Hex = (value: string, field: string) => {
  if (!SHA256_HEX_PATTERN.test(value)) {
    throw streamPublicationProofConflict('Invalid batch stream publication proof fingerprint', {
      field,
      value,
    });
  }
};

const assertPositiveInteger = (value: number, field: string) => {
  if (!Number.isInteger(value) || value <= 0) {
    throw streamPublicationProofConflict('Invalid batch stream publication proof limit', {
      field,
      value,
    });
  }
};

const normalizeBoundedLimit = (value: number | undefined, hardCap: number, field: string): number => {
  if (value === undefined) {
    return hardCap;
  }
  assertPositiveInteger(value, field);
  if (value > hardCap) {
    throw streamPublicationProofConflict('Batch stream publication proof limit exceeds the hard cap', {
      field,
      hard_cap: hardCap,
      value,
    });
  }
  return value;
};

const assertPublicationSequence = (publicationSequence: number) => {
  if (!Number.isInteger(publicationSequence) || publicationSequence < 0) {
    throw streamPublicationProofConflict('Invalid batch stream publication sequence', {
      publication_sequence: publicationSequence,
    });
  }
};

const assertPublishedAt = (publishedAt: string) => {
  if (!isNonEmptyString(publishedAt) || Number.isNaN(Date.parse(publishedAt))) {
    throw streamPublicationProofConflict('Invalid batch stream publication proof timestamp', {
      published_at: publishedAt,
    });
  }
};

export const buildBatchStreamPublicationKey = (event: ProofableStreamDataEvent): string => {
  const data = event?.data;
  if (
    !data
    || !isNonEmptyString(data.id)
    || !['create', 'delete', 'update'].includes(event.type)
    || !['external', 'internal'].includes(event.scope)
  ) {
    throw streamPublicationProofConflict('Batch stream publication proof only supports coalesced create, update, and delete events');
  }
  const publicationKey = `${event.scope}:${data.id}`;
  if (Buffer.byteLength(publicationKey) > BATCH_STREAM_PUBLICATION_KEY_MAX_BYTES) {
    throw streamPublicationProofConflict('Batch stream publication proof key exceeds the bounded key size', {
      publication_key_bytes: Buffer.byteLength(publicationKey),
      max_publication_key_bytes: BATCH_STREAM_PUBLICATION_KEY_MAX_BYTES,
    });
  }
  return publicationKey;
};

export const buildBatchStreamPublicationId = (
  deliveryId: string,
  publicationSequence: number,
  publicationKey: string,
): string => {
  assertBoundedNonEmptyString(deliveryId, 'delivery_id', BATCH_STREAM_PUBLICATION_DELIVERY_ID_MAX_LENGTH);
  assertPublicationSequence(publicationSequence);
  assertBoundedNonEmptyString(publicationKey, 'publication_key', BATCH_STREAM_PUBLICATION_KEY_MAX_BYTES);
  return hashSHA256(`${BATCH_STREAM_PUBLICATION_ID_SCOPE}${deliveryId}:${publicationSequence}:${publicationKey}`);
};

export const buildBatchStreamPublicationEventFingerprint = (event: ProofableStreamDataEvent): string => {
  buildBatchStreamPublicationKey(event);
  return hashSHA256(canonicalizeOrThrow(event, 'Invalid batch stream publication event payload'));
};

const assertBatchStreamPublicationProofShape = (value: unknown): BatchStreamPublicationProof => {
  if (!value || typeof value !== 'object') {
    throw streamPublicationProofConflict('Batch stream publication proof payload is malformed');
  }
  const proof = value as Partial<BatchStreamPublicationProof>;
  const unexpectedFields = Object.keys(proof).filter((field) => !BATCH_STREAM_PUBLICATION_PROOF_FIELDS.has(field));
  if (
    unexpectedFields.length > 0
    || !isNonEmptyString(proof.publication_id)
    || !SHA256_HEX_PATTERN.test(proof.publication_id)
    || !isNonEmptyString(proof.event_fingerprint)
    || !SHA256_HEX_PATTERN.test(proof.event_fingerprint)
    || !isNonEmptyString(proof.stream_entry_id)
    || !STREAM_ENTRY_ID_PATTERN.test(proof.stream_entry_id)
    || !isNonEmptyString(proof.published_at)
    || Number.isNaN(Date.parse(proof.published_at))
    || proof.proof_version !== BATCH_STREAM_PUBLICATION_PROOF_VERSION
  ) {
    throw streamPublicationProofConflict('Batch stream publication proof payload is malformed', {
      unexpected_fields: unexpectedFields,
    });
  }
  return proof as BatchStreamPublicationProof;
};

const parseBatchStreamPublicationProof = (rawProof: string): BatchStreamPublicationProof => {
  try {
    return assertBatchStreamPublicationProofShape(JSON.parse(rawProof));
  } catch (cause) {
    if ((cause as { extensions?: { data?: { batch_error_code?: string } } })?.extensions?.data?.batch_error_code === BatchAdmissionErrorCode.StreamPublicationProofConflict) {
      throw cause;
    }
    throw streamPublicationProofConflict('Batch stream publication proof payload is malformed', { cause });
  }
};

const assertBatchStreamPublicationProofIdentity = (
  proof: BatchStreamPublicationProof,
  publicationId: string,
  eventFingerprint?: string,
) => {
  const conflictingFields = Object.entries({
    publication_id: proof.publication_id !== publicationId,
    event_fingerprint: eventFingerprint !== undefined && proof.event_fingerprint !== eventFingerprint,
    proof_version: proof.proof_version !== BATCH_STREAM_PUBLICATION_PROOF_VERSION,
  }).filter(([, conflict]) => conflict).map(([field]) => field);
  if (conflictingFields.length > 0) {
    throw streamPublicationProofConflict('Batch stream publication proof is already associated with different immutable publication data', {
      publication_id: publicationId,
      conflicting_fields: conflictingFields,
    });
  }
};

export const readBatchStreamPublicationProof = async (
  deliveryId: string,
  publicationId: string,
): Promise<BatchStreamPublicationProof | null> => {
  assertBoundedNonEmptyString(deliveryId, 'delivery_id', BATCH_STREAM_PUBLICATION_DELIVERY_ID_MAX_LENGTH);
  assertSha256Hex(publicationId, 'publication_id');
  const rawProof = await rawReadLiveStreamPublicationProof(deliveryId, publicationId);
  if (!rawProof) {
    return null;
  }
  const proof = parseBatchStreamPublicationProof(rawProof);
  assertBatchStreamPublicationProofIdentity(proof, publicationId);
  return proof;
};

const assertRawAppendResult = (
  result: RedisStreamPublicationProofAppendResult,
  publicationId: string,
): void => {
  switch (result) {
    case RedisStreamPublicationProofAppendResult.Malformed:
      throw streamPublicationProofConflict('Batch stream publication proof storage contains malformed data', {
        publication_id: publicationId,
      });
    case RedisStreamPublicationProofAppendResult.Conflict:
      throw streamPublicationProofConflict('Batch stream publication proof is already associated with a different event fingerprint', {
        publication_id: publicationId,
      });
    case RedisStreamPublicationProofAppendResult.EntryLimitExceeded:
      throw streamPublicationProofConflict('Batch stream publication proof entry cap exceeded', {
        publication_id: publicationId,
      });
    case RedisStreamPublicationProofAppendResult.SerializedByteLimitExceeded:
      throw streamPublicationProofConflict('Batch stream publication proof serialized byte cap exceeded', {
        publication_id: publicationId,
      });
    case RedisStreamPublicationProofAppendResult.Appended:
    case RedisStreamPublicationProofAppendResult.Existing:
      return;
    default:
      throw streamPublicationProofConflict('Batch stream publication proof storage returned an invalid append result', {
        publication_id: publicationId,
        result,
      });
  }
};

export const appendOrReturnBatchStreamPublicationProof = async (
  input: AppendOrReturnBatchStreamPublicationProofInput,
  options: AppendOrReturnBatchStreamPublicationProofOptions = {},
): Promise<BatchStreamPublicationProof> => {
  assertBoundedNonEmptyString(input.deliveryId, 'delivery_id', BATCH_STREAM_PUBLICATION_DELIVERY_ID_MAX_LENGTH);
  assertSha256Hex(input.publicationId, 'publication_id');
  assertSha256Hex(input.eventFingerprint, 'event_fingerprint');
  const expectedEventFingerprint = buildBatchStreamPublicationEventFingerprint(input.event);
  if (expectedEventFingerprint !== input.eventFingerprint) {
    throw streamPublicationProofConflict('Batch stream publication proof event fingerprint does not match the event payload', {
      publication_id: input.publicationId,
      event_fingerprint: input.eventFingerprint,
      expected_event_fingerprint: expectedEventFingerprint,
    });
  }
  const publishedAt = options.publishedAt ?? now();
  assertPublishedAt(publishedAt);
  const result = await rawAppendOrReturnLiveStreamPublicationProof({
    deliveryId: input.deliveryId,
    event: input.event,
    eventFingerprint: input.eventFingerprint,
    maxEntries: normalizeBoundedLimit(options.maxEntries, BATCH_STREAM_PUBLICATION_PROOF_MAX_ENTRIES, 'max_entries'),
    maxSerializedBytes: normalizeBoundedLimit(
      options.maxSerializedBytes,
      BATCH_STREAM_PUBLICATION_PROOF_MAX_SERIALIZED_BYTES,
      'max_serialized_bytes',
    ),
    proofVersion: BATCH_STREAM_PUBLICATION_PROOF_VERSION,
    publicationId: input.publicationId,
    publishedAt,
  });
  assertRawAppendResult(result.result, input.publicationId);
  if (!result.rawProof) {
    throw streamPublicationProofConflict('Batch stream publication proof storage returned no proof payload', {
      publication_id: input.publicationId,
      result: result.result,
    });
  }
  const proof = parseBatchStreamPublicationProof(result.rawProof);
  assertBatchStreamPublicationProofIdentity(proof, input.publicationId, input.eventFingerprint);
  return proof;
};
