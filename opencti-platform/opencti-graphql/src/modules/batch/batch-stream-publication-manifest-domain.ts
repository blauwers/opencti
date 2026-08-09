import jsonCanonicalize from 'canonicalize';
import { FunctionalError } from '../../config/errors';
import { elIndex, elLoadById } from '../../database/engine';
import { coalesceBufferedStreamDataEvents } from '../../database/stream/stream-utils';
import { INDEX_INTERNAL_OBJECTS, READ_INDEX_INTERNAL_OBJECTS } from '../../database/utils';
import { lockResources } from '../../lock/master-lock';
import { BASE_TYPE_ENTITY } from '../../schema/general';
import { getParentTypes } from '../../schema/schemaUtils';
import type { AuthContext } from '../../types/user';
import { SYSTEM_USER } from '../../utils/access';
import { now } from '../../utils/format';
import { hashSHA256 } from '../../utils/hash';
import {
  BATCH_STREAM_PUBLICATION_PROOF_MAX_ENTRIES,
  buildBatchStreamPublicationEventFingerprint,
  buildBatchStreamPublicationId,
  buildBatchStreamPublicationKey,
  type ProofableStreamDataEvent,
} from './batch-stream-publication-proof-domain';
import {
  BatchAdmissionErrorCode,
  type BatchExecutionReceipt,
  type BatchStreamPublicationEventSnapshot,
  type BatchStreamPublicationManifest,
  type BatchStreamPublicationManifestEntry,
  ENTITY_TYPE_BATCH_STREAM_PUBLICATION_MANIFEST,
} from './batch-types';

const BATCH_STREAM_PUBLICATION_MANIFEST_ID_SCOPE = 'batch-stream-publication-manifest:';
const BATCH_STREAM_PUBLICATION_MANIFEST_LOCK_PREFIX = 'batch-stream-publication-manifest:';
const BATCH_STREAM_PUBLICATION_DELIVERY_ID_MAX_LENGTH = 128;
const SHA256_HEX_PATTERN = /^[a-f0-9]{64}$/;
const MANIFEST_RECORD_FIELDS = new Set([
  '_id',
  '_index',
  'id',
  'internal_id',
  'sort',
  'standard_id',
  'entity_type',
  'base_type',
  'parent_types',
  'manifest_id',
  'receipt_id',
  'delivery_id',
  'submission_id',
  'request_fingerprint',
  'request_contract_version',
  'manifest_version',
  'manifest_fingerprint',
  'entry_count',
  'serialized_bytes',
  'entries',
  'created_at',
  'updated_at',
]);
const MANIFEST_ENTRY_FIELDS = new Set([
  'publication_sequence',
  'publication_key',
  'publication_id',
  'event_fingerprint',
  'event_snapshot',
  'event_snapshot_bytes',
]);
const BASE_EVENT_FIELDS = new Set([
  'data',
  'event_id',
  'message',
  'noHistory',
  'origin',
  'scope',
  'type',
  'version',
]);
const UPDATE_EVENT_FIELDS = new Set([
  ...BASE_EVENT_FIELDS,
  'commit',
  'context',
]);
const UPDATE_EVENT_CONTEXT_FIELDS = new Set([
  'changes',
  'patch',
  'pir_ids',
  'related_restrictions',
  'reverse_patch',
]);

export const BATCH_STREAM_PUBLICATION_MANIFEST_VERSION = 1;
export const BATCH_STREAM_PUBLICATION_MANIFEST_MAX_ENTRIES = BATCH_STREAM_PUBLICATION_PROOF_MAX_ENTRIES;
export const BATCH_STREAM_PUBLICATION_MANIFEST_MAX_EVENT_SNAPSHOT_BYTES = 64 * 1024;
export const BATCH_STREAM_PUBLICATION_MANIFEST_MAX_SERIALIZED_BYTES = 1024 * 1024;

export enum BatchStreamPublicationManifestIneligibilityReason {
  RawPublication = 'RAW_PUBLICATION',
  MergePublication = 'MERGE_PUBLICATION',
  MissingDataId = 'MISSING_DATA_ID',
  UnsupportedScope = 'UNSUPPORTED_SCOPE',
  UnsupportedType = 'UNSUPPORTED_TYPE',
}

export interface BatchStreamPublicationManifestKeyedBucketSlot {
  kind: 'keyed';
  publicationKey: string;
  events: readonly BatchStreamPublicationEventSnapshot[];
}

export interface BatchStreamPublicationManifestRawSlot {
  kind: 'raw';
  event: unknown;
}

export type BatchStreamPublicationManifestCandidateSlot
  = BatchStreamPublicationManifestKeyedBucketSlot
    | BatchStreamPublicationManifestRawSlot;

export interface BuildBatchStreamPublicationManifestCandidateInput {
  receipt: Pick<
    BatchExecutionReceipt,
    'internal_id' | 'delivery_id' | 'submission_id' | 'request_fingerprint' | 'request_contract_version'
  >;
  slots: readonly BatchStreamPublicationManifestCandidateSlot[];
}

export interface BuildBatchStreamPublicationManifestOptions {
  maxEntries?: number;
  maxEventSnapshotBytes?: number;
  maxSerializedBytes?: number;
}

export interface BatchStreamPublicationManifestDraft {
  manifest_id: string;
  receipt_id: string;
  delivery_id: string;
  submission_id: string;
  request_fingerprint: string;
  request_contract_version: number;
  manifest_version: number;
  manifest_fingerprint: string;
  entry_count: number;
  serialized_bytes: number;
  entries: BatchStreamPublicationManifestEntry[];
}

export type BuildBatchStreamPublicationManifestCandidateResult
  = {
    eligible: false;
    reason: BatchStreamPublicationManifestIneligibilityReason;
    slotIndex: number;
    eventIndex: number | null;
  }
  | {
    eligible: true;
    manifest: BatchStreamPublicationManifestDraft;
  };

type BatchStreamPublicationManifestFingerprintInput = Omit<
  BatchStreamPublicationManifestDraft,
  'manifest_fingerprint'
>;

const batchStreamPublicationManifestConflict = (message: string, data: Record<string, unknown> = {}) => {
  return FunctionalError(message, {
    batch_error_code: BatchAdmissionErrorCode.StreamPublicationManifestConflict,
    ...data,
  });
};

const canonicalizeOrThrow = (value: unknown, message: string): string => {
  const canonicalValue = jsonCanonicalize(value);
  if (typeof canonicalValue !== 'string') {
    throw batchStreamPublicationManifestConflict(message);
  }
  return canonicalValue;
};

const isRecord = (value: unknown): value is Record<string, unknown> => {
  return !!value && typeof value === 'object' && !Array.isArray(value);
};

const isNonEmptyString = (value: unknown): value is string => typeof value === 'string' && value.length > 0;

const assertBoundedNonEmptyString = (value: string, field: string, maxBytes: number) => {
  if (!isNonEmptyString(value) || Buffer.byteLength(value) > maxBytes) {
    throw batchStreamPublicationManifestConflict('Invalid batch stream publication manifest identity', {
      field,
      value,
    });
  }
};

const assertSha256Hex = (value: string, field: string) => {
  if (!SHA256_HEX_PATTERN.test(value)) {
    throw batchStreamPublicationManifestConflict('Invalid batch stream publication manifest fingerprint', {
      field,
      value,
    });
  }
};

const assertNonNegativeInteger = (value: number, field: string) => {
  if (!Number.isInteger(value) || value < 0) {
    throw batchStreamPublicationManifestConflict('Invalid batch stream publication manifest integer field', {
      field,
      value,
    });
  }
};

const assertPositiveInteger = (value: number, field: string) => {
  if (!Number.isInteger(value) || value <= 0) {
    throw batchStreamPublicationManifestConflict('Invalid batch stream publication manifest limit', {
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
    throw batchStreamPublicationManifestConflict('Batch stream publication manifest limit exceeds the hard cap', {
      field,
      hard_cap: hardCap,
      value,
    });
  }
  return value;
};

const assertIsoTimestamp = (value: string, field: string) => {
  if (!isNonEmptyString(value) || Number.isNaN(Date.parse(value))) {
    throw batchStreamPublicationManifestConflict('Invalid batch stream publication manifest timestamp', {
      field,
      value,
    });
  }
};

const assertManifestIdentityInput = (
  input: Pick<
    BatchStreamPublicationManifestDraft,
    'receipt_id' | 'delivery_id' | 'submission_id' | 'request_fingerprint' | 'request_contract_version'
  >,
) => {
  assertBoundedNonEmptyString(input.delivery_id, 'delivery_id', BATCH_STREAM_PUBLICATION_DELIVERY_ID_MAX_LENGTH);
  if (!isNonEmptyString(input.receipt_id) || !isNonEmptyString(input.submission_id)) {
    throw batchStreamPublicationManifestConflict('Invalid batch stream publication manifest identity', {
      receipt_id: input.receipt_id,
      submission_id: input.submission_id,
    });
  }
  assertSha256Hex(input.request_fingerprint, 'request_fingerprint');
  assertPositiveInteger(input.request_contract_version, 'request_contract_version');
};

const classifyEventIneligibility = (event: unknown): BatchStreamPublicationManifestIneligibilityReason | null => {
  if (!isRecord(event)) {
    return BatchStreamPublicationManifestIneligibilityReason.RawPublication;
  }
  if (event.type === 'merge') {
    return BatchStreamPublicationManifestIneligibilityReason.MergePublication;
  }
  if (!['create', 'delete', 'update'].includes(String(event.type))) {
    return BatchStreamPublicationManifestIneligibilityReason.UnsupportedType;
  }
  if (!['external', 'internal'].includes(String(event.scope))) {
    return BatchStreamPublicationManifestIneligibilityReason.UnsupportedScope;
  }
  if (!isRecord(event.data) || !isNonEmptyString(event.data.id)) {
    return BatchStreamPublicationManifestIneligibilityReason.MissingDataId;
  }
  return null;
};

const buildIneligibleResult = (
  reason: BatchStreamPublicationManifestIneligibilityReason,
  slotIndex: number,
  eventIndex: number | null,
): BuildBatchStreamPublicationManifestCandidateResult => ({
  eligible: false,
  reason,
  slotIndex,
  eventIndex,
});

const assertAllowedEventSnapshotFields = (event: ProofableStreamDataEvent) => {
  const allowedFields = event.type === 'update' ? UPDATE_EVENT_FIELDS : BASE_EVENT_FIELDS;
  const unexpectedFields = Object.keys(event).filter((field) => !allowedFields.has(field));
  if (unexpectedFields.length > 0) {
    throw batchStreamPublicationManifestConflict('Batch stream publication manifest event snapshot contains unsupported fields', {
      event_type: event.type,
      unexpected_fields: unexpectedFields,
    });
  }
  if (event.type !== 'update') {
    return;
  }
  if (!isRecord((event as unknown as Record<string, unknown>).context)) {
    throw batchStreamPublicationManifestConflict('Batch stream publication manifest update snapshot has invalid context');
  }
  const unexpectedContextFields = Object.keys((event as unknown as Record<string, unknown>).context as Record<string, unknown>)
    .filter((field) => !UPDATE_EVENT_CONTEXT_FIELDS.has(field));
  if (unexpectedContextFields.length > 0) {
    throw batchStreamPublicationManifestConflict('Batch stream publication manifest update snapshot contains unsupported context fields', {
      unexpected_context_fields: unexpectedContextFields,
    });
  }
};

const canonicalizeEventSnapshot = (
  event: ProofableStreamDataEvent,
): { eventSnapshot: BatchStreamPublicationEventSnapshot; eventSnapshotBytes: number } => {
  assertAllowedEventSnapshotFields(event);
  const canonicalSnapshot = canonicalizeOrThrow(event, 'Invalid batch stream publication manifest event snapshot');
  return {
    eventSnapshot: JSON.parse(canonicalSnapshot) as BatchStreamPublicationEventSnapshot,
    eventSnapshotBytes: Buffer.byteLength(canonicalSnapshot),
  };
};

const buildManifestFingerprintPayload = (
  input: BatchStreamPublicationManifestFingerprintInput,
): Record<string, unknown> => ({
  manifest_version: input.manifest_version,
  receipt_id: input.receipt_id,
  delivery_id: input.delivery_id,
  submission_id: input.submission_id,
  request_fingerprint: input.request_fingerprint,
  request_contract_version: input.request_contract_version,
  entry_count: input.entry_count,
  serialized_bytes: input.serialized_bytes,
  entries: input.entries,
});

const buildBatchStreamPublicationManifestSerializedBytes = (
  input: Omit<BatchStreamPublicationManifestFingerprintInput, 'serialized_bytes'>,
): number => {
  let serializedBytes = 0;
  for (let iteration = 0; iteration < 8; iteration += 1) {
    const nextSerializedBytes = Buffer.byteLength(canonicalizeOrThrow(
      buildManifestFingerprintPayload({
        ...input,
        serialized_bytes: serializedBytes,
      }),
      'Invalid batch stream publication manifest payload',
    ));
    if (nextSerializedBytes === serializedBytes) {
      return serializedBytes;
    }
    serializedBytes = nextSerializedBytes;
  }
  throw batchStreamPublicationManifestConflict('Batch stream publication manifest serialized byte count did not stabilize');
};

export const buildBatchStreamPublicationManifestId = (deliveryId: string): string => {
  assertBoundedNonEmptyString(deliveryId, 'delivery_id', BATCH_STREAM_PUBLICATION_DELIVERY_ID_MAX_LENGTH);
  return hashSHA256(`${BATCH_STREAM_PUBLICATION_MANIFEST_ID_SCOPE}${deliveryId}`);
};

export const buildBatchStreamPublicationManifestLockId = (deliveryId: string): string => {
  assertBoundedNonEmptyString(deliveryId, 'delivery_id', BATCH_STREAM_PUBLICATION_DELIVERY_ID_MAX_LENGTH);
  return `${BATCH_STREAM_PUBLICATION_MANIFEST_LOCK_PREFIX}${deliveryId}`;
};

export const buildBatchStreamPublicationManifestFingerprint = (
  input: BatchStreamPublicationManifestFingerprintInput,
): string => {
  return hashSHA256(canonicalizeOrThrow(
    buildManifestFingerprintPayload(input),
    'Invalid batch stream publication manifest payload',
  ));
};

const buildManifestEntry = (
  deliveryId: string,
  publicationSequence: number,
  event: ProofableStreamDataEvent,
  maxEventSnapshotBytes: number,
): BatchStreamPublicationManifestEntry => {
  const publicationKey = buildBatchStreamPublicationKey(event);
  const { eventSnapshot, eventSnapshotBytes } = canonicalizeEventSnapshot(event);
  if (eventSnapshotBytes > maxEventSnapshotBytes) {
    throw batchStreamPublicationManifestConflict('Batch stream publication manifest event snapshot byte cap exceeded', {
      publication_sequence: publicationSequence,
      event_snapshot_bytes: eventSnapshotBytes,
      max_event_snapshot_bytes: maxEventSnapshotBytes,
    });
  }
  const eventFingerprint = buildBatchStreamPublicationEventFingerprint(eventSnapshot);
  return {
    publication_sequence: publicationSequence,
    publication_key: publicationKey,
    publication_id: buildBatchStreamPublicationId(deliveryId, publicationSequence, publicationKey),
    event_fingerprint: eventFingerprint,
    event_snapshot: eventSnapshot,
    event_snapshot_bytes: eventSnapshotBytes,
  };
};

export const buildBatchStreamPublicationManifestCandidate = (
  input: BuildBatchStreamPublicationManifestCandidateInput,
  options: BuildBatchStreamPublicationManifestOptions = {},
): BuildBatchStreamPublicationManifestCandidateResult => {
  const identity = {
    receipt_id: input.receipt.internal_id,
    delivery_id: input.receipt.delivery_id,
    submission_id: input.receipt.submission_id,
    request_fingerprint: input.receipt.request_fingerprint,
    request_contract_version: input.receipt.request_contract_version,
  };
  assertManifestIdentityInput(identity);
  const maxEntries = normalizeBoundedLimit(
    options.maxEntries,
    BATCH_STREAM_PUBLICATION_MANIFEST_MAX_ENTRIES,
    'max_entries',
  );
  const maxEventSnapshotBytes = normalizeBoundedLimit(
    options.maxEventSnapshotBytes,
    BATCH_STREAM_PUBLICATION_MANIFEST_MAX_EVENT_SNAPSHOT_BYTES,
    'max_event_snapshot_bytes',
  );
  const maxSerializedBytes = normalizeBoundedLimit(
    options.maxSerializedBytes,
    BATCH_STREAM_PUBLICATION_MANIFEST_MAX_SERIALIZED_BYTES,
    'max_serialized_bytes',
  );
  const entries: BatchStreamPublicationManifestEntry[] = [];

  for (let slotIndex = 0; slotIndex < input.slots.length; slotIndex += 1) {
    const slot = input.slots[slotIndex];
    if (slot.kind === 'raw') {
      return buildIneligibleResult(
        classifyEventIneligibility(slot.event) ?? BatchStreamPublicationManifestIneligibilityReason.RawPublication,
        slotIndex,
        null,
      );
    }
    if (slot.kind !== 'keyed' || !Array.isArray(slot.events) || slot.events.length === 0) {
      throw batchStreamPublicationManifestConflict('Batch stream publication manifest candidate contains an invalid keyed bucket', {
        slot_index: slotIndex,
      });
    }
    assertBoundedNonEmptyString(slot.publicationKey, 'publication_key', 512);
    for (let eventIndex = 0; eventIndex < slot.events.length; eventIndex += 1) {
      const event = slot.events[eventIndex];
      const ineligibility = classifyEventIneligibility(event);
      if (ineligibility) {
        return buildIneligibleResult(ineligibility, slotIndex, eventIndex);
      }
      const publicationKey = buildBatchStreamPublicationKey(event);
      if (publicationKey !== slot.publicationKey) {
        throw batchStreamPublicationManifestConflict('Batch stream publication manifest keyed bucket contains multiple publication keys', {
          slot_index: slotIndex,
          event_index: eventIndex,
          publication_key: slot.publicationKey,
          event_publication_key: publicationKey,
        });
      }
    }
    const finalEvents = coalesceBufferedStreamDataEvents([...slot.events] as ProofableStreamDataEvent[]);
    for (let eventIndex = 0; eventIndex < finalEvents.length; eventIndex += 1) {
      const finalEvent = finalEvents[eventIndex];
      const ineligibility = classifyEventIneligibility(finalEvent);
      if (ineligibility) {
        return buildIneligibleResult(ineligibility, slotIndex, eventIndex);
      }
      const entry = buildManifestEntry(identity.delivery_id, entries.length, finalEvent, maxEventSnapshotBytes);
      if (entry.publication_key !== slot.publicationKey) {
        throw batchStreamPublicationManifestConflict('Batch stream publication manifest coalescer changed the keyed bucket identity', {
          slot_index: slotIndex,
          event_index: eventIndex,
          publication_key: slot.publicationKey,
          event_publication_key: entry.publication_key,
        });
      }
      entries.push(entry);
      if (entries.length > maxEntries) {
        throw batchStreamPublicationManifestConflict('Batch stream publication manifest entry cap exceeded', {
          entry_count: entries.length,
          max_entries: maxEntries,
        });
      }
    }
  }

  const manifestWithoutFingerprint = {
    manifest_id: buildBatchStreamPublicationManifestId(identity.delivery_id),
    ...identity,
    manifest_version: BATCH_STREAM_PUBLICATION_MANIFEST_VERSION,
    entry_count: entries.length,
    entries,
  };
  const serializedBytes = buildBatchStreamPublicationManifestSerializedBytes(manifestWithoutFingerprint);
  if (serializedBytes > maxSerializedBytes) {
    throw batchStreamPublicationManifestConflict('Batch stream publication manifest serialized byte cap exceeded', {
      serialized_bytes: serializedBytes,
      max_serialized_bytes: maxSerializedBytes,
    });
  }
  const manifestFingerprintInput = {
    ...manifestWithoutFingerprint,
    serialized_bytes: serializedBytes,
  };
  return {
    eligible: true,
    manifest: {
      ...manifestFingerprintInput,
      manifest_fingerprint: buildBatchStreamPublicationManifestFingerprint(manifestFingerprintInput),
    },
  };
};

const assertManifestEntry = (
  manifest: Pick<BatchStreamPublicationManifestDraft, 'delivery_id'>,
  entry: unknown,
  index: number,
) => {
  if (!isRecord(entry)) {
    throw batchStreamPublicationManifestConflict('Batch stream publication manifest entry payload is malformed', {
      publication_sequence: index,
    });
  }
  const unexpectedFields = Object.keys(entry).filter((field) => !MANIFEST_ENTRY_FIELDS.has(field));
  if (unexpectedFields.length > 0) {
    throw batchStreamPublicationManifestConflict('Batch stream publication manifest entry payload is malformed', {
      publication_sequence: index,
      unexpected_fields: unexpectedFields,
    });
  }
  assertNonNegativeInteger(entry.publication_sequence as number, 'publication_sequence');
  if (entry.publication_sequence !== index) {
    throw batchStreamPublicationManifestConflict('Batch stream publication manifest publication sequence is not contiguous', {
      publication_sequence: entry.publication_sequence,
      expected_publication_sequence: index,
    });
  }
  if (!isNonEmptyString(entry.publication_key)) {
    throw batchStreamPublicationManifestConflict('Batch stream publication manifest entry payload is malformed', {
      publication_sequence: index,
      field: 'publication_key',
    });
  }
  const eventSnapshot = entry.event_snapshot as ProofableStreamDataEvent;
  const ineligibility = classifyEventIneligibility(eventSnapshot);
  if (ineligibility) {
    throw batchStreamPublicationManifestConflict('Batch stream publication manifest persisted an ineligible event snapshot', {
      publication_sequence: index,
      ineligibility,
    });
  }
  const rebuiltEntry = buildManifestEntry(
    manifest.delivery_id,
    index,
    eventSnapshot,
    BATCH_STREAM_PUBLICATION_MANIFEST_MAX_EVENT_SNAPSHOT_BYTES,
  );
  const conflictingFields = Object.entries({
    publication_key: entry.publication_key !== rebuiltEntry.publication_key,
    publication_id: entry.publication_id !== rebuiltEntry.publication_id,
    event_fingerprint: entry.event_fingerprint !== rebuiltEntry.event_fingerprint,
    event_snapshot_bytes: entry.event_snapshot_bytes !== rebuiltEntry.event_snapshot_bytes,
    event_snapshot: canonicalizeOrThrow(entry.event_snapshot, 'Invalid batch stream publication manifest event snapshot')
      !== canonicalizeOrThrow(rebuiltEntry.event_snapshot, 'Invalid batch stream publication manifest event snapshot'),
  }).filter(([, conflict]) => conflict).map(([field]) => field);
  if (conflictingFields.length > 0) {
    throw batchStreamPublicationManifestConflict('Batch stream publication manifest entry changed after persistence', {
      publication_sequence: index,
      conflicting_fields: conflictingFields,
    });
  }
};

const assertBatchStreamPublicationManifestDraft = (
  manifest: BatchStreamPublicationManifestDraft,
) => {
  assertManifestIdentityInput(manifest);
  if (manifest.manifest_id !== buildBatchStreamPublicationManifestId(manifest.delivery_id)) {
    throw batchStreamPublicationManifestConflict('Batch stream publication manifest id does not match its delivery identity', {
      manifest_id: manifest.manifest_id,
      delivery_id: manifest.delivery_id,
    });
  }
  if (manifest.manifest_version !== BATCH_STREAM_PUBLICATION_MANIFEST_VERSION) {
    throw batchStreamPublicationManifestConflict('Batch stream publication manifest version is invalid', {
      manifest_version: manifest.manifest_version,
    });
  }
  assertSha256Hex(manifest.manifest_fingerprint, 'manifest_fingerprint');
  assertNonNegativeInteger(manifest.entry_count, 'entry_count');
  assertNonNegativeInteger(manifest.serialized_bytes, 'serialized_bytes');
  if (!Array.isArray(manifest.entries) || manifest.entry_count !== manifest.entries.length) {
    throw batchStreamPublicationManifestConflict('Batch stream publication manifest entry count does not match the durable entry list', {
      entry_count: manifest.entry_count,
      entry_list_length: Array.isArray(manifest.entries) ? manifest.entries.length : null,
    });
  }
  if (manifest.entry_count > BATCH_STREAM_PUBLICATION_MANIFEST_MAX_ENTRIES) {
    throw batchStreamPublicationManifestConflict('Batch stream publication manifest entry cap exceeded', {
      entry_count: manifest.entry_count,
      max_entries: BATCH_STREAM_PUBLICATION_MANIFEST_MAX_ENTRIES,
    });
  }
  manifest.entries.forEach((entry, index) => assertManifestEntry(manifest, entry, index));
  const expectedSerializedBytes = buildBatchStreamPublicationManifestSerializedBytes({
    manifest_id: manifest.manifest_id,
    receipt_id: manifest.receipt_id,
    delivery_id: manifest.delivery_id,
    submission_id: manifest.submission_id,
    request_fingerprint: manifest.request_fingerprint,
    request_contract_version: manifest.request_contract_version,
    manifest_version: manifest.manifest_version,
    entry_count: manifest.entry_count,
    entries: manifest.entries,
  });
  if (manifest.serialized_bytes !== expectedSerializedBytes) {
    throw batchStreamPublicationManifestConflict('Batch stream publication manifest serialized byte count changed after persistence', {
      serialized_bytes: manifest.serialized_bytes,
      expected_serialized_bytes: expectedSerializedBytes,
    });
  }
  if (manifest.serialized_bytes > BATCH_STREAM_PUBLICATION_MANIFEST_MAX_SERIALIZED_BYTES) {
    throw batchStreamPublicationManifestConflict('Batch stream publication manifest serialized byte cap exceeded', {
      serialized_bytes: manifest.serialized_bytes,
      max_serialized_bytes: BATCH_STREAM_PUBLICATION_MANIFEST_MAX_SERIALIZED_BYTES,
    });
  }
  const expectedFingerprint = buildBatchStreamPublicationManifestFingerprint({
    manifest_id: manifest.manifest_id,
    receipt_id: manifest.receipt_id,
    delivery_id: manifest.delivery_id,
    submission_id: manifest.submission_id,
    request_fingerprint: manifest.request_fingerprint,
    request_contract_version: manifest.request_contract_version,
    manifest_version: manifest.manifest_version,
    entry_count: manifest.entry_count,
    serialized_bytes: manifest.serialized_bytes,
    entries: manifest.entries,
  });
  if (manifest.manifest_fingerprint !== expectedFingerprint) {
    throw batchStreamPublicationManifestConflict('Batch stream publication manifest fingerprint changed after persistence', {
      manifest_fingerprint: manifest.manifest_fingerprint,
      expected_manifest_fingerprint: expectedFingerprint,
    });
  }
};

const assertBatchStreamPublicationManifestRecord = (
  value: unknown,
): BatchStreamPublicationManifest => {
  if (!isRecord(value)) {
    throw batchStreamPublicationManifestConflict('Batch stream publication manifest payload is malformed');
  }
  const manifest = value as unknown as BatchStreamPublicationManifest;
  const unexpectedFields = Object.keys(value).filter((field) => !MANIFEST_RECORD_FIELDS.has(field));
  if (unexpectedFields.length > 0) {
    throw batchStreamPublicationManifestConflict('Batch stream publication manifest payload is malformed', {
      unexpected_fields: unexpectedFields,
    });
  }
  if (
    !isNonEmptyString(manifest.id)
    || !isNonEmptyString(manifest.internal_id)
    || !isNonEmptyString(manifest.standard_id)
    || manifest.entity_type !== ENTITY_TYPE_BATCH_STREAM_PUBLICATION_MANIFEST
    || manifest.base_type !== BASE_TYPE_ENTITY
    || !Array.isArray(manifest.parent_types)
    || !isNonEmptyString(manifest.created_at)
    || !isNonEmptyString(manifest.updated_at)
  ) {
    throw batchStreamPublicationManifestConflict('Batch stream publication manifest payload is malformed');
  }
  if (
    manifest.id !== manifest.manifest_id
    || manifest.internal_id !== manifest.manifest_id
    || manifest.standard_id !== manifest.manifest_id
    || (manifest._id !== undefined && manifest._id !== manifest.manifest_id)
  ) {
    throw batchStreamPublicationManifestConflict('Batch stream publication manifest record identity is malformed', {
      manifest_id: manifest.manifest_id,
    });
  }
  assertIsoTimestamp(manifest.created_at, 'created_at');
  assertIsoTimestamp(manifest.updated_at, 'updated_at');
  assertBatchStreamPublicationManifestDraft(manifest);
  return manifest;
};

export const loadBatchStreamPublicationManifest = async (
  context: AuthContext,
  deliveryId: string,
): Promise<BatchStreamPublicationManifest | null> => {
  const manifestId = buildBatchStreamPublicationManifestId(deliveryId);
  const manifest = await elLoadById(context, SYSTEM_USER, manifestId, {
    type: ENTITY_TYPE_BATCH_STREAM_PUBLICATION_MANIFEST,
    indices: READ_INDEX_INTERNAL_OBJECTS,
  });
  return manifest ? assertBatchStreamPublicationManifestRecord(manifest) : null;
};

export const assertBatchStreamPublicationManifestReservation = (
  manifest: BatchStreamPublicationManifest,
  input: BatchStreamPublicationManifestDraft,
) => {
  assertBatchStreamPublicationManifestDraft(input);
  const conflictingFields = Object.entries({
    manifest_id: manifest.manifest_id !== input.manifest_id,
    receipt_id: manifest.receipt_id !== input.receipt_id,
    delivery_id: manifest.delivery_id !== input.delivery_id,
    submission_id: manifest.submission_id !== input.submission_id,
    request_fingerprint: manifest.request_fingerprint !== input.request_fingerprint,
    request_contract_version: manifest.request_contract_version !== input.request_contract_version,
    manifest_version: manifest.manifest_version !== input.manifest_version,
    manifest_fingerprint: manifest.manifest_fingerprint !== input.manifest_fingerprint,
    entry_count: manifest.entry_count !== input.entry_count,
    serialized_bytes: manifest.serialized_bytes !== input.serialized_bytes,
    entries: canonicalizeOrThrow(manifest.entries, 'Invalid batch stream publication manifest entries')
      !== canonicalizeOrThrow(input.entries, 'Invalid batch stream publication manifest entries'),
  }).filter(([, conflict]) => conflict).map(([field]) => field);
  if (conflictingFields.length > 0) {
    throw batchStreamPublicationManifestConflict('Batch stream publication manifest is already associated with different immutable data', {
      manifest_id: manifest.internal_id,
      delivery_id: manifest.delivery_id,
      conflicting_fields: conflictingFields,
    });
  }
};

export const reserveBatchStreamPublicationManifest = async (
  context: AuthContext,
  input: BatchStreamPublicationManifestDraft,
): Promise<BatchStreamPublicationManifest> => {
  assertBatchStreamPublicationManifestDraft(input);
  const lock = await lockResources([buildBatchStreamPublicationManifestLockId(input.delivery_id)]);
  try {
    const existingManifest = await loadBatchStreamPublicationManifest(context, input.delivery_id);
    if (existingManifest) {
      assertBatchStreamPublicationManifestReservation(existingManifest, input);
      return existingManifest;
    }
    const createdAt = now();
    const manifest: BatchStreamPublicationManifest = {
      id: input.manifest_id,
      internal_id: input.manifest_id,
      standard_id: input.manifest_id,
      entity_type: ENTITY_TYPE_BATCH_STREAM_PUBLICATION_MANIFEST,
      base_type: BASE_TYPE_ENTITY,
      parent_types: getParentTypes(ENTITY_TYPE_BATCH_STREAM_PUBLICATION_MANIFEST),
      ...input,
      created_at: createdAt,
      updated_at: createdAt,
    };
    await elIndex(INDEX_INTERNAL_OBJECTS, manifest, { context });
    return manifest;
  } finally {
    await lock.unlock();
  }
};
