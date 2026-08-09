import jsonCanonicalize from 'canonicalize';
import * as jsonpatch from 'fast-json-patch';
import { v5 as uuidv5 } from 'uuid';
import { beforeEach, describe, expect, it, vi } from 'vitest';
import { elIndex, elLoadById } from '../../../../src/database/engine';
import { lockResources } from '../../../../src/lock/master-lock';
import {
  BATCH_STREAM_PUBLICATION_MANIFEST_MAX_ENTRIES,
  BATCH_STREAM_PUBLICATION_MANIFEST_MAX_EVENT_SNAPSHOT_BYTES,
  BATCH_STREAM_PUBLICATION_MANIFEST_MAX_SERIALIZED_BYTES,
  BatchStreamPublicationManifestIneligibilityReason,
  buildBatchStreamPublicationManifestCandidate,
  buildBatchStreamPublicationManifestId,
  buildBatchStreamPublicationManifestLockId,
  loadBatchStreamPublicationManifest,
  reserveBatchStreamPublicationManifest,
  type BatchStreamPublicationManifestCandidateSlot,
  type BatchStreamPublicationManifestDraft,
} from '../../../../src/modules/batch/batch-stream-publication-manifest-domain';
import { buildBatchStreamPublicationKey } from '../../../../src/modules/batch/batch-stream-publication-proof-domain';
import type {
  BatchExecutionReceipt,
  BatchStreamPublicationEventSnapshot,
  BatchStreamPublicationManifest,
} from '../../../../src/modules/batch/batch-types';
import { hashSHA256 } from '../../../../src/utils/hash';
import { testContext } from '../../../utils/testQuery';

vi.mock('../../../../src/database/engine', () => ({
  elIndex: vi.fn(),
  elLoadById: vi.fn(),
}));

vi.mock('../../../../src/lock/master-lock', () => ({
  lockResources: vi.fn(),
}));

const receipt = {
  internal_id: 'batch-execution-receipt--manifest-test',
  delivery_id: 'batch-delivery--manifest-test',
  submission_id: 'batch-submission--manifest-test',
  request_fingerprint: 'a'.repeat(64),
  request_contract_version: 1,
} as Pick<
  BatchExecutionReceipt,
  'internal_id' | 'delivery_id' | 'submission_id' | 'request_fingerprint' | 'request_contract_version'
>;

const buildCreateEvent = (
  id: string,
  data: Record<string, unknown> = {},
): BatchStreamPublicationEventSnapshot => ({
  data: {
    id,
    name: 'created',
    type: 'indicator',
    ...data,
  },
  event_id: `event-${id}`,
  message: 'Create indicator',
  origin: {
    applicant_id: 'connector--manifest-test',
  },
  scope: 'external',
  type: 'create',
  version: '4',
} as unknown as BatchStreamPublicationEventSnapshot);

const buildDeleteEvent = (
  id: string,
  data: Record<string, unknown> = {},
): BatchStreamPublicationEventSnapshot => ({
  data: {
    id,
    name: 'deleted',
    type: 'indicator',
    ...data,
  },
  event_id: `event-${id}-delete`,
  message: 'Delete indicator',
  origin: {
    applicant_id: 'connector--manifest-test',
  },
  scope: 'external',
  type: 'delete',
  version: '4',
} as unknown as BatchStreamPublicationEventSnapshot);

const buildUpdateEvent = (
  id: string,
  previousData: Record<string, unknown>,
  nextData: Record<string, unknown>,
): BatchStreamPublicationEventSnapshot => ({
  commit: undefined,
  context: {
    changes: [{ field: 'name' }],
    patch: jsonpatch.compare(previousData, nextData),
    reverse_patch: jsonpatch.compare(nextData, previousData),
  },
  data: nextData,
  event_id: `event-${id}-update`,
  message: 'Update 1 elements',
  origin: {
    applicant_id: 'connector--manifest-test',
  },
  scope: 'external',
  type: 'update',
  version: '4',
} as unknown as BatchStreamPublicationEventSnapshot);

const keyedSlot = (
  ...events: BatchStreamPublicationEventSnapshot[]
): BatchStreamPublicationManifestCandidateSlot => ({
  kind: 'keyed',
  publicationKey: buildBatchStreamPublicationKey(events[0]),
  events,
});

const rawSlot = (event: unknown): BatchStreamPublicationManifestCandidateSlot => ({
  kind: 'raw',
  event,
});

const buildEligibleManifest = (
  slots: readonly BatchStreamPublicationManifestCandidateSlot[],
): BatchStreamPublicationManifestDraft => {
  const result = buildBatchStreamPublicationManifestCandidate({ receipt, slots });
  expect(result.eligible).toBe(true);
  if (!result.eligible) {
    throw new Error(`Expected eligible manifest, got ${result.reason}`);
  }
  return result.manifest;
};

const buildRepresentativeCreateSlots = (objects: Array<Record<string, unknown>>) => {
  return objects.map((object) => keyedSlot(buildCreateEvent(object.id as string, object)));
};

const PHASE0_FIXED_TIMESTAMP = '2026-08-08T00:00:00.000Z';
const PHASE0_NAMESPACE = '0e0ffef0-bcdf-4c08-b21a-d26b73496989';
const PHASE0_FIXTURE_METADATA = {
  B1: {
    encodedBundleBytes: 77730,
    sha256: 'afbc60f5623f9053ad4fd3795a3ef2810960205659000188866faa18cdfb5b0b',
  },
  B2: {
    encodedBundleBytes: 204081,
    sha256: '38de037e57628c67caa956e6064f7e8ba6c593b56d3320fe7f5465f0cdba5f59',
  },
  B4: {
    encodedBundleBytes: 340034,
    sha256: '7a4183d518a4cca114890b2ec7e9cbdb4045fb9fe90346bbcc5c7941aadbdb04',
  },
} as const;

const phase0StixId = (type: string, label: string) => `${type}--${uuidv5(`${type}:${label}`, PHASE0_NAMESPACE)}`;

const phase0BaseObject = (type: string, label: string) => ({
  created: PHASE0_FIXED_TIMESTAMP,
  id: phase0StixId(type, label),
  modified: PHASE0_FIXED_TIMESTAMP,
  spec_version: '2.1',
  type,
});

const phase0Identity = (label: string, name: string) => ({
  ...phase0BaseObject('identity', label),
  identity_class: 'organization',
  name,
});

const phase0Indicator = (label: string, creatorId: string, domainName: string) => ({
  ...phase0BaseObject('indicator', label),
  created_by_ref: creatorId,
  name: domainName,
  pattern: `[domain-name:value = '${domainName}']`,
  pattern_type: 'stix',
  valid_from: PHASE0_FIXED_TIMESTAMP,
});

const phase0Vulnerability = (label: string, name: string) => ({
  ...phase0BaseObject('vulnerability', label),
  name,
});

const phase0Malware = (label: string, creatorId: string, name: string) => ({
  ...phase0BaseObject('malware', label),
  created_by_ref: creatorId,
  is_family: false,
  name,
});

const phase0Relationship = (label: string, creatorId: string, sourceRef: string, targetRef: string) => ({
  ...phase0BaseObject('relationship', label),
  created_by_ref: creatorId,
  relationship_type: 'related-to',
  source_ref: sourceRef,
  target_ref: targetRef,
});

const phase0Bundle = (label: string, objects: Array<Record<string, unknown>>) => ({
  id: phase0StixId('bundle', label),
  objects,
  type: 'bundle',
});

const buildPhase0B1Bundle = () => {
  const creator = phase0Identity('b1:creator', 'Peter Lowe (PGL Blocklist)');
  return phase0Bundle('b1:pgl-yoyo', [
    creator,
    ...Array.from({ length: 199 }, (_, index) => phase0Indicator(
      `b1:indicator:${index}`,
      creator.id,
      `ad-${index.toString().padStart(3, '0')}.phase0.example`,
    )),
  ]);
};

const buildPhase0B2Bundle = () => phase0Bundle('b2:bulk-vulnerabilities', Array.from(
  { length: 1000 },
  (_, index) => phase0Vulnerability(`b2:vulnerability:${index}`, `CVE-2026-${index.toString().padStart(5, '0')}`),
));

const buildPhase0B4Bundle = () => {
  const creator = phase0Identity('b4:creator', 'Phase 0 Relation Feed');
  const malwareObjects = Array.from(
    { length: 499 },
    (_, index) => phase0Malware(`b4:malware:${index}`, creator.id, `Phase0 Malware ${index.toString().padStart(3, '0')}`),
  );
  return phase0Bundle('b4:relation-heavy', [
    creator,
    ...malwareObjects,
    ...Array.from(
      { length: 500 },
      (_, index) => phase0Relationship(
        `b4:relationship:${index}`,
        creator.id,
        creator.id,
        malwareObjects[index % malwareObjects.length].id,
      ),
    ),
  ]);
};

const canonicalizeFixture = (value: unknown): string => {
  const canonicalValue = jsonCanonicalize(value);
  if (typeof canonicalValue !== 'string') {
    throw new Error('Expected a canonical Phase 0 fixture');
  }
  return canonicalValue;
};

describe('batch stream publication manifest domain', () => {
  let manifests: Map<string, any>;

  beforeEach(() => {
    vi.clearAllMocks();
    manifests = new Map();
    vi.mocked(lockResources).mockResolvedValue({ unlock: vi.fn() } as any);
    vi.mocked(elLoadById).mockImplementation(async (_context, _user, id) => manifests.get(id) ?? undefined);
    vi.mocked(elIndex).mockImplementation(async (_index, document) => {
      manifests.set(document.internal_id, document as BatchStreamPublicationManifest);
      return document;
    });
  });

  it('builds deterministic manifest identity, fingerprint, ordered entries, and publication identities', () => {
    const firstCreate = buildCreateEvent('indicator--2', { label: 'second' });
    const equivalentFirstCreate = {
      version: '4',
      type: 'create',
      scope: 'external',
      origin: {
        applicant_id: 'connector--manifest-test',
      },
      message: 'Create indicator',
      event_id: 'event-indicator--2',
      data: {
        type: 'indicator',
        label: 'second',
        name: 'created',
        id: 'indicator--2',
      },
    } as unknown as BatchStreamPublicationEventSnapshot;
    const secondCreate = buildCreateEvent('indicator--1', { label: 'first' });

    const first = buildEligibleManifest([
      keyedSlot(firstCreate),
      keyedSlot(secondCreate),
    ]);
    const second = buildEligibleManifest([
      keyedSlot(equivalentFirstCreate),
      keyedSlot(secondCreate),
    ]);

    expect(buildBatchStreamPublicationManifestId(receipt.delivery_id))
      .toBe(buildBatchStreamPublicationManifestId(receipt.delivery_id));
    expect(buildBatchStreamPublicationManifestLockId(receipt.delivery_id))
      .toBe(`batch-stream-publication-manifest:${receipt.delivery_id}`);
    expect(first.manifest_id).toBe(second.manifest_id);
    expect(first.manifest_fingerprint).toBe(second.manifest_fingerprint);
    expect(first.entries).toEqual(second.entries);
    expect(first.entries.map((entry) => entry.publication_key)).toEqual([
      'external:indicator--2',
      'external:indicator--1',
    ]);
    expect(first.entries.map((entry) => entry.publication_sequence)).toEqual([0, 1]);
    expect(first.entries[0].publication_id).not.toBe(first.entries[1].publication_id);
  });

  it('preserves bucket-first-seen order, coalescer output order, and zero-entry buckets', () => {
    const created = buildCreateEvent('indicator--create-delete');
    const deleted = buildDeleteEvent('indicator--create-delete');
    const previous = { id: 'indicator--updated', name: 'before', type: 'indicator' };
    const changed = { id: 'indicator--updated', name: 'after', type: 'indicator' };
    const updated = buildUpdateEvent('indicator--updated', previous, changed);

    const manifest = buildEligibleManifest([
      keyedSlot(created, deleted),
      keyedSlot(updated),
    ]);

    expect(manifest.entry_count).toBe(1);
    expect(manifest.entries).toHaveLength(1);
    expect(manifest.entries[0]).toMatchObject({
      publication_sequence: 0,
      publication_key: 'external:indicator--updated',
      event_snapshot: {
        type: 'update',
        data: changed,
      },
    });
  });

  it('keeps same-key multi-output coalescer results in order with distinct global sequences', () => {
    const deleted = buildDeleteEvent('indicator--same-key');
    const recreated = buildCreateEvent('indicator--same-key', { name: 'recreated' });

    const manifest = buildEligibleManifest([
      keyedSlot(deleted, recreated),
    ]);

    expect(manifest.entries.map((entry) => entry.event_snapshot.type)).toEqual(['delete', 'create']);
    expect(manifest.entries.map((entry) => entry.publication_sequence)).toEqual([0, 1]);
    expect(manifest.entries.map((entry) => entry.publication_key)).toEqual([
      'external:indicator--same-key',
      'external:indicator--same-key',
    ]);
    expect(new Set(manifest.entries.map((entry) => entry.publication_id)).size).toBe(2);
  });

  it.each([
    {
      reason: BatchStreamPublicationManifestIneligibilityReason.RawPublication,
      slot: rawSlot(buildCreateEvent('indicator--raw')),
    },
    {
      reason: BatchStreamPublicationManifestIneligibilityReason.MergePublication,
      slot: rawSlot({
        ...buildCreateEvent('indicator--merge'),
        type: 'merge',
      }),
    },
    {
      reason: BatchStreamPublicationManifestIneligibilityReason.MissingDataId,
      slot: rawSlot({
        ...buildCreateEvent('indicator--missing-id'),
        data: { name: 'missing id', type: 'indicator' },
      }),
    },
    {
      reason: BatchStreamPublicationManifestIneligibilityReason.UnsupportedScope,
      slot: rawSlot({
        ...buildCreateEvent('indicator--scope'),
        scope: 'partner',
      }),
    },
    {
      reason: BatchStreamPublicationManifestIneligibilityReason.UnsupportedType,
      slot: rawSlot({
        ...buildCreateEvent('indicator--type'),
        type: 'read',
      }),
    },
  ])('fails the whole candidate closed for $reason stream publications', ({ reason, slot }) => {
    const result = buildBatchStreamPublicationManifestCandidate({
      receipt,
      slots: [
        keyedSlot(buildCreateEvent('indicator--eligible')),
        slot,
      ],
    });

    expect(result).toEqual({
      eligible: false,
      reason,
      slotIndex: 1,
      eventIndex: null,
    });
  });

  it('rejects changed immutable manifest content on reservation and reads back the original record', async () => {
    const manifest = buildEligibleManifest([
      keyedSlot(buildCreateEvent('indicator--reservation')),
    ]);
    const first = await reserveBatchStreamPublicationManifest(testContext, manifest);
    const replay = await reserveBatchStreamPublicationManifest(testContext, manifest);
    const readback = await loadBatchStreamPublicationManifest(testContext, receipt.delivery_id);
    const changed = buildEligibleManifest([
      keyedSlot(buildCreateEvent('indicator--reservation', { name: 'changed' })),
    ]);

    expect(first).toMatchObject({
      internal_id: manifest.manifest_id,
      manifest_id: manifest.manifest_id,
      receipt_id: receipt.internal_id,
      delivery_id: receipt.delivery_id,
      submission_id: receipt.submission_id,
      entry_count: 1,
    });
    expect(replay).toBe(first);
    expect(readback).toStrictEqual(first);
    expect(lockResources).toHaveBeenCalledWith([buildBatchStreamPublicationManifestLockId(receipt.delivery_id)]);
    expect(elIndex).toHaveBeenCalledTimes(1);
    await expect(reserveBatchStreamPublicationManifest(testContext, changed))
      .rejects.toThrowError('Batch stream publication manifest is already associated with different immutable data');
  });

  it('accepts engine metadata fields added during durable readback', async () => {
    const manifest = await reserveBatchStreamPublicationManifest(testContext, buildEligibleManifest([
      keyedSlot(buildCreateEvent('indicator--engine-metadata')),
    ]));
    manifests.set(manifest.internal_id, {
      ...manifest,
      _id: manifest.internal_id,
      _index: 'internal_objects',
      sort: [manifest.internal_id],
    } as BatchStreamPublicationManifest);

    await expect(loadBatchStreamPublicationManifest(testContext, receipt.delivery_id))
      .resolves.toMatchObject({
        internal_id: manifest.internal_id,
        manifest_id: manifest.manifest_id,
        _id: manifest.internal_id,
      });
  });

  it('fails readback closed when durable state grows raw history or retry metadata outside the manifest contract', async () => {
    const manifest = await reserveBatchStreamPublicationManifest(testContext, buildEligibleManifest([
      keyedSlot(buildCreateEvent('indicator--malformed')),
    ]));
    manifests.set(manifest.internal_id, {
      ...manifest,
      raw_source_history: [buildCreateEvent('indicator--malformed')],
    } as unknown as BatchStreamPublicationManifest);

    await expect(loadBatchStreamPublicationManifest(testContext, receipt.delivery_id))
      .rejects.toThrowError('Batch stream publication manifest payload is malformed');
  });

  it('enforces entry, per-entry snapshot, and total manifest byte caps before durable reservation', () => {
    expect(() => buildEligibleManifest(Array.from(
      { length: BATCH_STREAM_PUBLICATION_MANIFEST_MAX_ENTRIES + 1 },
      (_, index) => keyedSlot(buildCreateEvent(`indicator--entry-cap-${index}`)),
    ))).toThrowError('Batch stream publication manifest entry cap exceeded');

    expect(() => buildEligibleManifest([
      keyedSlot(buildCreateEvent('indicator--entry-bytes', {
        description: 'x'.repeat(BATCH_STREAM_PUBLICATION_MANIFEST_MAX_EVENT_SNAPSHOT_BYTES),
      })),
    ])).toThrowError('Batch stream publication manifest event snapshot byte cap exceeded');

    expect(() => buildEligibleManifest(Array.from(
      { length: 18 },
      (_, index) => keyedSlot(buildCreateEvent(`indicator--total-bytes-${index}`, {
        description: 'x'.repeat(60000),
      })),
    ))).toThrowError('Batch stream publication manifest serialized byte cap exceeded');
    expect(elIndex).not.toHaveBeenCalled();
  });

  it('keeps Phase 0 B1, B2, and B4 representative create wrappers within the initial durable caps', () => {
    const fixturesByShape = [
      ['B1', buildPhase0B1Bundle(), 200],
      ['B2', buildPhase0B2Bundle(), 1000],
      ['B4', buildPhase0B4Bundle(), 1000],
    ] as const;

    const manifestsByShape = fixturesByShape.map(([shape, bundle, expectedEntryCount]) => {
      const serializedBundle = canonicalizeFixture(bundle);
      expect(Buffer.byteLength(serializedBundle), shape).toBe(PHASE0_FIXTURE_METADATA[shape].encodedBundleBytes);
      expect(hashSHA256(serializedBundle), shape).toBe(PHASE0_FIXTURE_METADATA[shape].sha256);
      return [shape, buildEligibleManifest(buildRepresentativeCreateSlots(bundle.objects)), expectedEntryCount] as const;
    });

    for (const [shape, manifest, expectedEntryCount] of manifestsByShape) {
      expect(manifest.entry_count, shape).toBe(expectedEntryCount);
      expect(manifest.serialized_bytes, shape).toBeLessThan(BATCH_STREAM_PUBLICATION_MANIFEST_MAX_SERIALIZED_BYTES);
      expect(Math.max(...manifest.entries.map((entry) => entry.event_snapshot_bytes)), shape)
        .toBeLessThan(BATCH_STREAM_PUBLICATION_MANIFEST_MAX_EVENT_SNAPSHOT_BYTES);
    }
    expect(manifestsByShape[1][1].serialized_bytes).toBeGreaterThan(manifestsByShape[0][1].serialized_bytes);
    expect(manifestsByShape[2][1].serialized_bytes).toBeGreaterThan(manifestsByShape[0][1].serialized_bytes);
  });
});
