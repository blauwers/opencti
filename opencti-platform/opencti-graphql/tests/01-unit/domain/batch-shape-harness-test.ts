import { describe, expect, it } from 'vitest';
import {
  buildBatchAdmission,
  buildBatchQueueMessage,
  prepareBundleSubmission,
} from '../../../src/modules/batch/batch-domain';
import {
  BatchExecutionMode,
  BatchExecutionReason,
  BatchWaitUntil,
} from '../../../src/modules/batch/batch-types';

type StixObject = Record<string, unknown>;

interface BundleShape {
  name: string;
  bundle: string;
  executionMode: BatchExecutionMode;
  executionReason: BatchExecutionReason;
  objectTypes: string[];
}

const stixId = (type: string, index: number) => {
  const suffix = index.toString().padStart(12, '0');
  return `${type}--00000000-0000-4000-8000-${suffix}`;
};

const buildBundle = (index: number, objects: StixObject[]) => JSON.stringify({
  type: 'bundle',
  id: stixId('bundle', index),
  objects,
});

const buildPglYoyoShape = (): BundleShape => {
  const creatorId = stixId('identity', 1);
  const objects = [
    {
      type: 'identity',
      id: creatorId,
      name: 'Peter Lowe (PGL Blocklist)',
      identity_class: 'organization',
    },
    ...Array.from({ length: 199 }, (_, index) => ({
      type: 'indicator',
      id: stixId('indicator', index + 2),
      created_by_ref: creatorId,
      pattern: `[domain-name:value = 'ad-${index}.example']`,
    })),
  ];
  return {
    name: 'PGL Yoyo identity plus indicators',
    bundle: buildBundle(1, objects),
    executionMode: BatchExecutionMode.Bulk,
    executionReason: BatchExecutionReason.GenericBulkCompatible,
    objectTypes: ['identity', 'indicator'],
  };
};

const buildRansomFeedShape = (): BundleShape => {
  const creatorId = stixId('identity', 300);
  const intrusionSetId = stixId('intrusion-set', 301);
  const indicatorId = stixId('indicator', 302);
  return {
    name: 'RansomFeed mixed run',
    bundle: buildBundle(2, [
      { type: 'identity', id: creatorId, name: 'RansomFeed' },
      { type: 'intrusion-set', id: intrusionSetId, name: 'Ransom Group' },
      {
        type: 'indicator',
        id: indicatorId,
        created_by_ref: creatorId,
        pattern: "[domain-name:value = 'ransom.example']",
      },
      {
        type: 'relationship',
        id: stixId('relationship', 303),
        relationship_type: 'indicates',
        source_ref: indicatorId,
        target_ref: intrusionSetId,
      },
    ]),
    executionMode: BatchExecutionMode.Bulk,
    executionReason: BatchExecutionReason.GenericBulkCompatible,
    objectTypes: ['identity', 'intrusion-set', 'indicator', 'relationship'],
  };
};

const buildFirstEpssShape = (): BundleShape => {
  const operationExtension = {
    'extension-definition--00000000-0000-4000-8000-000000000999': {
      opencti_operation: 'patch',
    },
  };
  return {
    name: 'FIRST EPSS update batch',
    bundle: buildBundle(3, Array.from({ length: 200 }, (_, index) => ({
      type: 'vulnerability',
      id: stixId('vulnerability', index + 400),
      name: `CVE-2026-${index.toString().padStart(4, '0')}`,
      extensions: operationExtension,
    }))),
    executionMode: BatchExecutionMode.Bulk,
    executionReason: BatchExecutionReason.GenericBulkCompatible,
    objectTypes: ['vulnerability'],
  };
};

const buildMitreShape = (): BundleShape => {
  const creatorId = stixId('identity', 700);
  const attackPatternId = stixId('attack-pattern', 701);
  const courseOfActionId = stixId('course-of-action', 702);
  return {
    name: 'MITRE dataset bundle',
    bundle: buildBundle(4, [
      { type: 'identity', id: creatorId, name: 'MITRE ATT&CK' },
      { type: 'attack-pattern', id: attackPatternId, name: 'Technique' },
      { type: 'course-of-action', id: courseOfActionId, name: 'Mitigation' },
      {
        type: 'relationship',
        id: stixId('relationship', 703),
        relationship_type: 'mitigates',
        source_ref: courseOfActionId,
        target_ref: attackPatternId,
      },
    ]),
    executionMode: BatchExecutionMode.Bulk,
    executionReason: BatchExecutionReason.GenericBulkCompatible,
    objectTypes: ['identity', 'attack-pattern', 'course-of-action', 'relationship'],
  };
};

const buildUrlHausShape = (): BundleShape => {
  const creatorId = stixId('identity', 800);
  const indicatorId = stixId('indicator', 801);
  const urlId = stixId('url', 802);
  return {
    name: 'URLHaus bounded generator batch',
    bundle: buildBundle(5, [
      { type: 'identity', id: creatorId, name: 'URLHaus' },
      {
        type: 'indicator',
        id: indicatorId,
        created_by_ref: creatorId,
        pattern: "[url:value = 'https://malware.example/payload']",
      },
      { type: 'url', id: urlId, value: 'https://malware.example/payload' },
      {
        type: 'relationship',
        id: stixId('relationship', 803),
        relationship_type: 'based-on',
        source_ref: indicatorId,
        target_ref: urlId,
      },
    ]),
    executionMode: BatchExecutionMode.Bulk,
    executionReason: BatchExecutionReason.GenericBulkCompatible,
    objectTypes: ['identity', 'indicator', 'url', 'relationship'],
  };
};

const buildCisaKevShape = (): BundleShape => {
  const creatorId = stixId('identity', 900);
  const vulnerabilityId = stixId('vulnerability', 901);
  const softwareId = stixId('software', 902);
  return {
    name: 'CISA KEV per-vulnerability bundle',
    bundle: buildBundle(6, [
      { type: 'identity', id: creatorId, name: 'CISA KEV' },
      { type: 'vulnerability', id: vulnerabilityId, name: 'CVE-2026-0001' },
      { type: 'software', id: softwareId, name: 'Affected Product' },
      {
        type: 'relationship',
        id: stixId('relationship', 903),
        relationship_type: 'has',
        source_ref: vulnerabilityId,
        target_ref: softwareId,
      },
      {
        type: 'relationship',
        id: stixId('relationship', 904),
        relationship_type: 'created-by',
        source_ref: vulnerabilityId,
        target_ref: creatorId,
      },
    ]),
    executionMode: BatchExecutionMode.Bulk,
    executionReason: BatchExecutionReason.GenericBulkCompatible,
    objectTypes: ['identity', 'vulnerability', 'software', 'relationship'],
  };
};

const buildRelationHeavyShape = (): BundleShape => {
  const creatorId = stixId('identity', 1000);
  const malwareObjects = Array.from({ length: 499 }, (_, index) => ({
    type: 'malware',
    id: stixId('malware', index + 1001),
    name: `Malware ${index}`,
  }));
  const relationshipObjects = Array.from({ length: 500 }, (_, index) => ({
    type: 'relationship',
    id: stixId('relationship', index + 1500),
    relationship_type: 'related-to',
    source_ref: creatorId,
    target_ref: malwareObjects[index % malwareObjects.length].id,
  }));
  return {
    name: 'relation-heavy bulk-compatible batch',
    bundle: buildBundle(7, [
      { type: 'identity', id: creatorId, name: 'Relation Feed' },
      ...malwareObjects,
      ...relationshipObjects,
    ]),
    executionMode: BatchExecutionMode.Bulk,
    executionReason: BatchExecutionReason.GenericBulkCompatible,
    objectTypes: ['identity', 'malware', 'relationship'],
  };
};

const representativeShapes = [
  buildPglYoyoShape(),
  buildRansomFeedShape(),
  buildFirstEpssShape(),
  buildMitreShape(),
  buildUrlHausShape(),
  buildCisaKevShape(),
  buildRelationHeavyShape(),
];

describe('batch storage representative shape harness', () => {
  it.each(representativeShapes)('keeps $name intact through admission', (shape) => {
    const prepared = prepareBundleSubmission(shape.bundle);
    const admission = buildBatchAdmission('connector--harness', 'work--harness', prepared);
    const queueMessage = buildBatchQueueMessage(admission, 'user--harness');
    const originalBundle = JSON.parse(shape.bundle);
    const queuedBundle = JSON.parse(Buffer.from(queueMessage.content, 'base64').toString('utf-8'));

    expect(prepared.objectCount).toBe(originalBundle.objects.length);
    expect(prepared.objectTypes).toEqual(shape.objectTypes);
    expect(prepared.executionMode).toBe(shape.executionMode);
    expect(prepared.executionReason).toBe(shape.executionReason);
    expect(prepared.waitUntil).toBe(BatchWaitUntil.Materialized);
    expect(queueMessage).toMatchObject({
      batch_id: originalBundle.id,
      batch_execution_mode: shape.executionMode,
      batch_execution_reason: shape.executionReason,
      batch_wait_until: BatchWaitUntil.Materialized,
      no_split: true,
      split_bundles: false,
    });
    expect(queuedBundle).toEqual(originalBundle);
  });

  it.each([1, 200, 1000])('preserves one queue message for %i object bundles', (objectCount) => {
    const bundle = buildBundle(20 + objectCount, Array.from({ length: objectCount }, (_, index) => ({
      type: 'vulnerability',
      id: stixId('vulnerability', 3000 + index),
      name: `CVE-2026-${index.toString().padStart(4, '0')}`,
    })));

    const prepared = prepareBundleSubmission(bundle);
    const queueMessage = buildBatchQueueMessage(
      buildBatchAdmission('connector--harness', 'work--harness', prepared),
      'user--harness',
    );

    expect(prepared.objectCount).toBe(objectCount);
    expect(prepared.executionMode).toBe(BatchExecutionMode.Bulk);
    expect(queueMessage.split_bundles).toBe(false);
    expect(JSON.parse(Buffer.from(queueMessage.content, 'base64').toString('utf-8')).objects)
      .toHaveLength(objectCount);
  });

  it('keeps repeat submissions on the same batch identity without child retries', () => {
    const shape = buildPglYoyoShape();
    const first = prepareBundleSubmission(shape.bundle, { waitUntil: BatchWaitUntil.Committed });
    const second = prepareBundleSubmission(shape.bundle, { waitUntil: BatchWaitUntil.Committed });

    expect(first.bundleId).toBe(second.bundleId);
    expect(first.idempotencyKey).toBe(second.idempotencyKey);
    expect(first.waitUntil).toBe(BatchWaitUntil.Committed);
    expect(buildBatchQueueMessage(
      buildBatchAdmission('connector--harness', 'work--first', first),
      'user--harness',
    )).toMatchObject({
      batch_id: first.bundleId,
      batch_idempotency_key: first.idempotencyKey,
      batch_wait_until: BatchWaitUntil.Committed,
      no_split: true,
      split_bundles: false,
    });
  });
});
