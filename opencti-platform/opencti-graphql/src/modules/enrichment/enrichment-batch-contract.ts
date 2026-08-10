import jsonCanonicalize from 'canonicalize';
import { FunctionalError } from '../../config/errors';
import { ENRICHMENT_RESOLUTION_DEFERRED, ENRICHMENT_RESOLUTION_STIX_BUNDLE } from '../../schema/general';
import { hashSHA256 } from '../../utils/hash';

const ENRICHMENT_BATCH_ID_PREFIX = 'enrichment-batch--';
const ENRICHMENT_BATCH_ITEM_ID_PREFIX = 'enrichment-item--';
const ENRICHMENT_BATCH_ID_MAX_BYTES = 128;
const ENRICHMENT_BATCH_ITEM_ID_MAX_BYTES = 128;
const ENRICHMENT_BATCH_STRING_ID_MAX_BYTES = 512;
const ENRICHMENT_BATCH_MESSAGE_MAX_BYTES = 4 * 1024;

export const ENRICHMENT_BATCH_MAX_ITEMS = 100;
export const ENRICHMENT_BATCH_MAX_STIX_OBJECTS = 1000;
export const ENRICHMENT_BATCH_MAX_SERIALIZED_BYTES = 1024 * 1024;
export const ENRICHMENT_BATCH_DEFAULT_WAIT_MS = 1000;
export const ENRICHMENT_BATCH_MAX_WAIT_MS = 60 * 1000;

export enum EnrichmentBatchProtocol {
  V1 = 1,
}

export enum EnrichmentBatchMode {
  Auto = 'auto',
  Manual = 'manual',
}

export enum EnrichmentBatchTrigger {
  Create = 'create',
  Update = 'update',
}

export enum EnrichmentBatchResultStatus {
  Processed = 'PROCESSED',
  Unchanged = 'UNCHANGED',
  Failed = 'FAILED',
  Retryable = 'RETRYABLE',
}

export enum EnrichmentBatchContractErrorCode {
  InvalidCapability = 'INVALID_CAPABILITY',
  InvalidEnvelope = 'INVALID_ENVELOPE',
  IncompatibleGrouping = 'INCOMPATIBLE_GROUPING',
  LimitExceeded = 'LIMIT_EXCEEDED',
  InvalidResult = 'INVALID_RESULT',
}

export type EnrichmentBatchResolution
  = typeof ENRICHMENT_RESOLUTION_DEFERRED
    | typeof ENRICHMENT_RESOLUTION_STIX_BUNDLE
    | 'none';

export type EnrichmentBatchJsonValue
  = string
    | number
    | boolean
    | null
    | EnrichmentBatchJsonValue[]
    | { [key: string]: EnrichmentBatchJsonValue };

export interface EnrichmentBatchCapability {
  protocol_version: EnrichmentBatchProtocol.V1;
  max_items: number;
  max_stix_objects: number;
  max_serialized_bytes: number;
  max_wait_ms: number;
}

export interface EnrichmentBatchCandidate {
  connectorId: string;
  workId: string;
  entityId: string;
  entityType: string;
  applicantId: string | null;
  draftId: string | null;
  mode: EnrichmentBatchMode;
  trigger: EnrichmentBatchTrigger;
  resolution: EnrichmentBatchResolution;
  playbookContext?: EnrichmentBatchJsonValue | null;
  configuration?: EnrichmentBatchJsonValue | null;
  sharedOrganizationIds?: readonly string[];
  stixEntity: string | null;
  stixObjects: string | null;
}

export interface EnrichmentBatchGroupContext {
  connector_id: string;
  applicant_id: string | null;
  draft_id: string | null;
  mode: EnrichmentBatchMode;
  trigger: EnrichmentBatchTrigger;
  resolution: EnrichmentBatchResolution;
  playbook_context: EnrichmentBatchJsonValue | null;
  configuration: EnrichmentBatchJsonValue | null;
  shared_organization_ids: string[];
  context_fingerprint: string;
}

export interface EnrichmentBatchItem {
  item_id: string;
  work_id: string;
  entity_id: string;
  entity_type: string;
  payload_fingerprint: string;
  stix_entity: string | null;
  stix_objects: string | null;
}

export interface EnrichmentBatchEnvelope {
  protocol_version: EnrichmentBatchProtocol.V1;
  batch_id: string;
  item_count: number;
  object_count: number;
  group_context: EnrichmentBatchGroupContext;
  items: EnrichmentBatchItem[];
}

export interface EnrichmentBatchItemResultInput {
  itemId: string;
  workId: string;
  status: EnrichmentBatchResultStatus;
  message?: string | null;
  outputObjectIds?: readonly string[];
}

export interface EnrichmentBatchItemResult {
  item_id: string;
  work_id: string;
  status: EnrichmentBatchResultStatus;
  message: string | null;
  output_object_ids: string[];
}

export interface EnrichmentBatchResultEnvelope {
  protocol_version: EnrichmentBatchProtocol.V1;
  batch_id: string;
  result_count: number;
  output_object_count: number;
  output_bundle: string | null;
  results: EnrichmentBatchItemResult[];
}

const CAPABILITY_FIELDS = new Set([
  'protocol_version',
  'max_items',
  'max_stix_objects',
  'max_serialized_bytes',
  'max_wait_ms',
]);
const ENVELOPE_FIELDS = new Set([
  'protocol_version',
  'batch_id',
  'item_count',
  'object_count',
  'group_context',
  'items',
]);
const GROUP_CONTEXT_FIELDS = new Set([
  'connector_id',
  'applicant_id',
  'draft_id',
  'mode',
  'trigger',
  'resolution',
  'playbook_context',
  'configuration',
  'shared_organization_ids',
  'context_fingerprint',
]);
const ITEM_FIELDS = new Set([
  'item_id',
  'work_id',
  'entity_id',
  'entity_type',
  'payload_fingerprint',
  'stix_entity',
  'stix_objects',
]);
const RESULT_ENVELOPE_FIELDS = new Set([
  'protocol_version',
  'batch_id',
  'result_count',
  'output_object_count',
  'output_bundle',
  'results',
]);
const RESULT_FIELDS = new Set([
  'item_id',
  'work_id',
  'status',
  'message',
  'output_object_ids',
]);
const GROUPING_FIELD_NAMES = [
  'connector_id',
  'applicant_id',
  'draft_id',
  'mode',
  'trigger',
  'resolution',
  'playbook_context',
  'configuration',
  'shared_organization_ids',
] as const;
const ENRICHMENT_BATCH_RESOLUTIONS = new Set<EnrichmentBatchResolution>([
  ENRICHMENT_RESOLUTION_DEFERRED,
  ENRICHMENT_RESOLUTION_STIX_BUNDLE,
  'none',
]);
const ENRICHMENT_BATCH_MODES = new Set(Object.values(EnrichmentBatchMode));
const ENRICHMENT_BATCH_TRIGGERS = new Set(Object.values(EnrichmentBatchTrigger));
const ENRICHMENT_BATCH_RESULT_STATUSES = new Set(Object.values(EnrichmentBatchResultStatus));

export const DEFAULT_ENRICHMENT_BATCH_CAPABILITY: EnrichmentBatchCapability = Object.freeze({
  protocol_version: EnrichmentBatchProtocol.V1,
  max_items: ENRICHMENT_BATCH_MAX_ITEMS,
  max_stix_objects: ENRICHMENT_BATCH_MAX_STIX_OBJECTS,
  max_serialized_bytes: ENRICHMENT_BATCH_MAX_SERIALIZED_BYTES,
  max_wait_ms: ENRICHMENT_BATCH_DEFAULT_WAIT_MS,
});

const enrichmentBatchContractError = (
  message: string,
  code: EnrichmentBatchContractErrorCode,
  data: Record<string, unknown> = {},
) => {
  return FunctionalError(message, {
    enrichment_batch_error_code: code,
    ...data,
  });
};

const isRecord = (value: unknown): value is Record<string, unknown> => {
  return !!value && typeof value === 'object' && !Array.isArray(value);
};

const isNonEmptyString = (value: unknown): value is string => {
  return typeof value === 'string' && value.length > 0;
};

const canonicalizeOrThrow = (
  value: unknown,
  message: string,
  code: EnrichmentBatchContractErrorCode,
  data: Record<string, unknown> = {},
): string => {
  try {
    const canonicalValue = jsonCanonicalize(value);
    if (typeof canonicalValue !== 'string') {
      throw new Error('Value is not JSON serializable');
    }
    return canonicalValue;
  } catch (cause) {
    throw enrichmentBatchContractError(message, code, { ...data, cause });
  }
};

const assertExactFields = (
  value: Record<string, unknown>,
  allowedFields: Set<string>,
  field: string,
  code: EnrichmentBatchContractErrorCode,
) => {
  const extraFields = Object.keys(value).filter((key) => !allowedFields.has(key));
  if (extraFields.length > 0) {
    throw enrichmentBatchContractError('Unexpected enrichment batch contract field', code, {
      field,
      extra_fields: extraFields,
    });
  }
};

function assertBoundedNonEmptyString(
  value: unknown,
  field: string,
  maxBytes: number,
  code: EnrichmentBatchContractErrorCode,
): asserts value is string {
  if (!isNonEmptyString(value) || Buffer.byteLength(value) > maxBytes) {
    throw enrichmentBatchContractError('Invalid enrichment batch string field', code, {
      field,
      value,
      max_bytes: maxBytes,
    });
  }
}

function assertNullableBoundedString(
  value: unknown,
  field: string,
  maxBytes: number,
  code: EnrichmentBatchContractErrorCode,
): asserts value is string | null {
  if (value === null) {
    return;
  }
  assertBoundedNonEmptyString(value, field, maxBytes, code);
}

function assertIntegerBetween(
  value: unknown,
  field: string,
  min: number,
  max: number,
  code: EnrichmentBatchContractErrorCode,
): asserts value is number {
  if (!Number.isInteger(value) || (value as number) < min || (value as number) > max) {
    throw enrichmentBatchContractError('Invalid enrichment batch integer field', code, {
      field,
      value,
      min,
      max,
    });
  }
}

const normalizeJsonValue = (
  value: EnrichmentBatchJsonValue | null | undefined,
  field: string,
  code: EnrichmentBatchContractErrorCode,
): EnrichmentBatchJsonValue | null => {
  if (value === undefined || value === null) {
    return null;
  }
  const canonicalValue = canonicalizeOrThrow(value, 'Invalid enrichment batch JSON field', code, { field });
  return JSON.parse(canonicalValue) as EnrichmentBatchJsonValue;
};

const normalizeStringArray = (
  values: readonly string[] | undefined,
  field: string,
  code: EnrichmentBatchContractErrorCode,
): string[] => {
  if (values === undefined) {
    return [];
  }
  if (!Array.isArray(values)) {
    throw enrichmentBatchContractError('Invalid enrichment batch string array field', code, { field, value: values });
  }
  const normalizedValues = values.map((value) => {
    assertBoundedNonEmptyString(value, field, ENRICHMENT_BATCH_STRING_ID_MAX_BYTES, code);
    return value;
  });
  return Array.from(new Set(normalizedValues)).sort();
};

function assertProtocolVersion(value: unknown, code: EnrichmentBatchContractErrorCode): asserts value is EnrichmentBatchProtocol.V1 {
  if (value !== EnrichmentBatchProtocol.V1) {
    throw enrichmentBatchContractError('Unsupported enrichment batch protocol version', code, {
      protocol_version: value,
      supported_protocol_version: EnrichmentBatchProtocol.V1,
    });
  }
}

function assertMode(value: unknown, code: EnrichmentBatchContractErrorCode): asserts value is EnrichmentBatchMode {
  if (!ENRICHMENT_BATCH_MODES.has(value as EnrichmentBatchMode)) {
    throw enrichmentBatchContractError('Invalid enrichment batch mode', code, { mode: value });
  }
}

function assertTrigger(value: unknown, code: EnrichmentBatchContractErrorCode): asserts value is EnrichmentBatchTrigger {
  if (!ENRICHMENT_BATCH_TRIGGERS.has(value as EnrichmentBatchTrigger)) {
    throw enrichmentBatchContractError('Invalid enrichment batch trigger', code, { trigger: value });
  }
}

function assertResolution(value: unknown, code: EnrichmentBatchContractErrorCode): asserts value is EnrichmentBatchResolution {
  if (!ENRICHMENT_BATCH_RESOLUTIONS.has(value as EnrichmentBatchResolution)) {
    throw enrichmentBatchContractError('Invalid enrichment batch resolution', code, { resolution: value });
  }
}

const parseJsonObjectString = (
  value: string,
  field: string,
  code: EnrichmentBatchContractErrorCode,
): Record<string, unknown> => {
  try {
    const parsed = JSON.parse(value);
    if (!isRecord(parsed)) {
      throw new Error('Value must be a JSON object');
    }
    return parsed;
  } catch (cause) {
    throw enrichmentBatchContractError('Invalid enrichment batch JSON payload', code, { field, cause });
  }
};

const countCandidatePayloadObjects = (
  resolution: EnrichmentBatchResolution,
  stixEntity: string | null,
  stixObjects: string | null,
  code: EnrichmentBatchContractErrorCode,
): number => {
  if (resolution === ENRICHMENT_RESOLUTION_DEFERRED) {
    if (stixEntity !== null || stixObjects !== null) {
      throw enrichmentBatchContractError('Deferred enrichment batch items cannot carry STIX payloads', code);
    }
    return 0;
  }
  if (stixEntity === null) {
    throw enrichmentBatchContractError('Non-deferred enrichment batch items require stix_entity', code);
  }
  parseJsonObjectString(stixEntity, 'stix_entity', code);
  if (resolution === 'none') {
    if (stixObjects !== null) {
      throw enrichmentBatchContractError('Entity-only enrichment batch items cannot carry stix_objects', code);
    }
    return 1;
  }
  if (stixObjects === null) {
    throw enrichmentBatchContractError('Bundle enrichment batch items require stix_objects', code);
  }
  const parsedBundle = parseJsonObjectString(stixObjects, 'stix_objects', code);
  if (parsedBundle.type !== 'bundle' || !Array.isArray(parsedBundle.objects)) {
    throw enrichmentBatchContractError('Enrichment batch stix_objects must be a STIX bundle', code);
  }
  return parsedBundle.objects.length;
};

const parseOutputBundleObjectIds = (
  outputBundle: string | null,
  code: EnrichmentBatchContractErrorCode,
): string[] => {
  if (outputBundle === null) {
    return [];
  }
  const parsedBundle = parseJsonObjectString(outputBundle, 'output_bundle', code);
  if (parsedBundle.type !== 'bundle' || !Array.isArray(parsedBundle.objects)) {
    throw enrichmentBatchContractError('Enrichment batch output_bundle must be a STIX bundle', code);
  }
  const objectIds = parsedBundle.objects.map((object, objectIndex) => {
    if (!isRecord(object)) {
      throw enrichmentBatchContractError('Enrichment batch output bundle object must be a JSON object', code, { object_index: objectIndex });
    }
    assertBoundedNonEmptyString(object.id, 'output_bundle.objects.id', ENRICHMENT_BATCH_STRING_ID_MAX_BYTES, code);
    return object.id;
  });
  if (new Set(objectIds).size !== objectIds.length) {
    throw enrichmentBatchContractError('Enrichment batch output bundle object ids must be unique', code);
  }
  return objectIds.sort();
};

const buildGroupContextWithoutFingerprint = (candidate: EnrichmentBatchCandidate): Omit<EnrichmentBatchGroupContext, 'context_fingerprint'> => {
  assertBoundedNonEmptyString(candidate.connectorId, 'connector_id', ENRICHMENT_BATCH_STRING_ID_MAX_BYTES, EnrichmentBatchContractErrorCode.InvalidEnvelope);
  assertNullableBoundedString(candidate.applicantId, 'applicant_id', ENRICHMENT_BATCH_STRING_ID_MAX_BYTES, EnrichmentBatchContractErrorCode.InvalidEnvelope);
  assertNullableBoundedString(candidate.draftId, 'draft_id', ENRICHMENT_BATCH_STRING_ID_MAX_BYTES, EnrichmentBatchContractErrorCode.InvalidEnvelope);
  assertMode(candidate.mode, EnrichmentBatchContractErrorCode.InvalidEnvelope);
  assertTrigger(candidate.trigger, EnrichmentBatchContractErrorCode.InvalidEnvelope);
  assertResolution(candidate.resolution, EnrichmentBatchContractErrorCode.InvalidEnvelope);
  return {
    connector_id: candidate.connectorId,
    applicant_id: candidate.applicantId,
    draft_id: candidate.draftId,
    mode: candidate.mode,
    trigger: candidate.trigger,
    resolution: candidate.resolution,
    playbook_context: normalizeJsonValue(candidate.playbookContext, 'playbook_context', EnrichmentBatchContractErrorCode.InvalidEnvelope),
    configuration: normalizeJsonValue(candidate.configuration, 'configuration', EnrichmentBatchContractErrorCode.InvalidEnvelope),
    shared_organization_ids: normalizeStringArray(
      candidate.sharedOrganizationIds,
      'shared_organization_ids',
      EnrichmentBatchContractErrorCode.InvalidEnvelope,
    ),
  };
};

const buildGroupContextFingerprint = (groupContext: Omit<EnrichmentBatchGroupContext, 'context_fingerprint'>): string => {
  return hashSHA256(canonicalizeOrThrow(
    groupContext,
    'Invalid enrichment batch group context',
    EnrichmentBatchContractErrorCode.InvalidEnvelope,
  ));
};

const buildGroupContext = (candidate: EnrichmentBatchCandidate): EnrichmentBatchGroupContext => {
  const groupContext = buildGroupContextWithoutFingerprint(candidate);
  return {
    ...groupContext,
    context_fingerprint: buildGroupContextFingerprint(groupContext),
  };
};

const assertCompatibleGroupContext = (
  expected: EnrichmentBatchGroupContext,
  actual: EnrichmentBatchGroupContext,
) => {
  for (const field of GROUPING_FIELD_NAMES) {
    const expectedValue = canonicalizeOrThrow(
      expected[field],
      'Invalid enrichment batch group context',
      EnrichmentBatchContractErrorCode.InvalidEnvelope,
      { field },
    );
    const actualValue = canonicalizeOrThrow(
      actual[field],
      'Invalid enrichment batch group context',
      EnrichmentBatchContractErrorCode.InvalidEnvelope,
      { field },
    );
    if (expectedValue !== actualValue) {
      throw enrichmentBatchContractError('Enrichment batch items have incompatible grouping context', EnrichmentBatchContractErrorCode.IncompatibleGrouping, {
        field,
        expected: expected[field],
        actual: actual[field],
      });
    }
  }
};

const buildEnrichmentBatchItemId = (
  groupContextFingerprint: string,
  workId: string,
  entityId: string,
  entityType: string,
  payloadFingerprint: string,
): string => {
  return `${ENRICHMENT_BATCH_ITEM_ID_PREFIX}${hashSHA256(JSON.stringify([
    groupContextFingerprint,
    workId,
    entityId,
    entityType,
    payloadFingerprint,
  ]))}`;
};

const buildEnrichmentBatchId = (
  groupContextFingerprint: string,
  itemIds: readonly string[],
): string => {
  return `${ENRICHMENT_BATCH_ID_PREFIX}${hashSHA256(JSON.stringify([
    EnrichmentBatchProtocol.V1,
    groupContextFingerprint,
    itemIds,
  ]))}`;
};

const buildItemFromCandidate = (
  candidate: EnrichmentBatchCandidate,
  groupContextFingerprint: string,
): { item: EnrichmentBatchItem; objectCount: number } => {
  assertBoundedNonEmptyString(candidate.workId, 'work_id', ENRICHMENT_BATCH_STRING_ID_MAX_BYTES, EnrichmentBatchContractErrorCode.InvalidEnvelope);
  assertBoundedNonEmptyString(candidate.entityId, 'entity_id', ENRICHMENT_BATCH_STRING_ID_MAX_BYTES, EnrichmentBatchContractErrorCode.InvalidEnvelope);
  assertBoundedNonEmptyString(candidate.entityType, 'entity_type', ENRICHMENT_BATCH_STRING_ID_MAX_BYTES, EnrichmentBatchContractErrorCode.InvalidEnvelope);
  assertNullableBoundedString(candidate.stixEntity, 'stix_entity', ENRICHMENT_BATCH_MAX_SERIALIZED_BYTES, EnrichmentBatchContractErrorCode.InvalidEnvelope);
  assertNullableBoundedString(candidate.stixObjects, 'stix_objects', ENRICHMENT_BATCH_MAX_SERIALIZED_BYTES, EnrichmentBatchContractErrorCode.InvalidEnvelope);
  const objectCount = countCandidatePayloadObjects(
    candidate.resolution,
    candidate.stixEntity,
    candidate.stixObjects,
    EnrichmentBatchContractErrorCode.InvalidEnvelope,
  );
  const payloadFingerprint = hashSHA256(canonicalizeOrThrow(
    {
      entity_type: candidate.entityType,
      stix_entity: candidate.stixEntity,
      stix_objects: candidate.stixObjects,
    },
    'Invalid enrichment batch item payload',
    EnrichmentBatchContractErrorCode.InvalidEnvelope,
  ));
  return {
    item: {
      item_id: buildEnrichmentBatchItemId(
        groupContextFingerprint,
        candidate.workId,
        candidate.entityId,
        candidate.entityType,
        payloadFingerprint,
      ),
      work_id: candidate.workId,
      entity_id: candidate.entityId,
      entity_type: candidate.entityType,
      payload_fingerprint: payloadFingerprint,
      stix_entity: candidate.stixEntity,
      stix_objects: candidate.stixObjects,
    },
    objectCount,
  };
};

const assertEnvelopeWithinCapability = (
  envelope: EnrichmentBatchEnvelope | EnrichmentBatchResultEnvelope,
  capability: EnrichmentBatchCapability,
  objectCount: number,
) => {
  const entryCount = 'item_count' in envelope ? envelope.item_count : envelope.result_count;
  if (entryCount > capability.max_items) {
    throw enrichmentBatchContractError('Enrichment batch item count exceeds connector capability', EnrichmentBatchContractErrorCode.LimitExceeded, {
      item_count: entryCount,
      max_items: capability.max_items,
    });
  }
  if (objectCount > capability.max_stix_objects) {
    throw enrichmentBatchContractError('Enrichment batch object count exceeds connector capability', EnrichmentBatchContractErrorCode.LimitExceeded, {
      object_count: objectCount,
      max_stix_objects: capability.max_stix_objects,
    });
  }
  const serializedBytes = Buffer.byteLength(canonicalizeOrThrow(
    envelope,
    'Invalid enrichment batch envelope',
    EnrichmentBatchContractErrorCode.InvalidEnvelope,
  ));
  if (serializedBytes > capability.max_serialized_bytes) {
    throw enrichmentBatchContractError('Enrichment batch payload exceeds connector capability', EnrichmentBatchContractErrorCode.LimitExceeded, {
      serialized_bytes: serializedBytes,
      max_serialized_bytes: capability.max_serialized_bytes,
    });
  }
};

const assertCapability = (capability: EnrichmentBatchCapability): EnrichmentBatchCapability => {
  const normalizedCapability = normalizeEnrichmentBatchCapability(capability);
  if (normalizedCapability === null) {
    throw enrichmentBatchContractError('Missing enrichment batch capability', EnrichmentBatchContractErrorCode.InvalidCapability);
  }
  return normalizedCapability;
};

const assertEnvelopeItems = (
  items: EnrichmentBatchItem[],
  groupContext: EnrichmentBatchGroupContext,
  code: EnrichmentBatchContractErrorCode,
): number => {
  if (!Array.isArray(items) || items.length === 0) {
    throw enrichmentBatchContractError('Enrichment batch envelope must contain items', code);
  }
  let objectCount = 0;
  const itemIds = new Set<string>();
  const workIds = new Set<string>();
  let previousItemId: string | null = null;
  items.forEach((item, itemIndex) => {
    if (!isRecord(item)) {
      throw enrichmentBatchContractError('Invalid enrichment batch item', code, { item_index: itemIndex });
    }
    assertExactFields(item, ITEM_FIELDS, `items[${itemIndex}]`, code);
    assertBoundedNonEmptyString(item.item_id, 'item_id', ENRICHMENT_BATCH_ITEM_ID_MAX_BYTES, code);
    assertBoundedNonEmptyString(item.work_id, 'work_id', ENRICHMENT_BATCH_STRING_ID_MAX_BYTES, code);
    assertBoundedNonEmptyString(item.entity_id, 'entity_id', ENRICHMENT_BATCH_STRING_ID_MAX_BYTES, code);
    assertBoundedNonEmptyString(item.entity_type, 'entity_type', ENRICHMENT_BATCH_STRING_ID_MAX_BYTES, code);
    assertBoundedNonEmptyString(item.payload_fingerprint, 'payload_fingerprint', 64, code);
    assertNullableBoundedString(item.stix_entity, 'stix_entity', ENRICHMENT_BATCH_MAX_SERIALIZED_BYTES, code);
    assertNullableBoundedString(item.stix_objects, 'stix_objects', ENRICHMENT_BATCH_MAX_SERIALIZED_BYTES, code);
    const expectedPayloadFingerprint = hashSHA256(canonicalizeOrThrow(
      {
        entity_type: item.entity_type,
        stix_entity: item.stix_entity,
        stix_objects: item.stix_objects,
      },
      'Invalid enrichment batch item payload',
      code,
      { item_index: itemIndex },
    ));
    if (item.payload_fingerprint !== expectedPayloadFingerprint) {
      throw enrichmentBatchContractError('Enrichment batch item payload fingerprint does not match payload', code, {
        item_index: itemIndex,
        payload_fingerprint: item.payload_fingerprint,
        expected_payload_fingerprint: expectedPayloadFingerprint,
      });
    }
    if (item.item_id !== buildEnrichmentBatchItemId(
      groupContext.context_fingerprint,
      item.work_id,
      item.entity_id,
      item.entity_type,
      item.payload_fingerprint,
    )) {
      throw enrichmentBatchContractError('Enrichment batch item id does not match its immutable identity', code, {
        item_index: itemIndex,
        item_id: item.item_id,
      });
    }
    if (itemIds.has(item.item_id) || workIds.has(item.work_id)) {
      throw enrichmentBatchContractError('Enrichment batch items must have unique identities', code, {
        item_index: itemIndex,
        item_id: item.item_id,
        work_id: item.work_id,
      });
    }
    if (previousItemId !== null && previousItemId.localeCompare(item.item_id) >= 0) {
      throw enrichmentBatchContractError('Enrichment batch items must be ordered by item id', code, {
        item_index: itemIndex,
        previous_item_id: previousItemId,
        item_id: item.item_id,
      });
    }
    itemIds.add(item.item_id);
    workIds.add(item.work_id);
    previousItemId = item.item_id;
    objectCount += countCandidatePayloadObjects(groupContext.resolution, item.stix_entity, item.stix_objects, code);
  });
  return objectCount;
};

const assertGroupContext = (
  value: unknown,
  code: EnrichmentBatchContractErrorCode,
): EnrichmentBatchGroupContext => {
  if (!isRecord(value)) {
    throw enrichmentBatchContractError('Invalid enrichment batch group context', code);
  }
  assertExactFields(value, GROUP_CONTEXT_FIELDS, 'group_context', code);
  assertBoundedNonEmptyString(value.connector_id, 'connector_id', ENRICHMENT_BATCH_STRING_ID_MAX_BYTES, code);
  assertNullableBoundedString(value.applicant_id, 'applicant_id', ENRICHMENT_BATCH_STRING_ID_MAX_BYTES, code);
  assertNullableBoundedString(value.draft_id, 'draft_id', ENRICHMENT_BATCH_STRING_ID_MAX_BYTES, code);
  assertMode(value.mode, code);
  assertTrigger(value.trigger, code);
  assertResolution(value.resolution, code);
  const playbookContext = normalizeJsonValue(value.playbook_context as EnrichmentBatchJsonValue | null, 'playbook_context', code);
  const configuration = normalizeJsonValue(value.configuration as EnrichmentBatchJsonValue | null, 'configuration', code);
  const sharedOrganizationIds = normalizeStringArray(value.shared_organization_ids as string[], 'shared_organization_ids', code);
  assertBoundedNonEmptyString(value.context_fingerprint, 'context_fingerprint', 64, code);
  const groupContextWithoutFingerprint = {
    connector_id: value.connector_id,
    applicant_id: value.applicant_id,
    draft_id: value.draft_id,
    mode: value.mode,
    trigger: value.trigger,
    resolution: value.resolution,
    playbook_context: playbookContext,
    configuration,
    shared_organization_ids: sharedOrganizationIds,
  };
  const expectedFingerprint = buildGroupContextFingerprint(groupContextWithoutFingerprint);
  if (value.context_fingerprint !== expectedFingerprint) {
    throw enrichmentBatchContractError('Enrichment batch group context fingerprint does not match payload', code, {
      context_fingerprint: value.context_fingerprint,
      expected_context_fingerprint: expectedFingerprint,
    });
  }
  return {
    ...groupContextWithoutFingerprint,
    context_fingerprint: value.context_fingerprint,
  };
};

export const normalizeEnrichmentBatchCapability = (value: unknown): EnrichmentBatchCapability | null => {
  if (value === undefined || value === null) {
    return null;
  }
  if (!isRecord(value)) {
    throw enrichmentBatchContractError('Invalid enrichment batch capability', EnrichmentBatchContractErrorCode.InvalidCapability);
  }
  assertExactFields(value, CAPABILITY_FIELDS, 'capability', EnrichmentBatchContractErrorCode.InvalidCapability);
  assertProtocolVersion(value.protocol_version, EnrichmentBatchContractErrorCode.InvalidCapability);
  assertIntegerBetween(value.max_items, 'max_items', 1, ENRICHMENT_BATCH_MAX_ITEMS, EnrichmentBatchContractErrorCode.InvalidCapability);
  assertIntegerBetween(
    value.max_stix_objects,
    'max_stix_objects',
    1,
    ENRICHMENT_BATCH_MAX_STIX_OBJECTS,
    EnrichmentBatchContractErrorCode.InvalidCapability,
  );
  assertIntegerBetween(
    value.max_serialized_bytes,
    'max_serialized_bytes',
    1,
    ENRICHMENT_BATCH_MAX_SERIALIZED_BYTES,
    EnrichmentBatchContractErrorCode.InvalidCapability,
  );
  assertIntegerBetween(value.max_wait_ms, 'max_wait_ms', 0, ENRICHMENT_BATCH_MAX_WAIT_MS, EnrichmentBatchContractErrorCode.InvalidCapability);
  return {
    protocol_version: value.protocol_version,
    max_items: value.max_items,
    max_stix_objects: value.max_stix_objects,
    max_serialized_bytes: value.max_serialized_bytes,
    max_wait_ms: value.max_wait_ms,
  };
};

export const buildEnrichmentBatchEnvelope = (
  candidates: readonly EnrichmentBatchCandidate[],
  capability: EnrichmentBatchCapability,
): EnrichmentBatchEnvelope => {
  const normalizedCapability = assertCapability(capability);
  if (!Array.isArray(candidates) || candidates.length === 0) {
    throw enrichmentBatchContractError('Enrichment batch envelope must contain candidates', EnrichmentBatchContractErrorCode.InvalidEnvelope);
  }
  const groupContext = buildGroupContext(candidates[0]);
  const itemResults = candidates.map((candidate) => {
    const candidateGroupContext = buildGroupContext(candidate);
    assertCompatibleGroupContext(groupContext, candidateGroupContext);
    return buildItemFromCandidate(candidate, groupContext.context_fingerprint);
  });
  const items = itemResults.map(({ item }) => item).sort((left, right) => left.item_id.localeCompare(right.item_id));
  const objectCount = itemResults.reduce((total, { objectCount: itemObjectCount }) => total + itemObjectCount, 0);
  const itemIds = items.map((item) => item.item_id);
  const envelope = {
    protocol_version: EnrichmentBatchProtocol.V1,
    batch_id: buildEnrichmentBatchId(groupContext.context_fingerprint, itemIds),
    item_count: items.length,
    object_count: objectCount,
    group_context: groupContext,
    items,
  };
  assertEnvelopeItems(items, groupContext, EnrichmentBatchContractErrorCode.InvalidEnvelope);
  assertEnvelopeWithinCapability(envelope, normalizedCapability, objectCount);
  return envelope;
};

export const serializeEnrichmentBatchEnvelope = (envelope: EnrichmentBatchEnvelope): string => {
  return canonicalizeOrThrow(envelope, 'Invalid enrichment batch envelope', EnrichmentBatchContractErrorCode.InvalidEnvelope);
};

export const parseEnrichmentBatchEnvelope = (
  serializedEnvelope: string,
  capability: EnrichmentBatchCapability,
): EnrichmentBatchEnvelope => {
  const normalizedCapability = assertCapability(capability);
  let parsedEnvelope: unknown;
  try {
    parsedEnvelope = JSON.parse(serializedEnvelope);
  } catch (cause) {
    throw enrichmentBatchContractError('Invalid enrichment batch envelope JSON', EnrichmentBatchContractErrorCode.InvalidEnvelope, { cause });
  }
  if (!isRecord(parsedEnvelope)) {
    throw enrichmentBatchContractError('Invalid enrichment batch envelope', EnrichmentBatchContractErrorCode.InvalidEnvelope);
  }
  assertExactFields(parsedEnvelope, ENVELOPE_FIELDS, 'envelope', EnrichmentBatchContractErrorCode.InvalidEnvelope);
  assertProtocolVersion(parsedEnvelope.protocol_version, EnrichmentBatchContractErrorCode.InvalidEnvelope);
  assertBoundedNonEmptyString(parsedEnvelope.batch_id, 'batch_id', ENRICHMENT_BATCH_ID_MAX_BYTES, EnrichmentBatchContractErrorCode.InvalidEnvelope);
  assertIntegerBetween(parsedEnvelope.item_count, 'item_count', 1, ENRICHMENT_BATCH_MAX_ITEMS, EnrichmentBatchContractErrorCode.InvalidEnvelope);
  assertIntegerBetween(parsedEnvelope.object_count, 'object_count', 0, ENRICHMENT_BATCH_MAX_STIX_OBJECTS, EnrichmentBatchContractErrorCode.InvalidEnvelope);
  const groupContext = assertGroupContext(parsedEnvelope.group_context, EnrichmentBatchContractErrorCode.InvalidEnvelope);
  if (!Array.isArray(parsedEnvelope.items)) {
    throw enrichmentBatchContractError('Invalid enrichment batch items', EnrichmentBatchContractErrorCode.InvalidEnvelope);
  }
  const items = parsedEnvelope.items as EnrichmentBatchItem[];
  const objectCount = assertEnvelopeItems(items, groupContext, EnrichmentBatchContractErrorCode.InvalidEnvelope);
  if (parsedEnvelope.item_count !== items.length || parsedEnvelope.object_count !== objectCount) {
    throw enrichmentBatchContractError('Enrichment batch envelope counts do not match payload', EnrichmentBatchContractErrorCode.InvalidEnvelope, {
      item_count: parsedEnvelope.item_count,
      expected_item_count: items.length,
      object_count: parsedEnvelope.object_count,
      expected_object_count: objectCount,
    });
  }
  const expectedBatchId = buildEnrichmentBatchId(groupContext.context_fingerprint, items.map((item) => item.item_id));
  if (parsedEnvelope.batch_id !== expectedBatchId) {
    throw enrichmentBatchContractError('Enrichment batch id does not match payload', EnrichmentBatchContractErrorCode.InvalidEnvelope, {
      batch_id: parsedEnvelope.batch_id,
      expected_batch_id: expectedBatchId,
    });
  }
  const envelope = {
    protocol_version: parsedEnvelope.protocol_version,
    batch_id: parsedEnvelope.batch_id,
    item_count: parsedEnvelope.item_count,
    object_count: parsedEnvelope.object_count,
    group_context: groupContext,
    items,
  };
  assertEnvelopeWithinCapability(envelope, normalizedCapability, objectCount);
  return envelope;
};

const normalizeResultItem = (
  result: EnrichmentBatchItemResultInput,
  envelopeItem: EnrichmentBatchItem,
): EnrichmentBatchItemResult => {
  assertBoundedNonEmptyString(result.itemId, 'item_id', ENRICHMENT_BATCH_ITEM_ID_MAX_BYTES, EnrichmentBatchContractErrorCode.InvalidResult);
  assertBoundedNonEmptyString(result.workId, 'work_id', ENRICHMENT_BATCH_STRING_ID_MAX_BYTES, EnrichmentBatchContractErrorCode.InvalidResult);
  if (result.itemId !== envelopeItem.item_id || result.workId !== envelopeItem.work_id) {
    throw enrichmentBatchContractError('Enrichment batch result does not match item identity', EnrichmentBatchContractErrorCode.InvalidResult, {
      item_id: result.itemId,
      work_id: result.workId,
      expected_item_id: envelopeItem.item_id,
      expected_work_id: envelopeItem.work_id,
    });
  }
  if (!ENRICHMENT_BATCH_RESULT_STATUSES.has(result.status)) {
    throw enrichmentBatchContractError('Invalid enrichment batch result status', EnrichmentBatchContractErrorCode.InvalidResult, {
      status: result.status,
    });
  }
  const message = result.message ?? null;
  assertNullableBoundedString(message, 'message', ENRICHMENT_BATCH_MESSAGE_MAX_BYTES, EnrichmentBatchContractErrorCode.InvalidResult);
  const outputObjectIds = normalizeStringArray(result.outputObjectIds, 'output_object_ids', EnrichmentBatchContractErrorCode.InvalidResult);
  if (result.status === EnrichmentBatchResultStatus.Processed) {
    if (outputObjectIds.length === 0) {
      throw enrichmentBatchContractError('Processed enrichment batch results require output object ids', EnrichmentBatchContractErrorCode.InvalidResult, {
        item_id: result.itemId,
      });
    }
  } else if (outputObjectIds.length > 0) {
    throw enrichmentBatchContractError('Only processed enrichment batch results may own output objects', EnrichmentBatchContractErrorCode.InvalidResult, {
      item_id: result.itemId,
      status: result.status,
    });
  }
  if (
    (result.status === EnrichmentBatchResultStatus.Failed || result.status === EnrichmentBatchResultStatus.Retryable)
    && message === null
  ) {
    throw enrichmentBatchContractError('Failed or retryable enrichment batch results require a message', EnrichmentBatchContractErrorCode.InvalidResult, {
      item_id: result.itemId,
      status: result.status,
    });
  }
  return {
    item_id: result.itemId,
    work_id: result.workId,
    status: result.status,
    message,
    output_object_ids: outputObjectIds,
  };
};

const assertResultOwnership = (
  results: EnrichmentBatchItemResult[],
  outputObjectIds: string[],
) => {
  const outputObjectIdSet = new Set(outputObjectIds);
  const ownedObjectIds = new Set<string>();
  results.forEach((result) => {
    result.output_object_ids.forEach((objectId) => {
      if (!outputObjectIdSet.has(objectId)) {
        throw enrichmentBatchContractError('Enrichment batch result owns an object missing from the output bundle', EnrichmentBatchContractErrorCode.InvalidResult, {
          item_id: result.item_id,
          object_id: objectId,
        });
      }
      ownedObjectIds.add(objectId);
    });
  });
  if (ownedObjectIds.size !== outputObjectIds.length) {
    throw enrichmentBatchContractError('Enrichment batch output bundle contains unowned objects', EnrichmentBatchContractErrorCode.InvalidResult, {
      output_object_ids: outputObjectIds,
      owned_object_ids: Array.from(ownedObjectIds).sort(),
    });
  }
};

export const buildEnrichmentBatchResultEnvelope = (
  envelope: EnrichmentBatchEnvelope,
  resultInputs: readonly EnrichmentBatchItemResultInput[],
  outputBundle: string | null,
  capability: EnrichmentBatchCapability,
): EnrichmentBatchResultEnvelope => {
  const normalizedCapability = assertCapability(capability);
  if (!Array.isArray(resultInputs) || resultInputs.length !== envelope.items.length) {
    throw enrichmentBatchContractError('Enrichment batch results must cover every item exactly once', EnrichmentBatchContractErrorCode.InvalidResult, {
      result_count: Array.isArray(resultInputs) ? resultInputs.length : null,
      expected_result_count: envelope.items.length,
    });
  }
  const envelopeItemsById = new Map(envelope.items.map((item) => [item.item_id, item]));
  const resultIds = new Set<string>();
  const results = resultInputs.map((result) => {
    const envelopeItem = envelopeItemsById.get(result.itemId);
    if (!envelopeItem || resultIds.has(result.itemId)) {
      throw enrichmentBatchContractError('Enrichment batch results must cover every item exactly once', EnrichmentBatchContractErrorCode.InvalidResult, {
        item_id: result.itemId,
      });
    }
    resultIds.add(result.itemId);
    return normalizeResultItem(result, envelopeItem);
  }).sort((left, right) => left.item_id.localeCompare(right.item_id));
  if (resultIds.size !== envelope.items.length) {
    throw enrichmentBatchContractError('Enrichment batch results must cover every item exactly once', EnrichmentBatchContractErrorCode.InvalidResult);
  }
  const outputObjectIds = parseOutputBundleObjectIds(outputBundle, EnrichmentBatchContractErrorCode.InvalidResult);
  const hasProcessedResults = results.some((result) => result.status === EnrichmentBatchResultStatus.Processed);
  if (hasProcessedResults && outputBundle === null) {
    throw enrichmentBatchContractError('Processed enrichment batch results require an output bundle', EnrichmentBatchContractErrorCode.InvalidResult);
  }
  if (!hasProcessedResults && outputBundle !== null) {
    throw enrichmentBatchContractError('Enrichment batch output bundle requires at least one processed result', EnrichmentBatchContractErrorCode.InvalidResult);
  }
  assertResultOwnership(results, outputObjectIds);
  const resultEnvelope = {
    protocol_version: EnrichmentBatchProtocol.V1,
    batch_id: envelope.batch_id,
    result_count: results.length,
    output_object_count: outputObjectIds.length,
    output_bundle: outputBundle,
    results,
  };
  assertEnvelopeWithinCapability(resultEnvelope, normalizedCapability, outputObjectIds.length);
  return resultEnvelope;
};

export const serializeEnrichmentBatchResultEnvelope = (resultEnvelope: EnrichmentBatchResultEnvelope): string => {
  return canonicalizeOrThrow(resultEnvelope, 'Invalid enrichment batch result envelope', EnrichmentBatchContractErrorCode.InvalidResult);
};

export const parseEnrichmentBatchResultEnvelope = (
  serializedResultEnvelope: string,
  envelope: EnrichmentBatchEnvelope,
  capability: EnrichmentBatchCapability,
): EnrichmentBatchResultEnvelope => {
  let parsedResultEnvelope: unknown;
  try {
    parsedResultEnvelope = JSON.parse(serializedResultEnvelope);
  } catch (cause) {
    throw enrichmentBatchContractError('Invalid enrichment batch result envelope JSON', EnrichmentBatchContractErrorCode.InvalidResult, { cause });
  }
  if (!isRecord(parsedResultEnvelope)) {
    throw enrichmentBatchContractError('Invalid enrichment batch result envelope', EnrichmentBatchContractErrorCode.InvalidResult);
  }
  assertExactFields(parsedResultEnvelope, RESULT_ENVELOPE_FIELDS, 'result_envelope', EnrichmentBatchContractErrorCode.InvalidResult);
  assertProtocolVersion(parsedResultEnvelope.protocol_version, EnrichmentBatchContractErrorCode.InvalidResult);
  if (parsedResultEnvelope.batch_id !== envelope.batch_id) {
    throw enrichmentBatchContractError('Enrichment batch result envelope does not match batch identity', EnrichmentBatchContractErrorCode.InvalidResult, {
      batch_id: parsedResultEnvelope.batch_id,
      expected_batch_id: envelope.batch_id,
    });
  }
  assertIntegerBetween(parsedResultEnvelope.result_count, 'result_count', 1, ENRICHMENT_BATCH_MAX_ITEMS, EnrichmentBatchContractErrorCode.InvalidResult);
  assertIntegerBetween(
    parsedResultEnvelope.output_object_count,
    'output_object_count',
    0,
    ENRICHMENT_BATCH_MAX_STIX_OBJECTS,
    EnrichmentBatchContractErrorCode.InvalidResult,
  );
  assertNullableBoundedString(
    parsedResultEnvelope.output_bundle,
    'output_bundle',
    ENRICHMENT_BATCH_MAX_SERIALIZED_BYTES,
    EnrichmentBatchContractErrorCode.InvalidResult,
  );
  if (!Array.isArray(parsedResultEnvelope.results)) {
    throw enrichmentBatchContractError('Invalid enrichment batch result items', EnrichmentBatchContractErrorCode.InvalidResult);
  }
  const resultInputs = parsedResultEnvelope.results.map((result, resultIndex) => {
    if (!isRecord(result)) {
      throw enrichmentBatchContractError('Invalid enrichment batch result item', EnrichmentBatchContractErrorCode.InvalidResult, { result_index: resultIndex });
    }
    assertExactFields(result, RESULT_FIELDS, `results[${resultIndex}]`, EnrichmentBatchContractErrorCode.InvalidResult);
    if (!Array.isArray(result.output_object_ids)) {
      throw enrichmentBatchContractError('Invalid enrichment batch output object ids', EnrichmentBatchContractErrorCode.InvalidResult, { result_index: resultIndex });
    }
    return {
      itemId: result.item_id as string,
      workId: result.work_id as string,
      status: result.status as EnrichmentBatchResultStatus,
      message: result.message as string | null,
      outputObjectIds: result.output_object_ids as string[],
    };
  });
  const resultEnvelope = buildEnrichmentBatchResultEnvelope(
    envelope,
    resultInputs,
    parsedResultEnvelope.output_bundle as string | null,
    capability,
  );
  if (
    parsedResultEnvelope.result_count !== resultEnvelope.result_count
    || parsedResultEnvelope.output_object_count !== resultEnvelope.output_object_count
  ) {
    throw enrichmentBatchContractError('Enrichment batch result counts do not match payload', EnrichmentBatchContractErrorCode.InvalidResult, {
      result_count: parsedResultEnvelope.result_count,
      expected_result_count: resultEnvelope.result_count,
      output_object_count: parsedResultEnvelope.output_object_count,
      expected_output_object_count: resultEnvelope.output_object_count,
    });
  }
  return resultEnvelope;
};
