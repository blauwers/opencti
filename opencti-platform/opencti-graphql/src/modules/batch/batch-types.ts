import type { BatchBundlePlan } from './batch-bundle-planner';

export const ENTITY_TYPE_BATCH_SUBMISSION = 'BatchSubmission';

export enum BatchAdmissionStatus {
  Accepted = 'ACCEPTED',
}

export enum BatchExecutionMode {
  Atomic = 'ATOMIC',
  Bulk = 'BULK',
  Compatibility = 'COMPATIBILITY',
  LegacySplit = 'LEGACY_SPLIT',
}

export enum BatchExecutionPreference {
  Auto = 'AUTO',
  Atomic = 'ATOMIC',
  Bulk = 'BULK',
  Compatibility = 'COMPATIBILITY',
  LegacySplit = 'LEGACY_SPLIT',
}

export enum BatchExecutionReason {
  ExplicitCompatibility = 'EXPLICIT_COMPATIBILITY',
  ExplicitLegacySplit = 'EXPLICIT_LEGACY_SPLIT',
  IdentityIndicatorAtomicCohort = 'IDENTITY_INDICATOR_ATOMIC_COHORT',
  GenericBulkCompatible = 'GENERIC_BULK_COMPATIBLE',
  OperationalBundleCompatibility = 'OPERATIONAL_BUNDLE_COMPATIBILITY',
}

export enum BatchWaitUntil {
  Committed = 'COMMITTED',
  Materialized = 'MATERIALIZED',
}

export enum BatchSubmissionState {
  Reserved = 'RESERVED',
  WorkBound = 'WORK_BOUND',
  ExpectationRecorded = 'EXPECTATION_RECORDED',
  Published = 'PUBLISHED',
}

export enum BatchSubmissionWorkOrigin {
  Generated = 'GENERATED',
  CallerProvided = 'CALLER_PROVIDED',
}

export enum BatchAdmissionErrorCode {
  InvalidBundle = 'INVALID_BUNDLE',
  InvalidBundleId = 'INVALID_BUNDLE_ID',
  InvalidConnectorId = 'INVALID_CONNECTOR_ID',
  InvalidIdempotencyKey = 'INVALID_IDEMPOTENCY_KEY',
  IdempotencyKeyConflict = 'IDEMPOTENCY_KEY_CONFLICT',
  InvalidWaitUntil = 'INVALID_WAIT_UNTIL',
  UnsupportedExecutionPreference = 'UNSUPPORTED_EXECUTION_PREFERENCE',
  ExecutionPreferenceNotEligible = 'EXECUTION_PREFERENCE_NOT_ELIGIBLE',
}

export interface BatchSubmitOptions {
  waitUntil?: BatchWaitUntil | string | null;
  executionPreference?: BatchExecutionPreference | string | null;
  idempotencyKey?: string | null;
  splitBundles?: boolean | null;
  cleanupInconsistentBundle?: boolean | null;
}

export interface PreparedBundleSubmission {
  bundle: string;
  bundleId: string;
  bundlePlan: BatchBundlePlan;
  objects: Record<string, any>[];
  objectCount: number;
  objectTypes: string[];
  executionPreference: BatchExecutionPreference;
  executionMode: BatchExecutionMode;
  executionReason: BatchExecutionReason;
  eligibleExecutionModes: BatchExecutionMode[];
  waitUntil: BatchWaitUntil;
  idempotencyKey: string;
  payloadFingerprint: string;
  cleanupInconsistentBundle: boolean;
}

export interface BatchAdmission {
  batchId: string;
  bundleId: string;
  bundlePlan: BatchBundlePlan;
  connectorId: string;
  workId: string;
  objectCount: number;
  objectTypes: string[];
  executionPreference: BatchExecutionPreference;
  executionMode: BatchExecutionMode;
  executionReason: BatchExecutionReason;
  eligibleExecutionModes: BatchExecutionMode[];
  waitUntil: BatchWaitUntil;
  status: BatchAdmissionStatus;
  idempotencyKey: string;
  cleanupInconsistentBundle: boolean;
  bundle: string;
  submissionId?: string;
}

export interface BatchQueueMessage {
  type: 'bundle';
  applicant_id: string;
  content: string;
  work_id: string;
  update: true;
  no_split: boolean;
  split_bundles: boolean;
  cleanup_inconsistent_bundle: boolean;
  batch_id: string;
  batch_execution_mode: BatchExecutionMode;
  batch_execution_reason: BatchExecutionReason;
  batch_eligible_execution_modes: BatchExecutionMode[];
  batch_wait_until: BatchWaitUntil;
  batch_idempotency_key: string;
  submission_id?: string;
  batch_plan: {
    execution_phases: Array<{
      object_ids: string[];
      phase: number;
    }>;
    ignored_object_count: number;
    incompatible_object_ids: string[];
    object_normalizations: Array<{
      external_reference_indexes?: number[];
      id: string;
      kill_chain_phase_indexes?: number[];
      reference_values?: Record<string, string | string[] | null>;
    }>;
    object_count: number;
    ordered_object_ids: string[];
    planned_object_count: number;
    version: 1;
  };
}

export interface BatchSubmission {
  id: string;
  internal_id: string;
  _index?: string;
  standard_id: string;
  entity_type: typeof ENTITY_TYPE_BATCH_SUBMISSION;
  base_type: 'ENTITY';
  parent_types: string[];
  connector_id: string;
  idempotency_key: string;
  payload_fingerprint: string;
  bundle_id: string;
  work_id: string;
  work_origin: BatchSubmissionWorkOrigin;
  work_timestamp: string | null;
  execution_preference: BatchExecutionPreference;
  execution_mode: BatchExecutionMode;
  execution_reason: BatchExecutionReason;
  eligible_execution_modes: BatchExecutionMode[];
  wait_until: BatchWaitUntil;
  cleanup_inconsistent_bundle: boolean;
  applicant_id: string;
  queue_message_version: 1;
  queue_payload: string;
  state: BatchSubmissionState;
  created_at: string;
  updated_at: string;
  expectation_recorded_at: string | null;
  published_at: string | null;
  last_error: string | null;
}

export interface BatchGraphqlOperationInput {
  query: string;
  variables?: string | null;
  operationName?: string | null;
  objectId?: string | null;
  executionGroup?: number | null;
  executionPhase?: number | null;
  files?: BatchGraphqlFileInput[] | null;
}

export interface BatchGraphqlFileInput {
  path: string;
  name: string;
  mimeType: string;
  data: string;
}

export interface BatchGraphqlExecutionPlanInput {
  executionPhases: Array<{
    objectIds: string[];
    phase: number;
  }>;
  version: number;
}
