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

export enum BatchAdmissionErrorCode {
  InvalidBundle = 'INVALID_BUNDLE',
  InvalidBundleId = 'INVALID_BUNDLE_ID',
  InvalidConnectorId = 'INVALID_CONNECTOR_ID',
  InvalidIdempotencyKey = 'INVALID_IDEMPOTENCY_KEY',
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
  objects: Record<string, any>[];
  objectCount: number;
  objectTypes: string[];
  executionPreference: BatchExecutionPreference;
  executionMode: BatchExecutionMode;
  executionReason: BatchExecutionReason;
  eligibleExecutionModes: BatchExecutionMode[];
  waitUntil: BatchWaitUntil;
  idempotencyKey: string;
  cleanupInconsistentBundle: boolean;
}

export interface BatchAdmission {
  batchId: string;
  bundleId: string;
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
}

export interface BatchGraphqlOperationInput {
  query: string;
  variables?: string | null;
  operationName?: string | null;
}
