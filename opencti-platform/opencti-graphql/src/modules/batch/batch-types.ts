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
  objectCount: number;
  objectTypes: string[];
  executionPreference: BatchExecutionPreference;
  executionMode: BatchExecutionMode;
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
  batch_wait_until: BatchWaitUntil;
  batch_idempotency_key: string;
}
