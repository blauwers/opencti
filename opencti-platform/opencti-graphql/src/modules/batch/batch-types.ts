import type { BatchBundlePlan } from './batch-bundle-planner';
import type { StreamDataEvent } from '../../types/event';

export const ENTITY_TYPE_BATCH_SUBMISSION = 'BatchSubmission';
export const ENTITY_TYPE_BATCH_DELIVERY = 'BatchDelivery';
export const ENTITY_TYPE_BATCH_EXECUTION_RECEIPT = 'BatchExecutionReceipt';
export const ENTITY_TYPE_BATCH_EXECUTION_RECONCILIATION = 'BatchExecutionReconciliation';
export const ENTITY_TYPE_BATCH_STREAM_PUBLICATION_MANIFEST = 'BatchStreamPublicationManifest';
export const ENTITY_TYPE_BATCH_EXECUTION_RESULT_STAGING = 'BatchExecutionResultStaging';

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

export enum BatchDeliveryProtocol {
  V1 = 1,
  V2 = 2,
}

export enum BatchDeliveryKind {
  Root = 'ROOT',
  Child = 'CHILD',
}

export enum BatchDeliveryBranchKind {
  Root = 'ROOT',
  LegacySplit = 'LEGACY_SPLIT',
  OversizedChunk = 'OVERSIZED_CHUNK',
  IntactReplay = 'INTACT_REPLAY',
  TerminalDeadLetter = 'TERMINAL_DEAD_LETTER',
}

export enum BatchDeliveryState {
  Ready = 'READY',
  Published = 'PUBLISHED',
}

export enum BatchDeliveryHandoffEvidence {
  None = 'NONE',
  Planned = 'PLANNED',
  ChildrenReserved = 'CHILDREN_RESERVED',
  ChildrenPublished = 'CHILDREN_PUBLISHED',
}

export enum BatchExecutionReceiptState {
  Prepared = 'PREPARED',
  Started = 'STARTED',
  Completed = 'COMPLETED',
  FailedTerminal = 'FAILED_TERMINAL',
  RequiresReconciliation = 'REQUIRES_RECONCILIATION',
}

export enum BatchExecutionReceiptCompletionBoundary {
  Materialized = 'MATERIALIZED',
}

export enum BatchExecutionReceiptFailureProof {
  PreStartValidation = 'PRE_START_VALIDATION',
  NoEffectTerminal = 'NO_EFFECT_TERMINAL',
}

export enum BatchExecutionReconciliationState {
  Open = 'OPEN',
  RunningObserved = 'RUNNING_OBSERVED',
  MaterializationPending = 'MATERIALIZATION_PENDING',
  Ambiguous = 'AMBIGUOUS',
  ResolvedCompleted = 'RESOLVED_COMPLETED',
  ResolvedFailedTerminal = 'RESOLVED_FAILED_TERMINAL',
}

export enum BatchExecutionReconciliationOpenedReason {
  CommittedWithoutDurableMaterialization = 'COMMITTED_WITHOUT_DURABLE_MATERIALIZATION',
  PostStartError = 'POST_START_ERROR',
  ExplicitStartedReceipt = 'EXPLICIT_STARTED_RECEIPT',
  ExplicitRequiresReconciliationReceipt = 'EXPLICIT_REQUIRES_RECONCILIATION_RECEIPT',
}

export enum BatchExecutionReconciliationEvidenceClass {
  ExistingTerminalReceipt = 'EXISTING_TERMINAL_RECEIPT',
  MaterializationTerminal = 'MATERIALIZATION_TERMINAL',
  NoEffectTerminal = 'NO_EFFECT_TERMINAL',
  ActiveAttempt = 'ACTIVE_ATTEMPT',
  MaterializationHandoff = 'MATERIALIZATION_HANDOFF',
}

export enum BatchExecutionReconciliationEvidenceRefType {
  BatchExecutionReceipt = 'BATCH_EXECUTION_RECEIPT',
  BatchMaterializationHandoff = 'BATCH_MATERIALIZATION_HANDOFF',
  BackendAttemptObservation = 'BACKEND_ATTEMPT_OBSERVATION',
}

export enum BatchAdmissionErrorCode {
  InvalidBundle = 'INVALID_BUNDLE',
  InvalidBundleId = 'INVALID_BUNDLE_ID',
  InvalidConnectorId = 'INVALID_CONNECTOR_ID',
  InvalidIdempotencyKey = 'INVALID_IDEMPOTENCY_KEY',
  IdempotencyKeyConflict = 'IDEMPOTENCY_KEY_CONFLICT',
  DeliveryIdentityConflict = 'DELIVERY_IDENTITY_CONFLICT',
  ExecutionReceiptConflict = 'EXECUTION_RECEIPT_CONFLICT',
  ExecutionReconciliationConflict = 'EXECUTION_RECONCILIATION_CONFLICT',
  ExecutionRequiresReconciliation = 'EXECUTION_REQUIRES_RECONCILIATION',
  ExecutionFailedTerminal = 'EXECUTION_FAILED_TERMINAL',
  ExecutionResultStagingConflict = 'EXECUTION_RESULT_STAGING_CONFLICT',
  StreamPublicationProofConflict = 'STREAM_PUBLICATION_PROOF_CONFLICT',
  StreamPublicationManifestConflict = 'STREAM_PUBLICATION_MANIFEST_CONFLICT',
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
  enrichmentBatchResult?: string | null;
  additionalWorkIds?: string[] | null;
  fingerprintContext?: unknown;
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
  enrichmentBatchResult?: string;
  additionalWorkIds?: string[];
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
  rootDeliveryId?: string;
  requiredDeliveryProtocol?: BatchDeliveryProtocol;
  enrichmentBatchResult?: string;
  additionalWorkIds?: string[];
}

export interface BatchDeliveryEnvelope {
  delivery_id: string;
  parent_delivery_id: string | null;
  delivery_kind: BatchDeliveryKind;
  delivery_protocol_version: BatchDeliveryProtocol.V2;
  delivery_branch_kind: BatchDeliveryBranchKind;
  delivery_branch_sequence: number;
  delivery_branch_ordinal: number;
}

export interface BatchDirectDeliveryContext extends BatchDeliveryEnvelope {
  submission_id: string;
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
  enrichment_batch_result?: string;
  additional_work_ids?: string[];
  submission_id?: string;
  delivery_id?: string;
  parent_delivery_id?: string | null;
  delivery_kind?: BatchDeliveryKind;
  delivery_protocol_version?: BatchDeliveryProtocol.V2;
  delivery_branch_kind?: BatchDeliveryBranchKind;
  delivery_branch_sequence?: number;
  delivery_branch_ordinal?: number;
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
  root_delivery_id: string;
  required_delivery_protocol: BatchDeliveryProtocol;
  queue_message_version: 1;
  queue_payload: string;
  state: BatchSubmissionState;
  created_at: string;
  updated_at: string;
  expectation_recorded_at: string | null;
  published_at: string | null;
  last_error: string | null;
}

export interface BatchDelivery {
  id: string;
  internal_id: string;
  _index?: string;
  standard_id: string;
  entity_type: typeof ENTITY_TYPE_BATCH_DELIVERY;
  base_type: 'ENTITY';
  parent_types: string[];
  submission_id: string;
  parent_delivery_id: string | null;
  delivery_kind: BatchDeliveryKind;
  branch_kind: BatchDeliveryBranchKind;
  branch_sequence: number;
  branch_ordinal: number;
  payload_fingerprint: string;
  queue_payload_version: 1;
  queue_payload: string;
  required_worker_protocol: BatchDeliveryProtocol;
  state: BatchDeliveryState;
  handoff_evidence: BatchDeliveryHandoffEvidence;
  child_set_fingerprint: string | null;
  child_count: number;
  child_delivery_ids: string[];
  created_at: string;
  updated_at: string;
  published_at: string | null;
  children_reserved_at: string | null;
  children_published_at: string | null;
  last_error: string | null;
}

export interface BatchExecutionReceipt {
  id: string;
  internal_id: string;
  _index?: string;
  standard_id: string;
  entity_type: typeof ENTITY_TYPE_BATCH_EXECUTION_RECEIPT;
  base_type: 'ENTITY';
  parent_types: string[];
  delivery_id: string;
  submission_id: string;
  delivery_payload_fingerprint: string;
  request_contract_version: number;
  request_fingerprint: string;
  batch_plan_fingerprint: string;
  operation_manifest_fingerprint: string;
  operation_count: number;
  execution_mode: BatchExecutionMode;
  wait_until: BatchWaitUntil;
  state: BatchExecutionReceiptState;
  result_fingerprint: string | null;
  result_version: number | null;
  result_operation_count: number | null;
  result_operation_errors: string | null;
  result_execution_mode: BatchExecutionMode | null;
  result_wait_until: BatchWaitUntil | null;
  result_side_effect_kinds: string[];
  result_materialized: boolean | null;
  completion_boundary: BatchExecutionReceiptCompletionBoundary | null;
  side_effect_kind_counts: string | null;
  prepared_at: string;
  started_at: string | null;
  completed_at: string | null;
  materialized_at: string | null;
  failure_stage: string | null;
  failure_code: string | null;
  failure_message: string | null;
  failure_fingerprint: string | null;
  failure_retryable: false | null;
  failure_proof: BatchExecutionReceiptFailureProof | null;
  failed_at: string | null;
  reconciliation_required_at: string | null;
  created_at: string;
  updated_at: string;
  last_error: string | null;
}

export interface BatchExecutionReconciliation {
  id: string;
  internal_id: string;
  _index?: string;
  standard_id: string;
  entity_type: typeof ENTITY_TYPE_BATCH_EXECUTION_RECONCILIATION;
  base_type: 'ENTITY';
  parent_types: string[];
  receipt_id: string;
  delivery_id: string;
  submission_id: string;
  request_fingerprint: string;
  request_contract_version: number;
  opened_from_receipt_state: BatchExecutionReceiptState;
  opened_reason: BatchExecutionReconciliationOpenedReason;
  state: BatchExecutionReconciliationState;
  evidence_class: BatchExecutionReconciliationEvidenceClass | null;
  evidence_ref_type: BatchExecutionReconciliationEvidenceRefType | null;
  evidence_ref_id: string | null;
  evidence_fingerprint: string | null;
  attempt_observation_id: string | null;
  attempt_observed_at: string | null;
  attempt_expires_at: string | null;
  materialization_handoff_id: string | null;
  materialization_handoff_state: string | null;
  resolved_receipt_state: BatchExecutionReceiptState | null;
  opened_at: string;
  last_observed_at: string | null;
  resolved_at: string | null;
  created_at: string;
  updated_at: string;
  last_error: string | null;
}

export interface BatchBackendAttemptObservation {
  observation_id: string;
  receipt_id: string;
  delivery_id: string;
  submission_id: string;
  request_fingerprint: string;
  request_contract_version: number;
  receipt_started_at: string;
  backend_node_id: string;
  observed_at: string;
  expires_at: string;
  observation_version: number;
}

export interface BatchStreamPublicationProof {
  publication_id: string;
  event_fingerprint: string;
  stream_entry_id: string;
  published_at: string;
  proof_version: number;
}

export type BatchStreamPublicationEventSnapshot = StreamDataEvent & {
  event_id?: string;
};

export interface BatchStreamPublicationManifestEntry {
  publication_sequence: number;
  publication_key: string;
  publication_id: string;
  event_fingerprint: string;
  event_snapshot: BatchStreamPublicationEventSnapshot;
  event_snapshot_bytes: number;
}

export interface BatchStreamPublicationManifest {
  id: string;
  internal_id: string;
  _id?: string;
  _index?: string;
  sort?: unknown;
  standard_id: string;
  entity_type: typeof ENTITY_TYPE_BATCH_STREAM_PUBLICATION_MANIFEST;
  base_type: 'ENTITY';
  parent_types: string[];
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
  created_at: string;
  updated_at: string;
}

export interface BatchExecutionResultStaging {
  id: string;
  internal_id: string;
  _id?: string;
  _index?: string;
  sort?: unknown;
  standard_id: string;
  entity_type: typeof ENTITY_TYPE_BATCH_EXECUTION_RESULT_STAGING;
  base_type: 'ENTITY';
  parent_types: string[];
  staging_id: string;
  receipt_id: string;
  delivery_id: string;
  submission_id: string;
  request_fingerprint: string;
  request_contract_version: number;
  result_version: number;
  operation_count: number;
  operation_errors: BatchExecutionReceiptOperationError[];
  execution_mode: BatchExecutionMode;
  wait_until: BatchWaitUntil;
  side_effect_kinds: string[];
  serialized_bytes: number;
  staging_fingerprint: string;
  staged_at: string;
  created_at: string;
  updated_at: string;
}

export interface BatchDeliveryChildReservationInput {
  branchKind: Exclude<BatchDeliveryBranchKind, BatchDeliveryBranchKind.Root>;
  branchSequence: number;
  branchOrdinal: number;
  queueMessage: BatchQueueMessage;
}

export interface BatchDeliveryHandoff {
  parentDelivery: BatchDelivery;
  children: BatchDelivery[];
  pendingChildren: BatchDelivery[];
}

export interface BatchWorkerRuntimeCapabilityInput {
  worker_id?: string | null;
  batch_delivery_protocol_max?: number | null;
}

export interface BatchWorkerRuntimeCapability {
  worker_id: string;
  batch_delivery_protocol_max: BatchDeliveryProtocol;
  observed_at: string;
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

export interface BatchExecutionReceiptOperationManifest {
  query: string;
  variables: Record<string, unknown>;
  operationName: string | null;
  objectId: string | null;
  executionGroup: number | null;
  executionPhase: number | null;
  files: Array<{
    path: string;
    name: string;
    mimeType: string;
    contentHash: string;
    byteLength: number;
  }> | null;
}

export interface BatchExecutionReceiptRequestInput {
  delivery: BatchDelivery;
  executionMode: BatchExecutionMode;
  waitUntil: BatchWaitUntil;
  batchPlan: BatchGraphqlExecutionPlanInput | null;
  operations: BatchExecutionReceiptOperationManifest[];
}

export interface BatchExecutionReceiptRequestMetadata {
  requestContractVersion: number;
  requestFingerprint: string;
  batchPlanFingerprint: string;
  operationManifestFingerprint: string;
  operationCount: number;
}

export interface BatchExecutionReceiptOperationError {
  code?: string;
  message: string;
  objectId?: string;
  operationIndex: number;
  retryable: boolean;
}

export interface BatchExecutionResultStagingPayload {
  operationCount: number;
  operationErrors: BatchExecutionReceiptOperationError[];
  executionMode: BatchExecutionMode;
  waitUntil: BatchWaitUntil;
  sideEffectKinds: string[];
}

export interface BatchExecutionReceiptResultMetadata {
  operationCount: number;
  operationErrors: BatchExecutionReceiptOperationError[];
  executionMode: BatchExecutionMode;
  waitUntil: BatchWaitUntil;
  sideEffectKinds: string[];
  materialized: true;
}

export interface BatchExecutionReceiptTerminalFailure {
  stage: string;
  code?: string;
  message: string;
  proof: BatchExecutionReceiptFailureProof;
}

export interface BatchExecutionReconciliationTerminalEvidence {
  evidenceClass: BatchExecutionReconciliationEvidenceClass | string;
  evidenceRefType?: BatchExecutionReconciliationEvidenceRefType | string | null;
  evidenceRefId?: string | null;
  evidenceFingerprint?: string | null;
  receipt?: BatchExecutionReceipt | null;
  requestFingerprint?: string | null;
  resultMetadata?: BatchExecutionReceiptResultMetadata | null;
  materialized?: boolean | null;
}
