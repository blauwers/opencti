import { v4 as uuidv4 } from 'uuid';
import { FunctionalError } from '../../config/errors';
import {
  BatchAdmissionErrorCode,
  BatchAdmissionStatus,
  BatchExecutionMode,
  BatchExecutionPreference,
  BatchExecutionReason,
  type BatchAdmission,
  type BatchQueueMessage,
  type BatchSubmitOptions,
  BatchWaitUntil,
  type PreparedBundleSubmission,
} from './batch-types';

const BUNDLE_PREFIX = 'bundle--';
const IDENTITY_TYPE = 'identity';
const INDICATOR_TYPE = 'indicator';

const batchContractError = (message: string, code: BatchAdmissionErrorCode, data: Record<string, unknown> = {}) => {
  return FunctionalError(message, { batch_error_code: code, ...data });
};

const normalizeBundleId = (bundleId: unknown): string => {
  if (bundleId === undefined || bundleId === null || bundleId === '') {
    return `${BUNDLE_PREFIX}${uuidv4()}`;
  }
  if (typeof bundleId !== 'string' || !bundleId.startsWith(BUNDLE_PREFIX)) {
    throw batchContractError('Invalid batch bundle id', BatchAdmissionErrorCode.InvalidBundleId, { bundle_id: bundleId });
  }
  return bundleId;
};

const normalizeWaitUntil = (waitUntil: BatchSubmitOptions['waitUntil']): BatchWaitUntil => {
  if (waitUntil === undefined || waitUntil === null) {
    return BatchWaitUntil.Materialized;
  }
  if (waitUntil === BatchWaitUntil.Committed || waitUntil === BatchWaitUntil.Materialized) {
    return waitUntil;
  }
  throw batchContractError('Invalid batch wait_until value', BatchAdmissionErrorCode.InvalidWaitUntil, { wait_until: waitUntil });
};

const hasOpenCtiOperation = (object: Record<string, any>): boolean => {
  if (object.opencti_operation !== undefined || object.x_opencti_operation !== undefined) {
    return true;
  }
  return Object.values(object.extensions ?? {}).some((extension) => {
    return typeof extension === 'object'
      && extension !== null
      && ('opencti_operation' in extension || 'x_opencti_operation' in extension);
  });
};

const isIdentityIndicatorAtomicCohort = (objects: Record<string, any>[]): boolean => {
  if (objects.length < 2 || objects.some((object) => hasOpenCtiOperation(object))) {
    return false;
  }
  const identities = objects.filter((object) => object.type === IDENTITY_TYPE);
  const indicators = objects.filter((object) => object.type === INDICATOR_TYPE);
  if (identities.length !== 1 || indicators.length !== objects.length - 1) {
    return false;
  }
  const identityId = identities[0].id;
  return typeof identityId === 'string'
    && indicators.every((indicator) => indicator.created_by_ref === identityId);
};

const normalizeExecutionPreference = (options: BatchSubmitOptions, objects: Record<string, any>[]): {
  executionPreference: BatchExecutionPreference;
  executionMode: BatchExecutionMode;
  executionReason: BatchExecutionReason;
  eligibleExecutionModes: BatchExecutionMode[];
} => {
  if (options.splitBundles === true) {
    return {
      executionPreference: BatchExecutionPreference.LegacySplit,
      executionMode: BatchExecutionMode.LegacySplit,
      executionReason: BatchExecutionReason.ExplicitLegacySplit,
      eligibleExecutionModes: [BatchExecutionMode.LegacySplit],
    };
  }
  const executionPreference = options.executionPreference ?? BatchExecutionPreference.Auto;
  const atomicEligible = isIdentityIndicatorAtomicCohort(objects);
  const eligibleExecutionModes = [
    ...(atomicEligible ? [BatchExecutionMode.Atomic] : []),
    BatchExecutionMode.Bulk,
    BatchExecutionMode.Compatibility,
  ];
  if (executionPreference === BatchExecutionPreference.LegacySplit) {
    return {
      executionPreference,
      executionMode: BatchExecutionMode.LegacySplit,
      executionReason: BatchExecutionReason.ExplicitLegacySplit,
      eligibleExecutionModes: [BatchExecutionMode.LegacySplit],
    };
  }
  if (executionPreference === BatchExecutionPreference.Compatibility) {
    return {
      executionPreference,
      executionMode: BatchExecutionMode.Compatibility,
      executionReason: BatchExecutionReason.ExplicitCompatibility,
      eligibleExecutionModes,
    };
  }
  if (executionPreference === BatchExecutionPreference.Atomic) {
    if (!atomicEligible) {
      throw batchContractError(
        'Requested batch execution preference is not eligible for this bundle',
        BatchAdmissionErrorCode.ExecutionPreferenceNotEligible,
        { execution_preference: executionPreference, eligible_execution_modes: eligibleExecutionModes },
      );
    }
    return {
      executionPreference,
      executionMode: BatchExecutionMode.Atomic,
      executionReason: BatchExecutionReason.IdentityIndicatorAtomicCohort,
      eligibleExecutionModes,
    };
  }
  if (executionPreference === BatchExecutionPreference.Bulk) {
    return {
      executionPreference,
      executionMode: BatchExecutionMode.Bulk,
      executionReason: BatchExecutionReason.GenericBulkCompatible,
      eligibleExecutionModes,
    };
  }
  if (executionPreference !== BatchExecutionPreference.Auto) {
    throw batchContractError(
      'Requested batch execution preference is not available',
      BatchAdmissionErrorCode.UnsupportedExecutionPreference,
      { execution_preference: executionPreference },
    );
  }
  if (atomicEligible) {
    return {
      executionPreference,
      executionMode: BatchExecutionMode.Atomic,
      executionReason: BatchExecutionReason.IdentityIndicatorAtomicCohort,
      eligibleExecutionModes,
    };
  }
  return {
    executionPreference,
    executionMode: BatchExecutionMode.Bulk,
    executionReason: BatchExecutionReason.GenericBulkCompatible,
    eligibleExecutionModes,
  };
};

const normalizeIdempotencyKey = (idempotencyKey: BatchSubmitOptions['idempotencyKey'], bundleId: string): string => {
  if (idempotencyKey === undefined || idempotencyKey === null || idempotencyKey === '') {
    return bundleId;
  }
  if (typeof idempotencyKey !== 'string') {
    throw batchContractError('Invalid batch idempotency key', BatchAdmissionErrorCode.InvalidIdempotencyKey, { idempotency_key: idempotencyKey });
  }
  return idempotencyKey;
};

export const prepareBundleSubmission = (bundle: string, options: BatchSubmitOptions = {}): PreparedBundleSubmission => {
  if (typeof bundle !== 'string') {
    throw batchContractError('Invalid stix bundle payload', BatchAdmissionErrorCode.InvalidBundle);
  }

  let jsonBundle: Record<string, any>;
  try {
    jsonBundle = JSON.parse(bundle);
  } catch (err) {
    throw batchContractError('Invalid stix bundle payload', BatchAdmissionErrorCode.InvalidBundle, { cause: err });
  }

  if (jsonBundle.type !== 'bundle' || !Array.isArray(jsonBundle.objects) || jsonBundle.objects.length === 0) {
    throw batchContractError('Invalid stix bundle payload', BatchAdmissionErrorCode.InvalidBundle);
  }

  const bundleId = normalizeBundleId(jsonBundle.id);
  const normalizedBundle = { ...jsonBundle, id: bundleId };
  const { executionPreference, executionMode, executionReason, eligibleExecutionModes } = normalizeExecutionPreference(options, jsonBundle.objects);
  const waitUntil = normalizeWaitUntil(options.waitUntil);
  const idempotencyKey = normalizeIdempotencyKey(options.idempotencyKey, bundleId);
  const objectTypes = Array.from(new Set(jsonBundle.objects
    .map((object: Record<string, unknown>) => object?.type)
    .filter((type: unknown): type is string => typeof type === 'string')));

  return {
    bundle: JSON.stringify(normalizedBundle),
    bundleId,
    objects: jsonBundle.objects,
    objectCount: jsonBundle.objects.length,
    objectTypes,
    executionPreference,
    executionMode,
    executionReason,
    eligibleExecutionModes,
    waitUntil,
    idempotencyKey,
    cleanupInconsistentBundle: options.cleanupInconsistentBundle === true,
  };
};

export const buildBatchAdmission = (
  connectorId: string,
  workId: string,
  prepared: PreparedBundleSubmission,
): BatchAdmission => {
  if (typeof connectorId !== 'string' || connectorId.length === 0) {
    throw batchContractError('Invalid batch connector id', BatchAdmissionErrorCode.InvalidConnectorId, { connector_id: connectorId });
  }
  return {
    batchId: prepared.bundleId,
    bundleId: prepared.bundleId,
    connectorId,
    workId,
    objectCount: prepared.objectCount,
    objectTypes: prepared.objectTypes,
    executionPreference: prepared.executionPreference,
    executionMode: prepared.executionMode,
    executionReason: prepared.executionReason,
    eligibleExecutionModes: prepared.eligibleExecutionModes,
    waitUntil: prepared.waitUntil,
    status: BatchAdmissionStatus.Accepted,
    idempotencyKey: prepared.idempotencyKey,
    cleanupInconsistentBundle: prepared.cleanupInconsistentBundle,
    bundle: prepared.bundle,
  };
};

export const buildBatchQueueMessage = (admission: BatchAdmission, applicantId: string): BatchQueueMessage => {
  const splitBundles = admission.executionMode === BatchExecutionMode.LegacySplit;
  return {
    type: 'bundle',
    applicant_id: applicantId,
    content: Buffer.from(admission.bundle, 'utf-8').toString('base64'),
    work_id: admission.workId,
    update: true,
    no_split: !splitBundles,
    split_bundles: splitBundles,
    cleanup_inconsistent_bundle: admission.cleanupInconsistentBundle,
    batch_id: admission.batchId,
    batch_execution_mode: admission.executionMode,
    batch_execution_reason: admission.executionReason,
    batch_eligible_execution_modes: admission.eligibleExecutionModes,
    batch_wait_until: admission.waitUntil,
    batch_idempotency_key: admission.idempotencyKey,
  };
};
