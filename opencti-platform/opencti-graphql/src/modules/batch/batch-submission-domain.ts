import { FunctionalError } from '../../config/errors';
import { elIndex, elLoadById, elUpdate } from '../../database/engine';
import { INDEX_INTERNAL_OBJECTS, READ_INDEX_INTERNAL_OBJECTS } from '../../database/utils';
import { BASE_TYPE_ENTITY } from '../../schema/general';
import { getParentTypes } from '../../schema/schemaUtils';
import type { AuthContext } from '../../types/user';
import { SYSTEM_USER } from '../../utils/access';
import { now } from '../../utils/format';
import { hashSHA256 } from '../../utils/hash';
import {
  type BatchAdmission,
  type BatchQueueMessage,
  type BatchSubmission,
  BatchSubmissionState,
  type BatchSubmissionWorkOrigin,
  ENTITY_TYPE_BATCH_SUBMISSION,
} from './batch-types';

const BATCH_SUBMISSION_PREFIX = 'batch-submission--';
const BATCH_SUBMISSION_QUEUE_MESSAGE_VERSION = 1;
const BATCH_SUBMISSION_STATE_ORDER = {
  [BatchSubmissionState.Reserved]: 0,
  [BatchSubmissionState.WorkBound]: 1,
  [BatchSubmissionState.ExpectationRecorded]: 2,
  [BatchSubmissionState.Published]: 3,
};

export interface ReserveBatchSubmissionInput {
  admission: BatchAdmission;
  applicantId: string;
  payloadFingerprint: string;
  queueMessage: BatchQueueMessage;
  workOrigin: BatchSubmissionWorkOrigin;
  workTimestamp?: string | null;
}

type BatchSubmissionStatePatch = Partial<Pick<BatchSubmission, 'expectation_recorded_at' | 'published_at' | 'last_error'>>;

const getBatchSubmissionStateOrder = (state: BatchSubmissionState): number => {
  const order = BATCH_SUBMISSION_STATE_ORDER[state];
  if (order === undefined) {
    throw FunctionalError('Invalid batch submission state', { state });
  }
  return order;
};

export const buildBatchSubmissionId = (connectorId: string, idempotencyKey: string): string => {
  return `${BATCH_SUBMISSION_PREFIX}${hashSHA256(JSON.stringify([connectorId, idempotencyKey]))}`;
};

export const loadBatchSubmission = async (
  context: AuthContext,
  connectorId: string,
  idempotencyKey: string,
): Promise<BatchSubmission | null> => {
  const submissionId = buildBatchSubmissionId(connectorId, idempotencyKey);
  const submission = await elLoadById(context, SYSTEM_USER, submissionId, {
    type: ENTITY_TYPE_BATCH_SUBMISSION,
    indices: READ_INDEX_INTERNAL_OBJECTS,
  });
  return submission ? submission as unknown as BatchSubmission : null;
};

export const isBatchSubmissionStateAtLeast = (submission: BatchSubmission, state: BatchSubmissionState): boolean => {
  return getBatchSubmissionStateOrder(submission.state) >= getBatchSubmissionStateOrder(state);
};

export const reserveBatchSubmission = async (
  context: AuthContext,
  input: ReserveBatchSubmissionInput,
): Promise<BatchSubmission> => {
  const createdAt = now();
  const submissionId = input.admission.submissionId ?? buildBatchSubmissionId(input.admission.connectorId, input.admission.idempotencyKey);
  const submission: BatchSubmission = {
    id: submissionId,
    internal_id: submissionId,
    standard_id: submissionId,
    entity_type: ENTITY_TYPE_BATCH_SUBMISSION,
    base_type: BASE_TYPE_ENTITY,
    parent_types: getParentTypes(ENTITY_TYPE_BATCH_SUBMISSION),
    connector_id: input.admission.connectorId,
    idempotency_key: input.admission.idempotencyKey,
    payload_fingerprint: input.payloadFingerprint,
    bundle_id: input.admission.bundleId,
    work_id: input.admission.workId,
    work_origin: input.workOrigin,
    work_timestamp: input.workTimestamp ?? null,
    execution_preference: input.admission.executionPreference,
    execution_mode: input.admission.executionMode,
    execution_reason: input.admission.executionReason,
    eligible_execution_modes: input.admission.eligibleExecutionModes,
    wait_until: input.admission.waitUntil,
    cleanup_inconsistent_bundle: input.admission.cleanupInconsistentBundle,
    applicant_id: input.applicantId,
    queue_message_version: BATCH_SUBMISSION_QUEUE_MESSAGE_VERSION,
    queue_payload: JSON.stringify(input.queueMessage),
    state: BatchSubmissionState.Reserved,
    created_at: createdAt,
    updated_at: createdAt,
    expectation_recorded_at: null,
    published_at: null,
    last_error: null,
  };
  await elIndex(INDEX_INTERNAL_OBJECTS, submission, { context });
  return submission;
};

export const advanceBatchSubmissionState = async (
  context: AuthContext,
  submission: BatchSubmission,
  state: BatchSubmissionState,
  patch: BatchSubmissionStatePatch = {},
): Promise<BatchSubmission> => {
  if (isBatchSubmissionStateAtLeast(submission, state)) {
    return submission;
  }
  const updatedSubmission = {
    ...submission,
    ...patch,
    state,
    updated_at: now(),
    last_error: null,
  };
  await elUpdate(context, submission._index ?? INDEX_INTERNAL_OBJECTS, submission.internal_id, {
    doc: {
      ...patch,
      state,
      updated_at: updatedSubmission.updated_at,
      last_error: null,
    },
  });
  return updatedSubmission;
};

export const recordBatchSubmissionError = async (
  context: AuthContext,
  submission: BatchSubmission,
  error: unknown,
): Promise<BatchSubmission> => {
  const updatedSubmission = {
    ...submission,
    updated_at: now(),
    last_error: error instanceof Error ? error.message : String(error),
  };
  await elUpdate(context, submission._index ?? INDEX_INTERNAL_OBJECTS, submission.internal_id, {
    doc: {
      updated_at: updatedSubmission.updated_at,
      last_error: updatedSubmission.last_error,
    },
  });
  return updatedSubmission;
};

export const readBatchSubmissionQueueMessage = (submission: BatchSubmission): BatchQueueMessage => {
  try {
    const queueMessage = JSON.parse(submission.queue_payload) as BatchQueueMessage;
    if (queueMessage.submission_id !== submission.internal_id) {
      throw FunctionalError('Invalid batch submission queue payload', {
        submission_id: submission.internal_id,
        queue_submission_id: queueMessage.submission_id,
      });
    }
    return queueMessage;
  } catch (cause) {
    throw FunctionalError('Invalid batch submission queue payload', {
      cause,
      submission_id: submission.internal_id,
    });
  }
};
