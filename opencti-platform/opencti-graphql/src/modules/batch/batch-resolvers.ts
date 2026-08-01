import { submitStixBundle } from '../../domain/stix';
import type { BatchExecutionPreference, BatchWaitUntil } from './batch-types';

interface BatchSubmitOptionsInput {
  wait_until?: BatchWaitUntil | null;
  execution_preference?: BatchExecutionPreference | null;
  idempotency_key?: string | null;
  split_bundles?: boolean | null;
  cleanup_inconsistent_bundle?: boolean | null;
}

const batchResolvers = {
  Mutation: {
    stixBundleSubmit: (
      _: unknown,
      {
        connectorId,
        bundle,
        work_id,
        options,
      }: { connectorId: string; bundle: string; work_id?: string | null; options?: BatchSubmitOptionsInput | null },
      context: any,
    ) => submitStixBundle(context, context.user, connectorId, bundle, work_id, {
      waitUntil: options?.wait_until,
      executionPreference: options?.execution_preference,
      idempotencyKey: options?.idempotency_key,
      splitBundles: options?.split_bundles,
      cleanupInconsistentBundle: options?.cleanup_inconsistent_bundle,
    }),
  },
  BatchAdmission: {
    batch_id: (admission: any) => admission.batchId,
    bundle_id: (admission: any) => admission.bundleId,
    work_id: (admission: any) => admission.workId,
    object_count: (admission: any) => admission.objectCount,
    object_types: (admission: any) => admission.objectTypes,
    execution_preference: (admission: any) => admission.executionPreference,
    execution_mode: (admission: any) => admission.executionMode,
    execution_reason: (admission: any) => admission.executionReason,
    eligible_execution_modes: (admission: any) => admission.eligibleExecutionModes,
    wait_until: (admission: any) => admission.waitUntil,
    idempotency_key: (admission: any) => admission.idempotencyKey,
  },
};

export default batchResolvers;
