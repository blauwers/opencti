import type { GraphQLResolveInfo } from 'graphql';
import { submitStixBundle } from '../../domain/stix';
import { loadBatchDeliveryHandoff, markBatchDeliveryChildrenPublished, reserveBatchDeliveryChildren } from './batch-delivery-domain';
import { loadBatchExecutionReconciliation } from './batch-execution-reconciliation-domain';
import { loadBatchExecutionReceipt, readBatchExecutionReceiptResultMetadata } from './batch-execution-receipt-domain';
import { executeBatchGraphqlOperations } from './batch-operation-executor';
import type {
  BatchDirectDeliveryContext,
  BatchDeliveryBranchKind,
  BatchDeliveryChildReservationInput,
  BatchDeliveryKind,
  BatchDeliveryProtocol,
  BatchExecutionMode,
  BatchExecutionPreference,
  BatchGraphqlExecutionPlanInput,
  BatchGraphqlFileInput,
  BatchGraphqlOperationInput,
  BatchQueueMessage,
  BatchWaitUntil,
} from './batch-types';

interface BatchSubmitOptionsInput {
  wait_until?: BatchWaitUntil | null;
  execution_preference?: BatchExecutionPreference | null;
  idempotency_key?: string | null;
  split_bundles?: boolean | null;
  cleanup_inconsistent_bundle?: boolean | null;
}

interface BatchExecuteOptionsInput {
  wait_until?: BatchWaitUntil | null;
  execution_mode?: BatchExecutionMode | null;
  batch_plan?: BatchGraphqlExecutionPlanInputValue | null;
  direct_delivery_context?: BatchDirectDeliveryContextInputValue | null;
}

interface BatchGraphqlOperationInputValue {
  query: string;
  variables?: string | null;
  operation_name?: string | null;
  object_id?: string | null;
  execution_group?: number | null;
  execution_phase?: number | null;
  files?: BatchGraphqlFileInputValue[] | null;
}

interface BatchGraphqlFileInputValue {
  path: string;
  name: string;
  mime_type: string;
  data: string;
}

interface BatchGraphqlExecutionPlanInputValue {
  version: number;
  execution_phases: Array<{
    object_ids: string[];
    phase: number;
  }>;
}

interface BatchDirectDeliveryContextInputValue {
  submission_id: string;
  delivery_id: string;
  parent_delivery_id?: string | null;
  delivery_kind: BatchDeliveryKind;
  delivery_protocol_version: BatchDeliveryProtocol.V2;
  delivery_branch_kind: BatchDeliveryBranchKind;
  delivery_branch_sequence: number;
  delivery_branch_ordinal: number;
}

interface BatchDeliveryChildReservationInputValue {
  branch_kind: Exclude<BatchDeliveryBranchKind, BatchDeliveryBranchKind.Root>;
  branch_sequence: number;
  branch_ordinal: number;
  queue_payload: string;
}

const parseBatchDeliveryQueuePayload = (queuePayload: string): BatchQueueMessage => {
  return JSON.parse(queuePayload) as BatchQueueMessage;
};

const batchResolvers = {
  Query: {
    batchDeliveryHandoff: (
      _: unknown,
      { parent_delivery_id }: { parent_delivery_id: string },
      context: any,
    ) => loadBatchDeliveryHandoff(context, parent_delivery_id),
    batchExecutionReceipt: (
      _: unknown,
      { delivery_id }: { delivery_id: string },
      context: any,
    ) => loadBatchExecutionReceipt(context, delivery_id),
    batchExecutionReconciliation: (
      _: unknown,
      { delivery_id }: { delivery_id: string },
      context: any,
    ) => loadBatchExecutionReconciliation(context, delivery_id),
  },
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
    batchMutationsExecute: (
      _: unknown,
      {
        operations,
        options,
      }: { operations: BatchGraphqlOperationInputValue[]; options?: BatchExecuteOptionsInput | null },
      context: any,
      info: GraphQLResolveInfo,
    ) => executeBatchGraphqlOperations(info.schema, context, operations.map((operation): BatchGraphqlOperationInput => ({
      query: operation.query,
      variables: operation.variables,
      operationName: operation.operation_name,
      objectId: operation.object_id,
      executionGroup: operation.execution_group,
      executionPhase: operation.execution_phase,
      files: operation.files?.map((file): BatchGraphqlFileInput => ({
        path: file.path,
        name: file.name,
        mimeType: file.mime_type,
        data: file.data,
      })),
    })), {
      executionMode: options?.execution_mode ?? undefined,
      waitUntil: options?.wait_until ?? undefined,
      pruneUnusedResultFields: true,
      bundlePlan: options?.batch_plan
        ? {
          version: options.batch_plan.version,
          executionPhases: options.batch_plan.execution_phases.map((phase) => ({
            phase: phase.phase,
            objectIds: phase.object_ids,
          })),
        } satisfies BatchGraphqlExecutionPlanInput
        : undefined,
      directDeliveryContext: options?.direct_delivery_context
        ? {
          submission_id: options.direct_delivery_context.submission_id,
          delivery_id: options.direct_delivery_context.delivery_id,
          parent_delivery_id: options.direct_delivery_context.parent_delivery_id ?? null,
          delivery_kind: options.direct_delivery_context.delivery_kind,
          delivery_protocol_version: options.direct_delivery_context.delivery_protocol_version,
          delivery_branch_kind: options.direct_delivery_context.delivery_branch_kind,
          delivery_branch_sequence: options.direct_delivery_context.delivery_branch_sequence,
          delivery_branch_ordinal: options.direct_delivery_context.delivery_branch_ordinal,
        } satisfies BatchDirectDeliveryContext
        : undefined,
    }),
    batchDeliveryReserveChildren: (
      _: unknown,
      {
        parent_delivery_id,
        children,
      }: { parent_delivery_id: string; children: BatchDeliveryChildReservationInputValue[] },
      context: any,
    ) => reserveBatchDeliveryChildren(context, parent_delivery_id, children.map((child): BatchDeliveryChildReservationInput => ({
      branchKind: child.branch_kind,
      branchSequence: child.branch_sequence,
      branchOrdinal: child.branch_ordinal,
      queueMessage: parseBatchDeliveryQueuePayload(child.queue_payload),
    }))),
    batchDeliveryMarkChildrenPublished: (
      _: unknown,
      {
        parent_delivery_id,
        child_delivery_ids,
      }: { parent_delivery_id: string; child_delivery_ids: string[] },
      context: any,
    ) => markBatchDeliveryChildrenPublished(context, parent_delivery_id, child_delivery_ids),
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
  BatchMutationExecution: {
    operation_count: (execution: any) => execution.results.length,
    operation_errors: (execution: any) => execution.operationErrors,
    execution_mode: (execution: any) => execution.executionMode,
    wait_until: (execution: any) => execution.waitUntil,
    side_effect_kinds: (execution: any) => execution.sideEffectKinds,
  },
  BatchMutationOperationError: {
    operation_index: (operationError: any) => operationError.operationIndex,
    object_id: (operationError: any) => operationError.objectId,
  },
  BatchDeliveryHandoff: {
    parent_delivery_id: (handoff: any) => handoff.parentDelivery.internal_id,
    handoff_evidence: (handoff: any) => handoff.parentDelivery.handoff_evidence,
    child_set_fingerprint: (handoff: any) => handoff.parentDelivery.child_set_fingerprint,
    child_count: (handoff: any) => handoff.parentDelivery.child_count,
    children: (handoff: any) => handoff.children,
    pending_children: (handoff: any) => handoff.pendingChildren,
  },
  BatchDeliveryChild: {
    delivery_id: (delivery: any) => delivery.internal_id,
    state: (delivery: any) => delivery.state,
    queue_payload: (delivery: any) => delivery.queue_payload,
  },
  BatchExecutionReceipt: {
    receipt_id: (receipt: any) => receipt.internal_id,
    result_operation_errors: (receipt: any) => readBatchExecutionReceiptResultMetadata(receipt)?.operationErrors ?? null,
  },
  BatchExecutionReconciliation: {
    reconciliation_id: (reconciliation: any) => reconciliation.internal_id,
  },
};

export default batchResolvers;
