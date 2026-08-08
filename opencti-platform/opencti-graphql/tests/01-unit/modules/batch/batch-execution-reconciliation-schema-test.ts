import { readFileSync } from 'node:fs';
import { Kind, parse } from 'graphql';
import { describe, expect, it } from 'vitest';

const batchSchema = readFileSync(
  new URL('../../../../src/modules/batch/batch.graphql', import.meta.url),
  'utf8',
);

describe('batch execution reconciliation schema', () => {
  it('exposes only bounded reconciliation metadata on the read surface', () => {
    const document = parse(batchSchema);
    const reconciliationType = document.definitions.find((definition) => {
      return definition.kind === Kind.OBJECT_TYPE_DEFINITION
        && definition.name.value === 'BatchExecutionReconciliation';
    });

    expect(reconciliationType?.kind).toBe(Kind.OBJECT_TYPE_DEFINITION);
    if (reconciliationType?.kind !== Kind.OBJECT_TYPE_DEFINITION) {
      throw new Error('BatchExecutionReconciliation type is missing');
    }

    const fieldNames = reconciliationType.fields?.map((field) => field.name.value) ?? [];
    expect(fieldNames).toEqual([
      'reconciliation_id',
      'receipt_id',
      'delivery_id',
      'submission_id',
      'request_fingerprint',
      'request_contract_version',
      'opened_from_receipt_state',
      'opened_reason',
      'state',
      'evidence_class',
      'evidence_ref_type',
      'evidence_ref_id',
      'evidence_fingerprint',
      'attempt_observation_id',
      'attempt_observed_at',
      'attempt_expires_at',
      'materialization_handoff_id',
      'materialization_handoff_state',
      'resolved_receipt_state',
      'opened_at',
      'last_observed_at',
      'resolved_at',
      'created_at',
      'updated_at',
      'last_error',
    ]);
    expect(fieldNames).not.toContain('queue_payload');
    expect(fieldNames).not.toContain('operations');
    expect(fieldNames).not.toContain('result_operation_errors');
    expect(fieldNames).not.toContain('work_id');

    const queryType = document.definitions.find((definition) => {
      return definition.kind === Kind.OBJECT_TYPE_DEFINITION
        && definition.name.value === 'Query';
    });
    const mutationType = document.definitions.find((definition) => {
      return definition.kind === Kind.OBJECT_TYPE_DEFINITION
        && definition.name.value === 'Mutation';
    });
    expect(queryType?.kind).toBe(Kind.OBJECT_TYPE_DEFINITION);
    expect(mutationType?.kind).toBe(Kind.OBJECT_TYPE_DEFINITION);
    if (queryType?.kind !== Kind.OBJECT_TYPE_DEFINITION || mutationType?.kind !== Kind.OBJECT_TYPE_DEFINITION) {
      throw new Error('Batch query or mutation type is missing');
    }
    expect(queryType.fields?.map((field) => field.name.value)).toContain('batchExecutionReconciliation');
    expect(mutationType.fields?.map((field) => field.name.value)).not.toContain('batchExecutionReconciliationOpen');
  });
});
