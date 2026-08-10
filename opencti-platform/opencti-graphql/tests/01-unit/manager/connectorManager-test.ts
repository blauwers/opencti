import { describe, expect, it } from 'vitest';
import { canReconcileWorkCompletionFromRedis } from '../../../src/domain/work';

describe('connector manager work reconciliation', () => {
  it('does not reconcile live work without any completion evidence', () => {
    expect(canReconcileWorkCompletionFromRedis({
      expected: 0,
      total: 0,
      isProcessed: false,
      isMultiPartWork: false,
    })).toBe(false);
  });

  it('does not reconcile incomplete work', () => {
    expect(canReconcileWorkCompletionFromRedis({
      expected: 1,
      total: 0,
      isProcessed: true,
      isMultiPartWork: false,
    })).toBe(false);
  });

  it('reconciles non-multipart work after a settled expectation report', () => {
    expect(canReconcileWorkCompletionFromRedis({
      expected: 1,
      total: 1,
      isProcessed: false,
      isMultiPartWork: false,
    })).toBe(true);
  });

  it('reconciles zero-output work only after connector processing completed', () => {
    expect(canReconcileWorkCompletionFromRedis({
      expected: 0,
      total: 0,
      isProcessed: true,
      isMultiPartWork: false,
    })).toBe(true);
  });

  it('requires multipart work to receive the connector completion signal', () => {
    expect(canReconcileWorkCompletionFromRedis({
      expected: 1,
      total: 1,
      isProcessed: false,
      isMultiPartWork: true,
    })).toBe(false);
    expect(canReconcileWorkCompletionFromRedis({
      expected: 1,
      total: 1,
      isProcessed: true,
      isMultiPartWork: true,
    })).toBe(true);
  });
});
