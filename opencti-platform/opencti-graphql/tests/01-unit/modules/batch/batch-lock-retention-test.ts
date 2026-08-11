import { describe, expect, it } from 'vitest';
import { resolveBatchRequestTimeout } from '../../../../src/config/conf-utils';
import { resolveBatchDirectDeliveryExecutionRetryCount } from '../../../../src/modules/batch/batch-lock-retention';

describe('batch lock retention', () => {
  it('derives the effective batch request timeout from the larger configured request budget', () => {
    expect(resolveBatchRequestTimeout(1200000, 3600000)).toBe(3600000);
    expect(resolveBatchRequestTimeout(7200000, 3600000)).toBe(7200000);
    expect(resolveBatchRequestTimeout(undefined, undefined)).toBe(3600000);
  });

  it('uses the full batch request budget for direct-delivery serialization waits', () => {
    expect(resolveBatchDirectDeliveryExecutionRetryCount({
      batchRequestTimeoutMs: 3600000,
      batchRetryCount: 3600,
      retryDelayMs: 250,
    })).toBe(14400);
  });

  it('never shortens the existing long-wait budget when the request window is smaller', () => {
    expect(resolveBatchDirectDeliveryExecutionRetryCount({
      batchRequestTimeoutMs: 600000,
      batchRetryCount: 3600,
      retryDelayMs: 250,
    })).toBe(3600);
  });

  it('keeps the existing long-wait budget when the timeout inputs are not usable', () => {
    expect(resolveBatchDirectDeliveryExecutionRetryCount({
      batchRequestTimeoutMs: undefined,
      batchRetryCount: 3600,
      retryDelayMs: 250,
    })).toBe(3600);
    expect(resolveBatchDirectDeliveryExecutionRetryCount({
      batchRequestTimeoutMs: 3600000,
      batchRetryCount: 3600,
      retryDelayMs: 0,
    })).toBe(3600);
  });
});
