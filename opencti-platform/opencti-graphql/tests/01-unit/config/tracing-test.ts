import { describe, expect, it, vi } from 'vitest';

vi.mock('opentelemetry-node-metrics', () => ({
  default: vi.fn(),
}));

vi.mock('../../../src/config/conf', () => ({
  ENABLED_METRICS: false,
  ENABLED_TRACING: false,
}));

import { MeterManager } from '../../../src/config/tracing';

describe('MeterManager', () => {
  it('registers bounded mutation counters and records only the supplied stable labels', () => {
    const counters = new Map<string, { add: ReturnType<typeof vi.fn> }>();
    const meter = {
      createCounter: vi.fn((name: string) => {
        const counter = { add: vi.fn() };
        counters.set(name, counter);
        return counter;
      }),
      createHistogram: vi.fn(() => ({ record: vi.fn() })),
      createGauge: vi.fn(() => ({ record: vi.fn() })),
    };
    const manager = new MeterManager({
      getMeter: vi.fn(() => meter),
    } as any);

    manager.registerMetrics();
    manager.mutationOutcome({ mutation_kind: 'UPDATE_ATTRIBUTE', outcome: 'unchanged' });
    manager.mutationSuppression({ mutation_kind: 'UPDATE_ATTRIBUTE', suppression: 'elastic_write' });

    expect(meter.createCounter.mock.calls.map(([name]) => name)).toEqual([
      'opencti_sent_email',
      'opencti_api_requests',
      'opencti_api_errors',
      'opencti_api_mutation_outcomes',
      'opencti_api_mutation_suppressions',
    ]);
    expect(counters.get('opencti_api_mutation_outcomes')?.add).toHaveBeenCalledWith(1, {
      mutation_kind: 'UPDATE_ATTRIBUTE',
      outcome: 'unchanged',
    });
    expect(counters.get('opencti_api_mutation_suppressions')?.add).toHaveBeenCalledWith(1, {
      mutation_kind: 'UPDATE_ATTRIBUTE',
      suppression: 'elastic_write',
    });
  });

  it('does not fail when metrics have not been registered', () => {
    const manager = new MeterManager({} as any);

    expect(() => manager.mutationOutcome({ mutation_kind: 'UPDATE_ATTRIBUTE', outcome: 'unchanged' })).not.toThrow();
    expect(() => manager.mutationSuppression({ mutation_kind: 'UPDATE_ATTRIBUTE', suppression: 'elastic_write' })).not.toThrow();
  });
});
