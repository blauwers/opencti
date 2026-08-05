import { describe, expect, it } from 'vitest';
import { createBatchExecutionAdmissionGate } from '../../../../src/modules/batch/batch-execution-admission';

describe('batch execution admission gate', () => {
  it('transfers released permits to queued executions in order', async () => {
    const gate = createBatchExecutionAdmissionGate(2);
    const releaseFirst = await gate.acquire();
    const releaseSecond = await gate.acquire();
    const admitted: string[] = [];
    const thirdReleasePromise = gate.acquire().then((release) => {
      admitted.push('third');
      return release;
    });
    const fourthReleasePromise = gate.acquire().then((release) => {
      admitted.push('fourth');
      return release;
    });

    expect(gate.snapshot()).toEqual({
      activeExecutions: 2,
      maxActiveExecutions: 2,
      waitingExecutions: 2,
    });

    releaseFirst();
    const releaseThird = await thirdReleasePromise;
    expect(admitted).toEqual(['third']);
    expect(gate.snapshot()).toEqual({
      activeExecutions: 2,
      maxActiveExecutions: 2,
      waitingExecutions: 1,
    });

    releaseSecond();
    const releaseFourth = await fourthReleasePromise;
    expect(admitted).toEqual(['third', 'fourth']);
    expect(gate.snapshot()).toEqual({
      activeExecutions: 2,
      maxActiveExecutions: 2,
      waitingExecutions: 0,
    });

    releaseThird();
    releaseFourth();
    expect(gate.snapshot()).toEqual({
      activeExecutions: 0,
      maxActiveExecutions: 2,
      waitingExecutions: 0,
    });
  });

  it('treats repeated release calls as a no-op', async () => {
    const gate = createBatchExecutionAdmissionGate(1);
    const release = await gate.acquire();

    release();
    release();

    expect(gate.snapshot()).toEqual({
      activeExecutions: 0,
      maxActiveExecutions: 1,
      waitingExecutions: 0,
    });
  });
});
