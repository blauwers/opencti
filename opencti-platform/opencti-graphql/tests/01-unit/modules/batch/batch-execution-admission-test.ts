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
      activeWeight: 2,
      maxActiveExecutions: 2,
      maxActiveWeight: 2,
      waitingExecutions: 2,
      waitingWeight: 2,
    });

    releaseFirst();
    const releaseThird = await thirdReleasePromise;
    expect(admitted).toEqual(['third']);
    expect(gate.snapshot()).toEqual({
      activeExecutions: 2,
      activeWeight: 2,
      maxActiveExecutions: 2,
      maxActiveWeight: 2,
      waitingExecutions: 1,
      waitingWeight: 1,
    });

    releaseSecond();
    const releaseFourth = await fourthReleasePromise;
    expect(admitted).toEqual(['third', 'fourth']);
    expect(gate.snapshot()).toEqual({
      activeExecutions: 2,
      activeWeight: 2,
      maxActiveExecutions: 2,
      maxActiveWeight: 2,
      waitingExecutions: 0,
      waitingWeight: 0,
    });

    releaseThird();
    releaseFourth();
    expect(gate.snapshot()).toEqual({
      activeExecutions: 0,
      activeWeight: 0,
      maxActiveExecutions: 2,
      maxActiveWeight: 2,
      waitingExecutions: 0,
      waitingWeight: 0,
    });
  });

  it('treats repeated release calls as a no-op', async () => {
    const gate = createBatchExecutionAdmissionGate(1);
    const release = await gate.acquire();

    release();
    release();

    expect(gate.snapshot()).toEqual({
      activeExecutions: 0,
      activeWeight: 0,
      maxActiveExecutions: 1,
      maxActiveWeight: 1,
      waitingExecutions: 0,
      waitingWeight: 0,
    });
  });

  it('preserves waiter order while admitting weighted executions', async () => {
    const gate = createBatchExecutionAdmissionGate(4);
    const releaseFirst = await gate.acquire(4);
    const admitted: string[] = [];
    const secondReleasePromise = gate.acquire(2).then((release) => {
      admitted.push('second');
      return release;
    });
    const thirdReleasePromise = gate.acquire(1).then((release) => {
      admitted.push('third');
      return release;
    });

    expect(gate.snapshot()).toEqual({
      activeExecutions: 1,
      activeWeight: 4,
      maxActiveExecutions: 4,
      maxActiveWeight: 4,
      waitingExecutions: 2,
      waitingWeight: 3,
    });

    releaseFirst();
    const releaseSecond = await secondReleasePromise;
    const releaseThird = await thirdReleasePromise;

    expect(admitted).toEqual(['second', 'third']);
    expect(gate.snapshot()).toEqual({
      activeExecutions: 2,
      activeWeight: 3,
      maxActiveExecutions: 4,
      maxActiveWeight: 4,
      waitingExecutions: 0,
      waitingWeight: 0,
    });

    releaseSecond();
    releaseThird();
  });

  it('uses bounded backfill when a heavy head waiter cannot fit yet', async () => {
    const gate = createBatchExecutionAdmissionGate(4);
    const releaseFirst = await gate.acquire(3);
    const admitted: string[] = [];
    const secondReleasePromise = gate.acquire(2).then((release) => {
      admitted.push('second');
      return release;
    });
    const thirdRelease = await gate.acquire(1).then((release) => {
      admitted.push('third');
      return release;
    });
    const fourthReleasePromise = gate.acquire(1).then((release) => {
      admitted.push('fourth');
      return release;
    });
    const fifthReleasePromise = gate.acquire(1).then((release) => {
      admitted.push('fifth');
      return release;
    });

    expect(admitted).toEqual(['third']);
    expect(gate.snapshot()).toEqual({
      activeExecutions: 2,
      activeWeight: 4,
      maxActiveExecutions: 4,
      maxActiveWeight: 4,
      waitingExecutions: 3,
      waitingWeight: 4,
    });

    thirdRelease();
    const fourthRelease = await fourthReleasePromise;
    expect(admitted).toEqual(['third', 'fourth']);
    expect(gate.snapshot()).toEqual({
      activeExecutions: 2,
      activeWeight: 4,
      maxActiveExecutions: 4,
      maxActiveWeight: 4,
      waitingExecutions: 2,
      waitingWeight: 3,
    });

    fourthRelease();
    await Promise.resolve();
    expect(admitted).toEqual(['third', 'fourth']);
    expect(gate.snapshot()).toEqual({
      activeExecutions: 1,
      activeWeight: 3,
      maxActiveExecutions: 4,
      maxActiveWeight: 4,
      waitingExecutions: 2,
      waitingWeight: 3,
    });

    releaseFirst();
    const releaseSecond = await secondReleasePromise;
    const releaseFifth = await fifthReleasePromise;
    expect(admitted).toEqual(['third', 'fourth', 'second', 'fifth']);

    releaseSecond();
    releaseFifth();
  });

  it('rejects weights that can never fit in the gate', async () => {
    const gate = createBatchExecutionAdmissionGate(2);

    await expect(gate.acquire(0)).rejects.toThrow('Batch execution admission weight must be a positive integer within the admission limit');
    await expect(gate.acquire(3)).rejects.toThrow('Batch execution admission weight must be a positive integer within the admission limit');
  });
});
