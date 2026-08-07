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

  it('keeps bounded active backfill flowing while a heavy head waiter is blocked', async () => {
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
    const releaseFifth = await fifthReleasePromise;
    expect(admitted).toEqual(['third', 'fourth', 'fifth']);
    expect(gate.snapshot()).toEqual({
      activeExecutions: 2,
      activeWeight: 4,
      maxActiveExecutions: 4,
      maxActiveWeight: 4,
      waitingExecutions: 1,
      waitingWeight: 2,
    });

    releaseFirst();
    const releaseSecond = await secondReleasePromise;
    expect(admitted).toEqual(['third', 'fourth', 'fifth', 'second']);
    expect(gate.snapshot()).toEqual({
      activeExecutions: 2,
      activeWeight: 3,
      maxActiveExecutions: 4,
      maxActiveWeight: 4,
      waitingExecutions: 0,
      waitingWeight: 0,
    });

    releaseSecond();
    releaseFifth();
  });

  it('counts existing bypassed work against later heavy waiters', async () => {
    const gate = createBatchExecutionAdmissionGate(4);
    const releaseFirst = await gate.acquire(3);
    const admitted: string[] = [];
    const secondReleasePromise = gate.acquire(2).then((release) => {
      admitted.push('second');
      return release;
    });
    const thirdReleasePromise = gate.acquire(3).then((release) => {
      admitted.push('third');
      return release;
    });
    const releaseFourth = await gate.acquire(1).then((release) => {
      admitted.push('fourth');
      return release;
    });
    const fifthReleasePromise = gate.acquire(1).then((release) => {
      admitted.push('fifth');
      return release;
    });

    expect(admitted).toEqual(['fourth']);

    releaseFirst();
    const releaseSecond = await secondReleasePromise;
    await Promise.resolve();
    expect(admitted).toEqual(['fourth', 'second']);
    expect(gate.snapshot()).toEqual({
      activeExecutions: 2,
      activeWeight: 3,
      maxActiveExecutions: 4,
      maxActiveWeight: 4,
      waitingExecutions: 2,
      waitingWeight: 4,
    });

    releaseSecond();
    const releaseThird = await thirdReleasePromise;
    await Promise.resolve();
    expect(admitted).toEqual(['fourth', 'second', 'third']);

    releaseFourth();
    const releaseFifth = await fifthReleasePromise;
    expect(admitted).toEqual(['fourth', 'second', 'third', 'fifth']);

    releaseThird();
    releaseFifth();
  });

  it('does not backfill behind a waiter that needs the full gate capacity', async () => {
    const gate = createBatchExecutionAdmissionGate(4);
    const releaseFirst = await gate.acquire(1);
    const admitted: string[] = [];
    const secondReleasePromise = gate.acquire(4).then((release) => {
      admitted.push('second');
      return release;
    });
    const thirdReleasePromise = gate.acquire(1).then((release) => {
      admitted.push('third');
      return release;
    });

    await Promise.resolve();
    expect(admitted).toEqual([]);

    releaseFirst();
    const releaseSecond = await secondReleasePromise;
    await Promise.resolve();
    expect(admitted).toEqual(['second']);

    releaseSecond();
    const releaseThird = await thirdReleasePromise;
    expect(admitted).toEqual(['second', 'third']);

    releaseThird();
  });

  it('rejects weights that can never fit in the gate', async () => {
    const gate = createBatchExecutionAdmissionGate(2);

    await expect(gate.acquire(0)).rejects.toThrow('Batch execution admission weight must be a positive integer within the admission limit');
    await expect(gate.acquire(3)).rejects.toThrow('Batch execution admission weight must be a positive integer within the admission limit');
  });
});
