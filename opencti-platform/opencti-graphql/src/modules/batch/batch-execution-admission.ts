export type BatchExecutionAdmissionRelease = () => void;

type BatchExecutionAdmissionWaiter = {
  resolve: (release: BatchExecutionAdmissionRelease) => void;
  sequence: number;
  weight: number;
};

export interface BatchExecutionAdmissionSnapshot {
  activeExecutions: number;
  activeWeight: number;
  maxActiveExecutions: number;
  maxActiveWeight: number;
  waitingExecutions: number;
  waitingWeight: number;
}

export interface BatchExecutionAdmissionGate {
  acquire: (weight?: number) => Promise<BatchExecutionAdmissionRelease>;
  snapshot: () => BatchExecutionAdmissionSnapshot;
}

export const createBatchExecutionAdmissionGate = (maxActiveExecutions: number): BatchExecutionAdmissionGate => {
  if (!Number.isInteger(maxActiveExecutions) || maxActiveExecutions < 1) {
    throw new Error('Batch execution admission limit must be a positive integer');
  }

  let activeExecutions = 0;
  let activeWeight = 0;
  let nextSequence = 0;
  const activeWeightsBySequence = new Map<number, number>();
  const waiters: BatchExecutionAdmissionWaiter[] = [];

  const validateWeight = (weight: number): void => {
    if (!Number.isInteger(weight) || weight < 1 || weight > maxActiveExecutions) {
      throw new Error('Batch execution admission weight must be a positive integer within the admission limit');
    }
  };

  const buildRelease = (sequence: number, weight: number): BatchExecutionAdmissionRelease => {
    let released = false;
    return () => {
      if (released) {
        return;
      }
      released = true;
      activeExecutions -= 1;
      activeWeight -= weight;
      activeWeightsBySequence.delete(sequence);
      drainWaiters();
    };
  };

  const admitExecution = (sequence: number, weight: number): BatchExecutionAdmissionRelease => {
    activeExecutions += 1;
    activeWeight += weight;
    activeWeightsBySequence.set(sequence, weight);
    return buildRelease(sequence, weight);
  };

  const admitWaiter = (waiterIndex: number): void => {
    const [waiter] = waiters.splice(waiterIndex, 1);
    waiter.resolve(admitExecution(waiter.sequence, waiter.weight));
  };

  const getActiveBypassWeight = (headSequence: number): number => {
    let activeBypassWeight = 0;
    for (const [sequence, weight] of activeWeightsBySequence.entries()) {
      if (sequence > headSequence) {
        activeBypassWeight += weight;
      }
    }
    return activeBypassWeight;
  };

  const findNextWaiterIndex = (): number => {
    const availableWeight = maxActiveExecutions - activeWeight;
    if (availableWeight < 1 || waiters.length === 0) {
      return -1;
    }
    const headWaiter = waiters[0];
    if (headWaiter.weight <= availableWeight) {
      return 0;
    }

    // A blocked head may allow younger work through only while enough capacity
    // stays reserved for it once all older active work finishes. Only active
    // bypassed work consumes that reserve, so short requests can keep flowing
    // without starving the oldest heavy batch.
    const remainingBypassWeight = maxActiveExecutions - headWaiter.weight - getActiveBypassWeight(headWaiter.sequence);
    if (remainingBypassWeight < 1) {
      return -1;
    }
    return waiters.findIndex((waiter, waiterIndex) => (
      waiterIndex > 0
      && waiter.weight <= availableWeight
      && waiter.weight <= remainingBypassWeight
    ));
  };

  const drainWaiters = (): void => {
    while (waiters.length > 0) {
      const waiterIndex = findNextWaiterIndex();
      if (waiterIndex < 0) {
        return;
      }
      admitWaiter(waiterIndex);
    }
  };

  return {
    acquire: async (weight = 1) => {
      validateWeight(weight);
      nextSequence += 1;
      const sequence = nextSequence;
      if (waiters.length === 0 && activeWeight + weight <= maxActiveExecutions) {
        return admitExecution(sequence, weight);
      }
      return new Promise<BatchExecutionAdmissionRelease>((resolve) => {
        waiters.push({ resolve, sequence, weight });
        drainWaiters();
      });
    },
    snapshot: () => ({
      activeExecutions,
      activeWeight,
      maxActiveExecutions,
      maxActiveWeight: maxActiveExecutions,
      waitingExecutions: waiters.length,
      waitingWeight: waiters.reduce((total, waiter) => total + waiter.weight, 0),
    }),
  };
};
