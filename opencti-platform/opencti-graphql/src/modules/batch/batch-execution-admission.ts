export type BatchExecutionAdmissionRelease = () => void;

type BatchExecutionAdmissionWaiter = {
  bypassedWeight: number;
  resolve: (release: BatchExecutionAdmissionRelease) => void;
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
  const waiters: BatchExecutionAdmissionWaiter[] = [];

  const validateWeight = (weight: number): void => {
    if (!Number.isInteger(weight) || weight < 1 || weight > maxActiveExecutions) {
      throw new Error('Batch execution admission weight must be a positive integer within the admission limit');
    }
  };

  const buildRelease = (weight: number): BatchExecutionAdmissionRelease => {
    let released = false;
    return () => {
      if (released) {
        return;
      }
      released = true;
      activeExecutions -= 1;
      activeWeight -= weight;
      drainWaiters();
    };
  };

  const admitWaiter = (waiterIndex: number): void => {
    const [waiter] = waiters.splice(waiterIndex, 1);
    activeExecutions += 1;
    activeWeight += waiter.weight;
    waiter.resolve(buildRelease(waiter.weight));
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

    // A blocked heavy head may allow a bounded amount of smaller work through
    // so the gate does not leave capacity idle indefinitely. The budget stays
    // attached to the head waiter, which prevents continuous small requests
    // from starving the oldest heavy batch.
    const remainingBypassWeight = headWaiter.weight - headWaiter.bypassedWeight;
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
      if (waiterIndex > 0) {
        waiters[0].bypassedWeight += waiters[waiterIndex].weight;
      }
      admitWaiter(waiterIndex);
    }
  };

  return {
    acquire: async (weight = 1) => {
      validateWeight(weight);
      if (waiters.length === 0 && activeWeight + weight <= maxActiveExecutions) {
        activeExecutions += 1;
        activeWeight += weight;
        return buildRelease(weight);
      }
      return new Promise<BatchExecutionAdmissionRelease>((resolve) => {
        waiters.push({ bypassedWeight: 0, resolve, weight });
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
