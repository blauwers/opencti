export type BatchExecutionAdmissionRelease = () => void;

type BatchExecutionAdmissionWaiter = {
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
      while (waiters.length > 0) {
        const nextWaiter = waiters[0];
        if (activeWeight + nextWaiter.weight > maxActiveExecutions) {
          break;
        }
        waiters.shift();
        activeExecutions += 1;
        activeWeight += nextWaiter.weight;
        nextWaiter.resolve(buildRelease(nextWaiter.weight));
      }
    };
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
        waiters.push({ resolve, weight });
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
