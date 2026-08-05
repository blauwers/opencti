export type BatchExecutionAdmissionRelease = () => void;

type BatchExecutionAdmissionWaiter = (release: BatchExecutionAdmissionRelease) => void;

export interface BatchExecutionAdmissionSnapshot {
  activeExecutions: number;
  maxActiveExecutions: number;
  waitingExecutions: number;
}

export interface BatchExecutionAdmissionGate {
  acquire: () => Promise<BatchExecutionAdmissionRelease>;
  snapshot: () => BatchExecutionAdmissionSnapshot;
}

export const createBatchExecutionAdmissionGate = (maxActiveExecutions: number): BatchExecutionAdmissionGate => {
  if (!Number.isInteger(maxActiveExecutions) || maxActiveExecutions < 1) {
    throw new Error('Batch execution admission limit must be a positive integer');
  }

  let activeExecutions = 0;
  const waiters: BatchExecutionAdmissionWaiter[] = [];

  const buildRelease = (): BatchExecutionAdmissionRelease => {
    let released = false;
    return () => {
      if (released) {
        return;
      }
      released = true;
      const nextWaiter = waiters.shift();
      if (nextWaiter) {
        nextWaiter(buildRelease());
        return;
      }
      activeExecutions -= 1;
    };
  };

  return {
    acquire: async () => {
      if (activeExecutions < maxActiveExecutions) {
        activeExecutions += 1;
        return buildRelease();
      }
      return new Promise<BatchExecutionAdmissionRelease>((resolve) => {
        waiters.push(resolve);
      });
    },
    snapshot: () => ({
      activeExecutions,
      maxActiveExecutions,
      waitingExecutions: waiters.length,
    }),
  };
};
