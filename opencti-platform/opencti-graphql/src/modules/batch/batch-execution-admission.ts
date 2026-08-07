export interface BatchExecutionAdmissionRelease {
  (): void;
  downgrade: (weight: number) => void;
}

type BatchExecutionAdmissionWaiter = {
  resolve: (release: BatchExecutionAdmissionRelease) => void;
  sequence: number;
  weight: number;
};

type BatchExecutionAdmissionReservation = {
  currentWeight: number;
  initialWeight: number;
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
  const activeReservationsBySequence = new Map<number, BatchExecutionAdmissionReservation>();
  const waiters: BatchExecutionAdmissionWaiter[] = [];

  const validateWeight = (weight: number): void => {
    if (!Number.isInteger(weight) || weight < 1 || weight > maxActiveExecutions) {
      throw new Error('Batch execution admission weight must be a positive integer within the admission limit');
    }
  };

  const buildRelease = (sequence: number, initialWeight: number): BatchExecutionAdmissionRelease => {
    let released = false;
    let weight = initialWeight;
    const release = (() => {
      if (released) {
        return;
      }
      released = true;
      activeExecutions -= 1;
      activeWeight -= weight;
      activeReservationsBySequence.delete(sequence);
      drainWaiters();
    }) as BatchExecutionAdmissionRelease;
    release.downgrade = (nextWeight: number) => {
      validateWeight(nextWeight);
      if (released || nextWeight === weight) {
        return;
      }
      if (nextWeight > weight) {
        throw new Error('Batch execution admission weight can only be downgraded');
      }
      activeWeight -= weight - nextWeight;
      weight = nextWeight;
      const reservation = activeReservationsBySequence.get(sequence);
      if (reservation) {
        reservation.currentWeight = weight;
      }
      drainWaiters();
    };
    return release;
  };

  const admitExecution = (sequence: number, weight: number): BatchExecutionAdmissionRelease => {
    activeExecutions += 1;
    activeWeight += weight;
    activeReservationsBySequence.set(sequence, {
      currentWeight: weight,
      initialWeight: weight,
    });
    return buildRelease(sequence, weight);
  };

  const admitWaiter = (waiterIndex: number): void => {
    const [waiter] = waiters.splice(waiterIndex, 1);
    waiter.resolve(admitExecution(waiter.sequence, waiter.weight));
  };

  const getActiveBypassWeight = (headSequence: number): number => {
    let activeBypassWeight = 0;
    for (const [sequence, reservation] of activeReservationsBySequence.entries()) {
      if (sequence > headSequence) {
        activeBypassWeight += reservation.currentWeight;
      }
    }
    return activeBypassWeight;
  };

  const getOlderReleasedWeight = (headSequence: number): number => {
    let olderReleasedWeight = 0;
    for (const [sequence, reservation] of activeReservationsBySequence.entries()) {
      if (sequence < headSequence) {
        olderReleasedWeight += reservation.initialWeight - reservation.currentWeight;
      }
    }
    return olderReleasedWeight;
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

    // A blocked head may allow younger work through only when it would have
    // fit beside the head after older work finishes, plus capacity explicitly
    // released by older downgraded executions. This keeps the materialization
    // lane flowing without letting ordinary idle capacity delay a full head.
    const remainingBypassWeight = maxActiveExecutions
      - headWaiter.weight
      + getOlderReleasedWeight(headWaiter.sequence)
      - getActiveBypassWeight(headWaiter.sequence);
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
