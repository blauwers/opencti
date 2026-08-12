export type BatchTemporalCoordinatorConfig = {
  dwellMs: number;
  maxItems: number;
  maxBytes: number;
};

export type BatchTemporalCoordinatorEntry<T> = {
  key: string;
  dedupeKey?: string;
  itemCount: number;
  encodedBytes: number;
  barrier?: boolean;
  payload: T;
};

type QueuedBatchTemporalCoordinatorEntry<T, R> = {
  entry: BatchTemporalCoordinatorEntry<T>;
  reject: (reason?: unknown) => void;
  resolve: (value: R | PromiseLike<R>) => void;
};

type BatchTemporalCoordinatorBucket<T, R> = {
  encodedBytes: number;
  entries: Array<QueuedBatchTemporalCoordinatorEntry<T, R>>;
  itemCount: number;
  key: string;
  pendingResultsByDedupeKey: Map<string, Promise<R>>;
  timer?: NodeJS.Timeout;
};

const assertBatchTemporalCoordinatorResults = <R>(
  results: R[],
  expectedCount: number,
): void => {
  if (results.length !== expectedCount) {
    throw new Error(`Temporal batch executor returned ${results.length} results for ${expectedCount} entries`);
  }
};

export class BatchTemporalCoordinator<T, R> {
  private readonly activeFlushesByKey = new Map<string, Promise<void>>();

  private readonly pendingBucketsByKey = new Map<string, BatchTemporalCoordinatorBucket<T, R>>();

  constructor(
    private readonly config: BatchTemporalCoordinatorConfig,
    private readonly executeBatch: (payloads: T[]) => Promise<R[]>,
  ) {}

  enqueue(entry: BatchTemporalCoordinatorEntry<T>): Promise<R> {
    if (this.config.dwellMs <= 0) {
      return this.executeImmediate(entry);
    }
    if (entry.barrier || this.exceedsLimits(entry.itemCount, entry.encodedBytes)) {
      return this.flushThenExecuteImmediate(entry);
    }
    const existingBucket = this.pendingBucketsByKey.get(entry.key);
    const duplicateResult = entry.dedupeKey ? existingBucket?.pendingResultsByDedupeKey.get(entry.dedupeKey) : undefined;
    if (duplicateResult) {
      return duplicateResult;
    }
    if (
      existingBucket
      && this.exceedsLimits(
        existingBucket.itemCount + entry.itemCount,
        existingBucket.encodedBytes + entry.encodedBytes,
      )
    ) {
      void this.flushBucket(existingBucket);
    }
    const bucket = this.pendingBucketsByKey.get(entry.key) ?? this.createBucket(entry.key);
    let resolveEntry!: (value: R | PromiseLike<R>) => void;
    let rejectEntry!: (reason?: unknown) => void;
    const result = new Promise<R>((resolve, reject) => {
      resolveEntry = resolve;
      rejectEntry = reject;
    });
    bucket.entries.push({ entry, resolve: resolveEntry, reject: rejectEntry });
    if (entry.dedupeKey) {
      bucket.pendingResultsByDedupeKey.set(entry.dedupeKey, result);
    }
    bucket.itemCount += entry.itemCount;
    bucket.encodedBytes += entry.encodedBytes;
    if (this.reachesLimits(bucket.itemCount, bucket.encodedBytes)) {
      void this.flushBucket(bucket);
    }
    return result;
  }

  async flushPending(): Promise<void> {
    const pendingBuckets = Array.from(this.pendingBucketsByKey.values());
    await Promise.all(pendingBuckets.map((bucket) => this.flushBucket(bucket)));
    await Promise.all(Array.from(this.activeFlushesByKey.values()));
  }

  pendingBucketCount(): number {
    return this.pendingBucketsByKey.size;
  }

  private exceedsLimits(itemCount: number, encodedBytes: number): boolean {
    return itemCount > this.config.maxItems || encodedBytes > this.config.maxBytes;
  }

  private reachesLimits(itemCount: number, encodedBytes: number): boolean {
    return itemCount >= this.config.maxItems || encodedBytes >= this.config.maxBytes;
  }

  private createBucket(key: string): BatchTemporalCoordinatorBucket<T, R> {
    const bucket: BatchTemporalCoordinatorBucket<T, R> = {
      encodedBytes: 0,
      entries: [],
      itemCount: 0,
      key,
      pendingResultsByDedupeKey: new Map(),
    };
    bucket.timer = setTimeout(() => {
      void this.flushBucket(bucket);
    }, this.config.dwellMs);
    bucket.timer.unref();
    this.pendingBucketsByKey.set(key, bucket);
    return bucket;
  }

  private async executeImmediate(entry: BatchTemporalCoordinatorEntry<T>): Promise<R> {
    const results = await this.executeBatch([entry.payload]);
    assertBatchTemporalCoordinatorResults(results, 1);
    return results[0];
  }

  private async flushThenExecuteImmediate(entry: BatchTemporalCoordinatorEntry<T>): Promise<R> {
    const bucket = this.pendingBucketsByKey.get(entry.key);
    if (bucket) {
      await this.flushBucket(bucket);
    }
    return this.scheduleExecution(entry.key, [{
      entry,
      resolve: () => undefined,
      reject: () => undefined,
    }]).then((results) => {
      assertBatchTemporalCoordinatorResults(results, 1);
      return results[0];
    });
  }

  private async flushBucket(bucket: BatchTemporalCoordinatorBucket<T, R>): Promise<void> {
    if (this.pendingBucketsByKey.get(bucket.key) !== bucket) {
      return;
    }
    this.pendingBucketsByKey.delete(bucket.key);
    if (bucket.timer) {
      clearTimeout(bucket.timer);
    }
    if (bucket.entries.length === 0) {
      return;
    }
    try {
      const results = await this.scheduleExecution(bucket.key, bucket.entries);
      assertBatchTemporalCoordinatorResults(results, bucket.entries.length);
      bucket.entries.forEach((queuedEntry, index) => queuedEntry.resolve(results[index]));
    } catch (error) {
      bucket.entries.forEach((queuedEntry) => queuedEntry.reject(error));
    }
  }

  private scheduleExecution(
    key: string,
    queuedEntries: Array<QueuedBatchTemporalCoordinatorEntry<T, R>>,
  ): Promise<R[]> {
    const previousExecution = this.activeFlushesByKey.get(key) ?? Promise.resolve();
    const execution = previousExecution
      .catch(() => undefined)
      .then(() => this.executeBatch(queuedEntries.map((queuedEntry) => queuedEntry.entry.payload)));
    const trackedExecution = execution
      .then(() => undefined, () => undefined)
      .finally(() => {
        if (this.activeFlushesByKey.get(key) === trackedExecution) {
          this.activeFlushesByKey.delete(key);
        }
      });
    this.activeFlushesByKey.set(key, trackedExecution);
    return execution;
  }
}
