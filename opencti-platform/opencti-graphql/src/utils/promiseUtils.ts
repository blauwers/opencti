/**
 * Execute async operations with concurrency control.
 * Modern native alternative to Bluebird's Promise.map with concurrency.
 *
 * @param items - Array of items to process
 * @param mapper - Async function to apply to each item
 * @param concurrency - Maximum number of concurrent operations
 * @returns Promise resolving to array of results
 *
 * @example
 * const results = await promiseMap(
 *   files,
 *   (file) => processFile(file),
 *   5 // process 5 files at a time
 * );
 */
export const promiseMap = async <T, R>(
  items: T[],
  mapper: (item: T, index: number) => Promise<R>,
  concurrency: number,
): Promise<R[]> => {
  if (!Number.isInteger(concurrency) || concurrency < 1) {
    throw new Error('Promise map concurrency must be a positive integer');
  }

  const results = new Array<R>(items.length);
  let nextIndex = 0;
  let failed = false;
  const worker = async () => {
    while (!failed) {
      const index = nextIndex;
      if (index >= items.length) {
        return;
      }
      nextIndex += 1;
      try {
        results[index] = await mapper(items[index], index);
      } catch (cause) {
        failed = true;
        throw cause;
      }
    }
  };

  const workerCount = Math.min(concurrency, items.length);
  await Promise.all(Array.from({ length: workerCount }, () => worker()));
  return results;
};

/**
 * Execute async operations with concurrency control while keeping ownership of
 * already-started mapper calls until they settle after the first failure.
 *
 * New items stop being scheduled after the first observed rejection, but the
 * returned promise does not reject until every mapper call that already
 * started has settled.
 */
export const promiseMapWaitForStarted = async <T, R>(
  items: T[],
  mapper: (item: T, index: number) => Promise<R>,
  concurrency: number,
): Promise<R[]> => {
  if (!Number.isInteger(concurrency) || concurrency < 1) {
    throw new Error('Promise map concurrency must be a positive integer');
  }

  const results = new Array<R>(items.length);
  let nextIndex = 0;
  let firstFailure: unknown;
  let hasFailure = false;
  const worker = async () => {
    while (!hasFailure) {
      const index = nextIndex;
      if (index >= items.length) {
        return;
      }
      nextIndex += 1;
      try {
        results[index] = await mapper(items[index], index);
      } catch (cause) {
        if (!hasFailure) {
          hasFailure = true;
          firstFailure = cause;
        }
      }
    }
  };

  const workerCount = Math.min(concurrency, items.length);
  await Promise.all(Array.from({ length: workerCount }, () => worker()));
  if (hasFailure) {
    throw firstFailure;
  }
  return results;
};

export const forgetPromise = (promise: Promise<unknown>) => {
  promise.catch(() => {}); // avoid unhandled promise rejection
};

export class TimeoutError extends Error {
  name = 'TimeoutError';
}

export const callWithTimeout = async <T>(promise: Promise<T>, timeout: number): Promise<T> => {
  let timeoutHandle: NodeJS.Timeout | undefined;
  const timeoutPromise = new Promise((_, reject) => {
    timeoutHandle = setTimeout(() => reject(new TimeoutError('Operation timed out.')), timeout);
  });
  try {
    const result = await Promise.race([promise, timeoutPromise]);
    return result as T; // only 'promise' can resolve
  } finally {
    clearTimeout(timeoutHandle!);
  }
};
