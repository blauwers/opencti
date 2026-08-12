import { describe, expect, it } from 'vitest';
import { waitForAllIngestionPromises } from '../../../../src/manager/ingestionManager';

const deferred = <T>() => {
  let resolve: (value: T) => void = () => {};
  let reject: (reason?: unknown) => void = () => {};
  const promise = new Promise<T>((promiseResolve, promiseReject) => {
    resolve = promiseResolve;
    reject = promiseReject;
  });
  return { promise, reject, resolve };
};

describe('ingestion manager promise settlement', () => {
  it('waits for sibling work before surfacing a rejection', async () => {
    const sibling = deferred<string>();
    const failure = deferred<never>();
    const error = new Error('json ingestion failed');
    let rejected = false;

    const waiting = waitForAllIngestionPromises([
      failure.promise,
      sibling.promise,
    ]).catch((reason) => {
      rejected = true;
      throw reason;
    });

    failure.reject(error);
    await Promise.resolve();
    await Promise.resolve();
    expect(rejected).toBe(false);

    sibling.resolve('done');
    await expect(waiting).rejects.toBe(error);
  });

  it('preserves fulfilled results in input order', async () => {
    await expect(waitForAllIngestionPromises([
      Promise.resolve('rss'),
      Promise.resolve('taxii'),
      Promise.resolve('csv'),
    ])).resolves.toEqual(['rss', 'taxii', 'csv']);
  });

  it('surfaces the first observed rejection after all siblings settle', async () => {
    const first = deferred<never>();
    const second = deferred<never>();
    const firstError = new Error('first array entry failed later');
    const secondError = new Error('second array entry failed first');

    const waiting = waitForAllIngestionPromises([first.promise, second.promise]);
    second.reject(secondError);
    await Promise.resolve();
    first.reject(firstError);

    await expect(waiting).rejects.toBe(secondError);
  });
});
