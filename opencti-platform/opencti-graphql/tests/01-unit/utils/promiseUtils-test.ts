import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';
import { callWithTimeout, promiseMap, TimeoutError } from '../../../src/utils/promiseUtils';

describe('Promise utilities: promiseMap', () => {
  it('fills available concurrency slots as soon as an item settles while preserving result order', async () => {
    const started: number[] = [];
    const resolveByIndex: Array<((value: string) => void) | undefined> = [];
    const values = [0, 1, 2, 3];

    const promise = promiseMap(values, async (value, index) => {
      started.push(value);
      return new Promise<string>((resolve) => {
        resolveByIndex[index] = resolve;
      });
    }, 2);

    await Promise.resolve();
    expect(started).toEqual([0, 1]);

    resolveByIndex[1]?.('one');
    await Promise.resolve();
    await Promise.resolve();
    expect(started).toEqual([0, 1, 2]);

    resolveByIndex[0]?.('zero');
    await Promise.resolve();
    await Promise.resolve();
    expect(started).toEqual([0, 1, 2, 3]);

    resolveByIndex[2]?.('two');
    resolveByIndex[3]?.('three');

    await expect(promise).resolves.toEqual(['zero', 'one', 'two', 'three']);
  });

  it('rejects invalid concurrency values', async () => {
    await expect(promiseMap([], async () => 'unused', 0)).rejects.toThrow('Promise map concurrency must be a positive integer');
  });
});

describe('Promise utilities: callWithTimeout', () => {
  beforeEach(() => {
    vi.useFakeTimers();
  });

  afterEach(() => {
    vi.useRealTimers();
  });

  it('should resolve when the wrapped promise settles before timeout', async () => {
    const wrappedPromise = new Promise<string>((resolve) => {
      setTimeout(() => resolve('ok'), 50);
    });

    const promise = callWithTimeout(wrappedPromise, 500);

    await vi.advanceTimersByTimeAsync(50);

    await expect(promise).resolves.toBe('ok');
  });

  it('should reject with TimeoutError when timeout is reached first', async () => {
    const neverResolvingPromise = new Promise<string>(() => {});
    const promise = callWithTimeout(neverResolvingPromise, 100);
    const promiseInstanceOfAssertion = expect(promise).rejects.toBeInstanceOf(TimeoutError);
    const promiseMessageAssertion = expect(promise).rejects.toMatchObject({ message: 'Operation timed out.' });
    await vi.advanceTimersByTimeAsync(100);
    await promiseInstanceOfAssertion;
    await promiseMessageAssertion;
  });

  it('should propagate wrapped promise rejection when it rejects before timeout', async () => {
    const wrappedError = new Error('wrapped failure');
    const wrappedPromise = new Promise<string>((_resolve, reject) => {
      setTimeout(() => reject(wrappedError), 25);
    });
    const promise = callWithTimeout(wrappedPromise, 500);
    const promiseAssertion = expect(promise).rejects.toBe(wrappedError);
    await vi.advanceTimersByTimeAsync(25);
    await promiseAssertion;
  });
});
