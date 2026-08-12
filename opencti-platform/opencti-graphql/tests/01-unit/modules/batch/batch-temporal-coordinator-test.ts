import { afterEach, describe, expect, it, vi } from 'vitest';
import { BatchTemporalCoordinator } from '../../../../src/modules/batch/batch-temporal-coordinator';

describe('batch temporal coordinator', () => {
  afterEach(() => {
    vi.useRealTimers();
  });

  it('flushes compatible entries together after the dwell window', async () => {
    vi.useFakeTimers();
    const executeBatch = vi.fn(async (payloads: string[]) => payloads.map((payload) => `result:${payload}`));
    const coordinator = new BatchTemporalCoordinator({
      dwellMs: 1000,
      maxItems: 10,
      maxBytes: 1000,
    }, executeBatch);

    const first = coordinator.enqueue({ key: 'same', itemCount: 1, encodedBytes: 10, payload: 'one' });
    const second = coordinator.enqueue({ key: 'same', itemCount: 1, encodedBytes: 10, payload: 'two' });

    expect(executeBatch).not.toHaveBeenCalled();
    await vi.advanceTimersByTimeAsync(1000);

    await expect(Promise.all([first, second])).resolves.toEqual(['result:one', 'result:two']);
    expect(executeBatch).toHaveBeenCalledOnce();
    expect(executeBatch).toHaveBeenCalledWith(['one', 'two']);
  });

  it('shares one pending result for duplicate entries in the same bucket', async () => {
    vi.useFakeTimers();
    const executeBatch = vi.fn(async (payloads: string[]) => payloads.map((payload) => `result:${payload}`));
    const coordinator = new BatchTemporalCoordinator({
      dwellMs: 1000,
      maxItems: 10,
      maxBytes: 1000,
    }, executeBatch);

    const first = coordinator.enqueue({ key: 'same', dedupeKey: 'delivery--1', itemCount: 1, encodedBytes: 10, payload: 'one' });
    const duplicate = coordinator.enqueue({ key: 'same', dedupeKey: 'delivery--1', itemCount: 1, encodedBytes: 10, payload: 'duplicate' });

    await vi.advanceTimersByTimeAsync(1000);

    await expect(Promise.all([first, duplicate])).resolves.toEqual(['result:one', 'result:one']);
    expect(executeBatch).toHaveBeenCalledOnce();
    expect(executeBatch).toHaveBeenCalledWith(['one']);
  });

  it('flushes immediately when an item or byte bound is reached', async () => {
    vi.useFakeTimers();
    const executeBatch = vi.fn(async (payloads: string[]) => payloads);
    const itemCoordinator = new BatchTemporalCoordinator({
      dwellMs: 1000,
      maxItems: 2,
      maxBytes: 1000,
    }, executeBatch);

    const first = itemCoordinator.enqueue({ key: 'items', itemCount: 1, encodedBytes: 10, payload: 'one' });
    const second = itemCoordinator.enqueue({ key: 'items', itemCount: 1, encodedBytes: 10, payload: 'two' });

    await expect(Promise.all([first, second])).resolves.toEqual(['one', 'two']);
    expect(executeBatch).toHaveBeenNthCalledWith(1, ['one', 'two']);

    const byteCoordinator = new BatchTemporalCoordinator({
      dwellMs: 1000,
      maxItems: 10,
      maxBytes: 20,
    }, executeBatch);
    const third = byteCoordinator.enqueue({ key: 'bytes', itemCount: 1, encodedBytes: 10, payload: 'three' });
    const fourth = byteCoordinator.enqueue({ key: 'bytes', itemCount: 1, encodedBytes: 10, payload: 'four' });

    await expect(Promise.all([third, fourth])).resolves.toEqual(['three', 'four']);
    expect(executeBatch).toHaveBeenNthCalledWith(2, ['three', 'four']);
  });

  it('keeps incompatible keys in separate buckets and flushes them on shutdown', async () => {
    const executeBatch = vi.fn(async (payloads: string[]) => payloads.map((payload) => `done:${payload}`));
    const coordinator = new BatchTemporalCoordinator({
      dwellMs: 1000,
      maxItems: 10,
      maxBytes: 1000,
    }, executeBatch);

    const first = coordinator.enqueue({ key: 'one', itemCount: 1, encodedBytes: 10, payload: 'first' });
    const second = coordinator.enqueue({ key: 'two', itemCount: 1, encodedBytes: 10, payload: 'second' });

    expect(coordinator.pendingBucketCount()).toBe(2);
    await coordinator.flushPending();

    await expect(Promise.all([first, second])).resolves.toEqual(['done:first', 'done:second']);
    expect(executeBatch).toHaveBeenCalledTimes(2);
    expect(executeBatch).toHaveBeenCalledWith(['first']);
    expect(executeBatch).toHaveBeenCalledWith(['second']);
    expect(coordinator.pendingBucketCount()).toBe(0);
  });

  it('executes immediately when disabled and treats barriers as isolated flush points', async () => {
    const executeBatch = vi.fn(async (payloads: string[]) => payloads);
    const disabledCoordinator = new BatchTemporalCoordinator({
      dwellMs: 0,
      maxItems: 10,
      maxBytes: 1000,
    }, executeBatch);

    await expect(disabledCoordinator.enqueue({ key: 'same', itemCount: 1, encodedBytes: 10, payload: 'one' })).resolves.toBe('one');
    await expect(disabledCoordinator.enqueue({ key: 'same', itemCount: 1, encodedBytes: 10, payload: 'two' })).resolves.toBe('two');
    expect(executeBatch).toHaveBeenNthCalledWith(1, ['one']);
    expect(executeBatch).toHaveBeenNthCalledWith(2, ['two']);

    const barrierCoordinator = new BatchTemporalCoordinator({
      dwellMs: 1000,
      maxItems: 10,
      maxBytes: 1000,
    }, executeBatch);
    const pending = barrierCoordinator.enqueue({ key: 'barrier', itemCount: 1, encodedBytes: 10, payload: 'pending' });
    const barrier = barrierCoordinator.enqueue({ key: 'barrier', itemCount: 1, encodedBytes: 10, payload: 'barrier', barrier: true });

    await expect(Promise.all([pending, barrier])).resolves.toEqual(['pending', 'barrier']);
    expect(executeBatch).toHaveBeenNthCalledWith(3, ['pending']);
    expect(executeBatch).toHaveBeenNthCalledWith(4, ['barrier']);
  });

  it('rejects every original entry when a shared execution fails', async () => {
    const executeBatch = vi.fn(async () => {
      throw new Error('shared failure');
    });
    const coordinator = new BatchTemporalCoordinator({
      dwellMs: 1000,
      maxItems: 2,
      maxBytes: 1000,
    }, executeBatch);

    const first = coordinator.enqueue({ key: 'same', itemCount: 1, encodedBytes: 10, payload: 'one' });
    const second = coordinator.enqueue({ key: 'same', itemCount: 1, encodedBytes: 10, payload: 'two' });

    await expect(Promise.all([first, second])).rejects.toThrow('shared failure');
    expect(executeBatch).toHaveBeenCalledWith(['one', 'two']);
  });
});
