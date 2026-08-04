import { afterEach, describe, expect, it, vi } from 'vitest';
import * as jsonpatch from 'fast-json-patch';
import { BatchMutationKind, BatchSideEffectKind, executeBatchMutations } from '../../../src/modules/batch/batch-executor';

const { mockRawPushToStream } = vi.hoisted(() => ({
  mockRawPushToStream: vi.fn(),
}));

vi.mock('../../../src/database/redis-stream', () => ({
  rawRedisStreamClient: {
    rawPushToStream: mockRawPushToStream,
  },
}));

vi.mock('../../../src/config/tracing', () => ({
  telemetry: async (_context: unknown, _user: unknown, _operation: string, _attributes: unknown, execute: () => Promise<void>) => execute(),
}));

import { publishStixToStream } from '../../../src/database/stream/stream-handler';

describe('stream handler batch publication', () => {
  const context = { eventId: 'batch-event-id' } as any;
  const user = {} as any;
  const event = { type: 'create', scope: 'external' } as any;

  afterEach(() => {
    vi.clearAllMocks();
  });

  it('publishes immediately outside a batch scope', async () => {
    await publishStixToStream(context, user, event);

    expect(mockRawPushToStream).toHaveBeenCalledWith({
      ...event,
      event_id: context.eventId,
    });
  });

  it('does not publish stream events from an aborted batch', async () => {
    await expect(executeBatchMutations([
      {
        kind: BatchMutationKind.CreateEntity,
        executeWrite: async () => {
          await publishStixToStream(context, user, event);
          expect(mockRawPushToStream).not.toHaveBeenCalled();
          throw new Error('abort stream batch');
        },
      },
    ])).rejects.toThrow('abort stream batch');

    expect(mockRawPushToStream).not.toHaveBeenCalled();
  });

  it('publishes stream events after batch commit', async () => {
    const execution = await executeBatchMutations([
      {
        kind: BatchMutationKind.CreateEntity,
        executeWrite: async () => {
          await publishStixToStream(context, user, event);
          expect(mockRawPushToStream).not.toHaveBeenCalled();
          return null;
        },
      },
    ]);

    expect(execution.sideEffectKinds).toContain(BatchSideEffectKind.StreamPublication);
    expect(mockRawPushToStream).toHaveBeenCalledWith({
      ...event,
      event_id: context.eventId,
    });
  });

  it('coalesces create and update publications for the same object inside a batch', async () => {
    const created = {
      data: { id: 'indicator--1', name: 'created' },
      message: 'Create indicator',
      origin: {},
      scope: 'external',
      type: 'create',
      version: '4',
    } as any;
    const updated = {
      commit: undefined,
      context: {
        changes: [{ field: 'name' }],
        patch: [{ op: 'replace', path: '/name', value: 'updated' }],
        reverse_patch: [{ op: 'replace', path: '/name', value: 'created' }],
      },
      data: { id: 'indicator--1', name: 'updated' },
      message: 'Update 1 elements',
      origin: {},
      scope: 'external',
      type: 'update',
      version: '4',
    } as any;

    await executeBatchMutations([
      {
        kind: BatchMutationKind.CreateEntity,
        executeWrite: async () => {
          await publishStixToStream(context, user, created);
          await publishStixToStream(context, user, updated);
          return null;
        },
      },
    ]);

    expect(mockRawPushToStream).toHaveBeenCalledTimes(1);
    expect(mockRawPushToStream).toHaveBeenCalledWith({
      ...created,
      data: updated.data,
      event_id: context.eventId,
    });
  });

  it('drops buffered updates that cancel each other before publication', async () => {
    const original = { id: 'indicator--1', name: 'original' };
    const changed = { id: 'indicator--1', name: 'changed' };
    const firstUpdate = {
      commit: undefined,
      context: {
        changes: [{ field: 'name' }],
        patch: jsonpatch.compare(original, changed),
        reverse_patch: jsonpatch.compare(changed, original),
      },
      data: changed,
      message: 'Update 1 elements',
      origin: {},
      scope: 'external',
      type: 'update',
      version: '4',
    } as any;
    const secondUpdate = {
      commit: undefined,
      context: {
        changes: [{ field: 'name' }],
        patch: jsonpatch.compare(changed, original),
        reverse_patch: jsonpatch.compare(original, changed),
      },
      data: original,
      message: 'Update 1 elements',
      origin: {},
      scope: 'external',
      type: 'update',
      version: '4',
    } as any;

    await executeBatchMutations([
      {
        kind: BatchMutationKind.UpdateAttribute,
        executeWrite: async () => {
          await publishStixToStream(context, user, firstUpdate);
          await publishStixToStream(context, user, secondUpdate);
          return null;
        },
      },
    ]);

    expect(mockRawPushToStream).not.toHaveBeenCalled();
  });
});
