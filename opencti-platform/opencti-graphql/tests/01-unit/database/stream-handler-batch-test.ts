import { afterEach, describe, expect, it, vi } from 'vitest';
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
});
