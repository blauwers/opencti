import { afterEach, describe, expect, it, vi } from 'vitest';
import * as jsonpatch from 'fast-json-patch';
import {
  BATCH_SIDE_EFFECT_SEAL_DESCRIPTORS,
  BatchMutationKind,
  BatchSideEffectKind,
  evaluateBatchSideEffectSeal,
  executeBatchMutations,
} from '../../../src/modules/batch/batch-executor';
import { STIX_EXT_OCTI } from '../../../src/types/stix-2-1-extensions';

const { mockRawAppendOrReturnLiveStreamPublicationProof, mockRawPushToStream } = vi.hoisted(() => ({
  mockRawAppendOrReturnLiveStreamPublicationProof: vi.fn(),
  mockRawPushToStream: vi.fn(),
}));

vi.mock('../../../src/database/redis-stream', () => ({
  rawAppendOrReturnLiveStreamPublicationProof: mockRawAppendOrReturnLiveStreamPublicationProof,
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
    expect(mockRawAppendOrReturnLiveStreamPublicationProof).not.toHaveBeenCalled();
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
    let sealSnapshot: ReturnType<typeof evaluateBatchSideEffectSeal> = undefined;

    const execution = await executeBatchMutations([
      {
        kind: BatchMutationKind.CreateEntity,
        executeWrite: async () => {
          await publishStixToStream(context, user, event);
          expect(mockRawPushToStream).not.toHaveBeenCalled();
          return null;
        },
      },
    ], {
      onSideEffectSealEvaluated: (snapshot) => {
        sealSnapshot = snapshot;
      },
    });

    expect(execution.sideEffectKinds).toContain(BatchSideEffectKind.StreamPublication);
    expect(sealSnapshot).toEqual([BATCH_SIDE_EFFECT_SEAL_DESCRIPTORS.streamPublicationRaw]);
    expect(mockRawPushToStream).toHaveBeenCalledWith({
      ...event,
      event_id: context.eventId,
    });
    expect(mockRawAppendOrReturnLiveStreamPublicationProof).not.toHaveBeenCalled();
  });

  it('coalesces create and update publications for the same object inside a batch', async () => {
    let sealSnapshot: ReturnType<typeof evaluateBatchSideEffectSeal> = undefined;
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
    ], {
      onSideEffectSealEvaluated: (snapshot) => {
        sealSnapshot = snapshot;
      },
    });

    expect(sealSnapshot).toEqual([BATCH_SIDE_EFFECT_SEAL_DESCRIPTORS.streamPublicationKeyedCoalesced]);
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

  it('drops buffered updates that only leave freshness fields changed', async () => {
    const original = {
      extensions: {
        [STIX_EXT_OCTI]: {
          modified_at: '2026-08-01T00:00:00.000Z',
          updated_at: '2026-08-01T00:00:00.000Z',
        },
      },
      id: 'indicator--1',
      modified: '2026-08-01T00:00:00.000Z',
      name: 'original',
    };
    const changed = {
      ...original,
      extensions: {
        [STIX_EXT_OCTI]: {
          ...original.extensions[STIX_EXT_OCTI],
          updated_at: '2026-08-02T00:00:00.000Z',
        },
      },
      modified: '2026-08-02T00:00:00.000Z',
      name: 'changed',
    };
    const restored = {
      ...original,
      extensions: {
        [STIX_EXT_OCTI]: {
          ...original.extensions[STIX_EXT_OCTI],
          updated_at: '2026-08-03T00:00:00.000Z',
        },
      },
      modified: '2026-08-03T00:00:00.000Z',
    };
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
        patch: jsonpatch.compare(changed, restored),
        reverse_patch: jsonpatch.compare(restored, changed),
      },
      data: restored,
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
