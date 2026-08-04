import { AsyncLocalStorage } from 'node:async_hooks';
import { internalFindByIds } from '../../database/middleware-loader';
import { isNotEmptyField } from '../../database/utils';
import { lockResources } from '../../lock/master-lock';
import { getInstanceIds } from '../../schema/identifier';
import type { BasicStoreObject } from '../../types/store';
import type { AuthContext } from '../../types/user';
import { SYSTEM_USER } from '../../utils/access';

type BatchEntityCreateGroupState = 'collecting' | 'waiting' | 'ready' | 'active' | 'completed';

type BatchEntityCreateLock = {
  signal: AbortSignal;
  unlock: () => Promise<void>;
};

export type BatchEntityCreateLookupInput = {
  draftId?: string;
  finderIds: string[];
  participantIds: string[];
  type: string;
};

export type BatchEntityCreateLookupResolution = {
  existingByIds: BasicStoreObject[];
  lock: BatchEntityCreateLock;
};

export type BatchParticipantLockInput = {
  draftId?: string;
  participantIds: string[];
};

type PendingBatchEntityCreateLookup = {
  groupId: number;
  input: BatchEntityCreateLookupInput;
  reject: (cause: unknown) => void;
  resolve: (resolution: BatchEntityCreateLookupResolution) => void;
};

type ReadyBatchEntityCreateLookup = {
  lookup: PendingBatchEntityCreateLookup;
  participantIds: string[];
  resolution: BatchEntityCreateLookupResolution;
};

type BatchEntityCreateCoordinatorScope = {
  coordinator: BatchEntityCreateCoordinator;
  groupId: number;
};

const MAX_DIRECT_ID_LOOKUP_SIZE = 5000;
const batchEntityCreateCoordinatorStorage = new AsyncLocalStorage<BatchEntityCreateCoordinatorScope>();

const normalizeIds = (ids: string[]): string[] => Array.from(new Set(ids.filter((id) => isNotEmptyField(id))));

const chunkIds = (ids: string[]): string[][] => {
  const chunks: string[][] = [];
  for (let index = 0; index < ids.length; index += MAX_DIRECT_ID_LOOKUP_SIZE) {
    chunks.push(ids.slice(index, index + MAX_DIRECT_ID_LOOKUP_SIZE));
  }
  return chunks;
};

export class BatchEntityCreateCoordinator {
  private readonly context: AuthContext;

  private readonly groupOrder: number[];

  private readonly maxActiveGroups: number;

  private readonly groupStates = new Map<number, BatchEntityCreateGroupState>();

  private readonly heldLocks: BatchEntityCreateLock[] = [];

  private readonly heldParticipantIdsByDraft = new Map<string, Set<string>>();

  private readonly ownedParticipantIdsByGroup = new Map<number, Map<string, Set<string>>>();

  private readonly reservedParticipantIdsByGroup = new Map<number, Map<string, Set<string>>>();

  private readonly pendingLookups = new Map<number, PendingBatchEntityCreateLookup>();

  private readonly readyLookups: ReadyBatchEntityCreateLookup[] = [];

  private readonly sharedAbortController = new AbortController();

  private activeGroupCount = 0;

  private flushPromise: Promise<void> | undefined;

  private closed = false;

  constructor(context: AuthContext, groupIds: number[], maxActiveGroups = Number.POSITIVE_INFINITY) {
    this.context = context;
    this.groupOrder = [...groupIds];
    this.maxActiveGroups = Math.max(1, maxActiveGroups);
    this.groupOrder.forEach((groupId) => this.groupStates.set(groupId, 'collecting'));
  }

  getHeldParticipantIds(groupId: number, draftId?: string): string[] {
    return Array.from(this.ownedParticipantIdsByGroup.get(groupId)?.get(draftId ?? '') ?? []);
  }

  registerLookup(groupId: number, input: BatchEntityCreateLookupInput): Promise<BatchEntityCreateLookupResolution> {
    if (this.closed) {
      return Promise.reject(new Error('Batch entity create coordinator is closed'));
    }
    if (this.pendingLookups.has(groupId)) {
      return Promise.reject(new Error(`Batch entity create group ${groupId} already has a pending lookup`));
    }
    if (this.groupStates.get(groupId) === 'active') {
      this.activeGroupCount -= 1;
      this.reservedParticipantIdsByGroup.delete(groupId);
    }
    return new Promise<BatchEntityCreateLookupResolution>((resolve, reject) => {
      this.pendingLookups.set(groupId, {
        groupId,
        input: {
          ...input,
          finderIds: normalizeIds(input.finderIds),
          participantIds: normalizeIds(input.participantIds),
        },
        reject,
        resolve,
      });
      this.groupStates.set(groupId, 'waiting');
      this.releaseReadyLookups();
      this.scheduleFlushIfReady();
    });
  }

  completeGroup(groupId: number): void {
    if (this.groupStates.get(groupId) === 'active') {
      this.activeGroupCount -= 1;
    }
    this.groupStates.set(groupId, 'completed');
    this.ownedParticipantIdsByGroup.delete(groupId);
    this.reservedParticipantIdsByGroup.delete(groupId);
    this.releaseReadyLookups();
    this.scheduleFlushIfReady();
  }

  async close(): Promise<void> {
    this.closed = true;
    await this.flushPromise?.catch(() => undefined);
    const pending = Array.from(this.pendingLookups.values());
    this.pendingLookups.clear();
    pending.forEach((lookup) => lookup.reject(new Error('Batch entity create phase ended before lookup resolution')));
    const ready = this.readyLookups.splice(0);
    ready.forEach(({ lookup }) => lookup.reject(new Error('Batch entity create phase ended before lookup resolution')));
    await Promise.allSettled(this.heldLocks.reverse().map((lock) => lock.unlock()));
  }

  private scheduleFlushIfReady(): void {
    if (this.flushPromise || this.closed || this.pendingLookups.size === 0 || !this.isReadyToFlush()) {
      return;
    }
    this.flushPromise = this.flushReadyLookups()
      .finally(() => {
        this.flushPromise = undefined;
        this.scheduleFlushIfReady();
      });
  }

  private isReadyToFlush(): boolean {
    return this.groupOrder.every((groupId) => {
      const state = this.groupStates.get(groupId);
      return state === 'waiting' || state === 'completed';
    });
  }

  private selectDisjointLookups(): PendingBatchEntityCreateLookup[] {
    const selected: PendingBatchEntityCreateLookup[] = [];
    const selectedParticipantIds = new Set<string>();
    this.groupOrder.forEach((groupId) => {
      const lookup = this.pendingLookups.get(groupId);
      if (!lookup) {
        return;
      }
      const draftKey = lookup.input.draftId ?? '';
      const conflicts = lookup.input.participantIds.some((id) => selectedParticipantIds.has(`${draftKey}:${id}`));
      if (conflicts) {
        return;
      }
      selected.push(lookup);
      lookup.input.participantIds.forEach((id) => selectedParticipantIds.add(`${draftKey}:${id}`));
    });
    return selected;
  }

  private async flushReadyLookups(): Promise<void> {
    const selectedLookups = this.selectDisjointLookups();
    if (selectedLookups.length === 0) {
      return;
    }
    selectedLookups.forEach((lookup) => this.pendingLookups.delete(lookup.groupId));
    try {
      await this.acquireParticipantLocks(selectedLookups);
      const resolvedByType = await this.resolveDirectIdsByType(selectedLookups);
      const readyLookups = selectedLookups.map((lookup) => {
        const expectedIds = new Set(lookup.input.finderIds);
        const existingByIds = (resolvedByType.get(lookup.input.type) ?? []).filter((element) => {
          return getInstanceIds(element).some((id) => expectedIds.has(id));
        });
        const participantIds = normalizeIds([
          ...lookup.input.participantIds,
          ...existingByIds.flatMap((element) => getInstanceIds(element)),
        ]);
        return {
          lookup,
          participantIds,
          resolution: {
            existingByIds,
            lock: {
              signal: this.sharedAbortController.signal,
              unlock: async () => undefined,
            },
          },
        };
      });
      await this.acquireParticipantLocks(readyLookups.map(({ lookup, participantIds }) => ({
        ...lookup,
        input: {
          ...lookup.input,
          participantIds,
        },
      })));
      readyLookups.forEach((readyLookup) => {
        const { lookup, participantIds } = readyLookup;
        this.addGroupOwnedParticipantIds(lookup.groupId, lookup.input.draftId, participantIds);
        this.reserveGroupParticipantIds(lookup.groupId);
        this.groupStates.set(lookup.groupId, 'ready');
        this.readyLookups.push(readyLookup);
      });
      this.releaseReadyLookups();
    } catch (cause) {
      selectedLookups.forEach((lookup) => {
        this.groupStates.set(lookup.groupId, 'collecting');
        lookup.reject(cause);
      });
    }
  }

  private releaseReadyLookups(): void {
    if (this.closed) {
      return;
    }
    while (this.activeGroupCount < this.maxActiveGroups && this.readyLookups.length > 0) {
      const readyIndex = this.readyLookups.findIndex((readyLookup) => !this.hasReservedParticipantConflict(readyLookup.lookup.groupId));
      if (readyIndex === -1) {
        return;
      }
      const [ready] = this.readyLookups.splice(readyIndex, 1);
      this.groupStates.set(ready.lookup.groupId, 'active');
      this.activeGroupCount += 1;
      ready.lookup.resolve(ready.resolution);
    }
  }

  private addGroupOwnedParticipantIds(groupId: number, draftId: string | undefined, participantIds: string[]): void {
    const draftKey = draftId ?? '';
    const idsByDraft = this.ownedParticipantIdsByGroup.get(groupId) ?? new Map<string, Set<string>>();
    const groupParticipantIds = idsByDraft.get(draftKey) ?? new Set<string>();
    participantIds.forEach((id) => groupParticipantIds.add(id));
    idsByDraft.set(draftKey, groupParticipantIds);
    this.ownedParticipantIdsByGroup.set(groupId, idsByDraft);
  }

  private reserveGroupParticipantIds(groupId: number): void {
    const ownedIdsByDraft = this.ownedParticipantIdsByGroup.get(groupId);
    if (!ownedIdsByDraft) {
      this.reservedParticipantIdsByGroup.delete(groupId);
      return;
    }
    this.reservedParticipantIdsByGroup.set(groupId, new Map(
      Array.from(ownedIdsByDraft.entries()).map(([draftKey, participantIds]) => [draftKey, new Set(participantIds)]),
    ));
  }

  private hasReservedParticipantConflict(groupId: number): boolean {
    const participantIdsByDraft = this.reservedParticipantIdsByGroup.get(groupId);
    if (!participantIdsByDraft) {
      return false;
    }
    for (const [otherGroupId, otherIdsByDraft] of this.reservedParticipantIdsByGroup.entries()) {
      if (otherGroupId === groupId) {
        continue;
      }
      const otherState = this.groupStates.get(otherGroupId);
      if (otherState !== 'active') {
        continue;
      }
      for (const [draftKey, participantIds] of participantIdsByDraft.entries()) {
        const otherParticipantIds = otherIdsByDraft.get(draftKey);
        if (otherParticipantIds && Array.from(participantIds).some((id) => otherParticipantIds.has(id))) {
          return true;
        }
      }
    }
    return false;
  }

  private async acquireParticipantLocks(lookups: PendingBatchEntityCreateLookup[]): Promise<void> {
    const idsByDraft = new Map<string, Set<string>>();
    lookups.forEach((lookup) => {
      const draftKey = lookup.input.draftId ?? '';
      const draftIds = idsByDraft.get(draftKey) ?? new Set<string>();
      const heldIds = this.heldParticipantIdsByDraft.get(draftKey) ?? new Set<string>();
      lookup.input.participantIds.forEach((id) => {
        if (!heldIds.has(id)) {
          draftIds.add(id);
        }
      });
      idsByDraft.set(draftKey, draftIds);
    });

    for (const [draftKey, ids] of idsByDraft.entries()) {
      if (ids.size === 0) {
        continue;
      }
      const lock = await lockResources(Array.from(ids), { draftId: draftKey || undefined }) as BatchEntityCreateLock;
      if (lock.signal.aborted) {
        this.sharedAbortController.abort(lock.signal.reason);
      } else {
        lock.signal.addEventListener('abort', () => this.sharedAbortController.abort(lock.signal.reason), { once: true });
      }
      this.heldLocks.push(lock);
      const heldIds = this.heldParticipantIdsByDraft.get(draftKey) ?? new Set<string>();
      ids.forEach((id) => heldIds.add(id));
      this.heldParticipantIdsByDraft.set(draftKey, heldIds);
    }
  }

  private async resolveDirectIdsByType(lookups: PendingBatchEntityCreateLookup[]): Promise<Map<string, BasicStoreObject[]>> {
    const idsByType = new Map<string, Set<string>>();
    lookups.forEach((lookup) => {
      if (lookup.input.finderIds.length === 0) {
        return;
      }
      const typeIds = idsByType.get(lookup.input.type) ?? new Set<string>();
      lookup.input.finderIds.forEach((id) => typeIds.add(id));
      idsByType.set(lookup.input.type, typeIds);
    });

    const resolvedByType = new Map<string, BasicStoreObject[]>();
    await Promise.all(Array.from(idsByType.entries()).map(async ([type, ids]) => {
      const resolvedElements: BasicStoreObject[] = [];
      for (const chunk of chunkIds(Array.from(ids))) {
        const chunkElements = chunk.length === 0
          ? []
          : await internalFindByIds<BasicStoreObject>(this.context, SYSTEM_USER, chunk, { type }) as BasicStoreObject[];
        resolvedElements.push(...chunkElements);
      }
      resolvedByType.set(type, resolvedElements);
    }));
    return resolvedByType;
  }
}

export const runWithBatchEntityCreateCoordinator = async <T>(
  coordinator: BatchEntityCreateCoordinator,
  groupId: number,
  execute: () => Promise<T>,
): Promise<T> => {
  return batchEntityCreateCoordinatorStorage.run({ coordinator, groupId }, async () => {
    try {
      return await execute();
    } finally {
      coordinator.completeGroup(groupId);
    }
  });
};

export const resolveBatchEntityCreateLookup = (
  input: BatchEntityCreateLookupInput,
): Promise<BatchEntityCreateLookupResolution> | undefined => {
  const scope = batchEntityCreateCoordinatorStorage.getStore();
  return scope?.coordinator.registerLookup(scope.groupId, input);
};

export const resolveBatchParticipantLock = (
  input: BatchParticipantLockInput,
): Promise<BatchEntityCreateLock> | undefined => {
  const scope = batchEntityCreateCoordinatorStorage.getStore();
  return scope?.coordinator.registerLookup(scope.groupId, {
    ...input,
    finderIds: [],
    type: '',
  }).then((resolution) => resolution.lock);
};

export const getBatchEntityCreateCoordinatorHeldParticipantIds = (draftId?: string): string[] => {
  const scope = batchEntityCreateCoordinatorStorage.getStore();
  return scope?.coordinator.getHeldParticipantIds(scope.groupId, draftId) ?? [];
};
