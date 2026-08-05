import { AsyncLocalStorage } from 'node:async_hooks';
import { internalFindByIds } from '../../database/middleware-loader';
import { isNotEmptyField } from '../../database/utils';
import { lockResources } from '../../lock/master-lock';
import { getInstanceIds } from '../../schema/identifier';
import type { BasicStoreObject } from '../../types/store';
import type { AuthContext } from '../../types/user';
import { SYSTEM_USER } from '../../utils/access';
import { retainBatchLockUntilCommit } from './batch-lock-retention';

type BatchEntityCreateGroupState = 'collecting' | 'waiting' | 'ready' | 'active' | 'parked' | 'completed';
type BatchEntityCreateLookupRetention = 'group' | 'lock';

type BatchEntityCreateLock = {
  draftId?: string;
  participantIds?: string[];
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
  retention: BatchEntityCreateLookupRetention;
  resolve: (resolution: BatchEntityCreateLookupResolution) => void;
};

type ReadyBatchEntityCreateActivation = {
  groupId: number;
  participantIds: string[];
  reject: (cause: unknown) => void;
  resolve: () => void;
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

  private readonly scopedLocks = new Set<BatchEntityCreateLock>();

  private readonly heldParticipantIdsByDraft = new Map<string, Set<string>>();

  private readonly ownedParticipantIdsByGroup = new Map<number, Map<string, Set<string>>>();

  private readonly scopedParticipantIdCountsByGroup = new Map<number, Map<string, Map<string, number>>>();

  private readonly reservedParticipantIdsByGroup = new Map<number, Map<string, Set<string>>>();

  private readonly pendingLookups = new Map<number, PendingBatchEntityCreateLookup>();

  private readonly readyActivations: ReadyBatchEntityCreateActivation[] = [];

  private readonly sharedAbortController = new AbortController();

  private activeGroupCount = 0;

  private flushPromise: Promise<void> | undefined;

  private closed = false;

  private reservationChangePromise!: Promise<void>;

  private reservationChangeResolver!: () => void;

  constructor(context: AuthContext, groupIds: number[], maxActiveGroups = Number.POSITIVE_INFINITY) {
    this.context = context;
    this.groupOrder = [...groupIds];
    this.maxActiveGroups = Math.max(1, maxActiveGroups);
    this.resetReservationChangePromise();
    this.groupOrder.forEach((groupId) => this.groupStates.set(groupId, 'collecting'));
  }

  getHeldParticipantIds(groupId: number, draftId?: string): string[] {
    const draftKey = draftId ?? '';
    return normalizeIds([
      ...Array.from(this.ownedParticipantIdsByGroup.get(groupId)?.get(draftKey) ?? []),
      ...Array.from(this.scopedParticipantIdCountsByGroup.get(groupId)?.get(draftKey)?.keys() ?? []),
    ]);
  }

  registerLookup(
    groupId: number,
    input: BatchEntityCreateLookupInput,
    retention: BatchEntityCreateLookupRetention = 'group',
  ): Promise<BatchEntityCreateLookupResolution> {
    if (this.closed) {
      return Promise.reject(new Error('Batch entity create coordinator is closed'));
    }
    if (this.pendingLookups.has(groupId)) {
      return Promise.reject(new Error(`Batch entity create group ${groupId} already has a pending lookup`));
    }
    if (this.groupStates.get(groupId) === 'active') {
      this.activeGroupCount -= 1;
      this.reservedParticipantIdsByGroup.delete(groupId);
      this.notifyReservationChange();
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
        retention,
        resolve,
      });
      this.groupStates.set(groupId, 'waiting');
      this.releaseReadyActivations();
      this.scheduleFlushIfReady();
    });
  }

  completeGroup(groupId: number): void {
    if (this.groupStates.get(groupId) === 'active') {
      this.activeGroupCount -= 1;
    }
    this.groupStates.set(groupId, 'completed');
    this.ownedParticipantIdsByGroup.delete(groupId);
    this.scopedParticipantIdCountsByGroup.delete(groupId);
    this.reservedParticipantIdsByGroup.delete(groupId);
    this.notifyReservationChange();
    this.releaseReadyActivations();
    this.scheduleFlushIfReady();
  }

  acquireParticipantLock(groupId: number, input: BatchParticipantLockInput): Promise<BatchEntityCreateLock> {
    if (this.groupStates.get(groupId) !== 'active') {
      return this.registerLookup(groupId, {
        ...input,
        finderIds: [],
        type: '',
      }, 'lock').then((resolution) => resolution.lock);
    }
    return this.acquireAdditionalParticipantLock(groupId, input);
  }

  waitForPromise<T>(groupId: number, promise: Promise<T>): Promise<T> {
    if (this.closed) {
      return Promise.reject(new Error('Batch entity create coordinator is closed'));
    }
    this.parkGroup(groupId);
    return promise.then(
      async (value) => {
        await this.resumeParkedGroup(groupId);
        return value;
      },
      async (cause) => {
        await this.resumeParkedGroup(groupId);
        throw cause;
      },
    );
  }

  async close(): Promise<void> {
    this.closed = true;
    await this.flushPromise?.catch(() => undefined);
    const pending = Array.from(this.pendingLookups.values());
    this.pendingLookups.clear();
    pending.forEach((lookup) => lookup.reject(new Error('Batch entity create phase ended before lookup resolution')));
    const ready = this.readyActivations.splice(0);
    ready.forEach(({ reject }) => reject(new Error('Batch entity create phase ended before lookup resolution')));
    const retainedLocks = this.heldLocks.filter((lock) => {
      return retainBatchLockUntilCommit(lock, lock.participantIds ?? [], lock.draftId);
    });
    await this.releasePhysicalLocks([
      ...Array.from(this.scopedLocks),
      ...this.heldLocks.filter((lock) => !retainedLocks.includes(lock)).reverse(),
    ]);
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
      return state === 'waiting' || state === 'parked' || state === 'completed';
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
    const scopedLocksByGroup = new Map<number, BatchEntityCreateLock[]>();
    try {
      await this.acquireParticipantLocks(selectedLookups, scopedLocksByGroup);
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
            lock: this.buildNoopLock(),
          },
        };
      });
      await this.acquireParticipantLocks(readyLookups.map(({ lookup, participantIds }) => ({
        ...lookup,
        input: {
          ...lookup.input,
          participantIds,
        },
      })), scopedLocksByGroup);
      readyLookups.forEach((readyLookup) => {
        const { lookup, participantIds } = readyLookup;
        if (lookup.retention === 'lock') {
          this.addGroupScopedParticipantIds(lookup.groupId, lookup.input.draftId, participantIds);
          readyLookup.resolution.lock = this.buildScopedParticipantLock(
            lookup.groupId,
            lookup.input.draftId,
            participantIds,
            scopedLocksByGroup.get(lookup.groupId) ?? [],
          );
        } else {
          this.addGroupOwnedParticipantIds(lookup.groupId, lookup.input.draftId, participantIds);
        }
        this.reserveGroupParticipantIds(lookup.groupId);
        this.groupStates.set(lookup.groupId, 'ready');
        this.readyActivations.push({
          groupId: lookup.groupId,
          participantIds,
          reject: lookup.reject,
          resolve: () => lookup.resolve(readyLookup.resolution),
        });
      });
      this.releaseReadyActivations();
    } catch (cause) {
      await this.releasePhysicalLocks(Array.from(scopedLocksByGroup.values()).flat());
      selectedLookups.forEach((lookup) => {
        this.groupStates.set(lookup.groupId, 'collecting');
        lookup.reject(cause);
      });
    }
  }

  private releaseReadyActivations(): void {
    if (this.closed) {
      return;
    }
    while (this.activeGroupCount < this.maxActiveGroups && this.readyActivations.length > 0) {
      const readyIndex = this.readyActivations.findIndex((readyActivation) => !this.hasReservedParticipantConflict(readyActivation.groupId));
      if (readyIndex === -1) {
        return;
      }
      const [ready] = this.readyActivations.splice(readyIndex, 1);
      this.groupStates.set(ready.groupId, 'active');
      this.activeGroupCount += 1;
      ready.resolve();
    }
  }

  private parkGroup(groupId: number): void {
    const state = this.groupStates.get(groupId);
    if (state === 'active') {
      this.activeGroupCount -= 1;
      this.reservedParticipantIdsByGroup.delete(groupId);
      this.notifyReservationChange();
    } else if (state !== 'collecting') {
      throw new Error(`Batch entity create group ${groupId} cannot be parked from state ${state}`);
    }
    this.groupStates.set(groupId, 'parked');
    this.releaseReadyActivations();
    this.scheduleFlushIfReady();
  }

  private resumeParkedGroup(groupId: number): Promise<void> {
    if (this.closed) {
      return Promise.reject(new Error('Batch entity create coordinator is closed'));
    }
    if (this.groupStates.get(groupId) !== 'parked') {
      return Promise.reject(new Error(`Batch entity create group ${groupId} is not parked`));
    }
    return new Promise<void>((resolve, reject) => {
      this.reserveGroupParticipantIds(groupId);
      this.groupStates.set(groupId, 'ready');
      this.readyActivations.push({
        groupId,
        participantIds: this.getHeldParticipantIds(groupId),
        reject,
        resolve,
      });
      this.releaseReadyActivations();
    });
  }

  private addGroupOwnedParticipantIds(groupId: number, draftId: string | undefined, participantIds: string[]): void {
    const draftKey = draftId ?? '';
    const idsByDraft = this.ownedParticipantIdsByGroup.get(groupId) ?? new Map<string, Set<string>>();
    const groupParticipantIds = idsByDraft.get(draftKey) ?? new Set<string>();
    participantIds.forEach((id) => groupParticipantIds.add(id));
    idsByDraft.set(draftKey, groupParticipantIds);
    this.ownedParticipantIdsByGroup.set(groupId, idsByDraft);
  }

  private addGroupScopedParticipantIds(groupId: number, draftId: string | undefined, participantIds: string[]): void {
    const draftKey = draftId ?? '';
    const countsByDraft = this.scopedParticipantIdCountsByGroup.get(groupId) ?? new Map<string, Map<string, number>>();
    const participantCounts = countsByDraft.get(draftKey) ?? new Map<string, number>();
    participantIds.forEach((id) => participantCounts.set(id, (participantCounts.get(id) ?? 0) + 1));
    countsByDraft.set(draftKey, participantCounts);
    this.scopedParticipantIdCountsByGroup.set(groupId, countsByDraft);
  }

  private removeGroupScopedParticipantIds(groupId: number, draftId: string | undefined, participantIds: string[]): void {
    const draftKey = draftId ?? '';
    const countsByDraft = this.scopedParticipantIdCountsByGroup.get(groupId);
    const participantCounts = countsByDraft?.get(draftKey);
    if (!participantCounts) {
      return;
    }
    participantIds.forEach((id) => {
      const nextCount = (participantCounts.get(id) ?? 0) - 1;
      if (nextCount > 0) {
        participantCounts.set(id, nextCount);
      } else {
        participantCounts.delete(id);
      }
    });
    if (participantCounts.size === 0) {
      countsByDraft?.delete(draftKey);
    }
    if (countsByDraft?.size === 0) {
      this.scopedParticipantIdCountsByGroup.delete(groupId);
    }
    this.reserveGroupParticipantIds(groupId);
    this.releaseReadyActivations();
  }

  private reserveGroupParticipantIds(groupId: number): void {
    const ownedIdsByDraft = this.ownedParticipantIdsByGroup.get(groupId);
    const scopedCountsByDraft = this.scopedParticipantIdCountsByGroup.get(groupId);
    if (!ownedIdsByDraft && !scopedCountsByDraft) {
      this.reservedParticipantIdsByGroup.delete(groupId);
      this.notifyReservationChange();
      return;
    }
    const reservedIdsByDraft = new Map<string, Set<string>>();
    ownedIdsByDraft?.forEach((participantIds, draftKey) => {
      reservedIdsByDraft.set(draftKey, new Set(participantIds));
    });
    scopedCountsByDraft?.forEach((participantCounts, draftKey) => {
      const participantIds = reservedIdsByDraft.get(draftKey) ?? new Set<string>();
      participantCounts.forEach((_count, participantId) => participantIds.add(participantId));
      reservedIdsByDraft.set(draftKey, participantIds);
    });
    this.reservedParticipantIdsByGroup.set(groupId, reservedIdsByDraft);
    this.notifyReservationChange();
  }

  private getActiveConflictGroupIds(groupId: number, draftId: string | undefined, participantIds: string[]): number[] {
    const draftKey = draftId ?? '';
    const requestedIds = new Set(participantIds);
    const conflictingGroupIds: number[] = [];
    this.reservedParticipantIdsByGroup.forEach((idsByDraft, otherGroupId) => {
      if (otherGroupId === groupId || this.groupStates.get(otherGroupId) !== 'active') {
        return;
      }
      const otherIds = idsByDraft.get(draftKey);
      if (otherIds && Array.from(requestedIds).some((id) => otherIds.has(id))) {
        conflictingGroupIds.push(otherGroupId);
      }
    });
    return conflictingGroupIds;
  }

  private async acquireAdditionalParticipantLock(groupId: number, input: BatchParticipantLockInput): Promise<BatchEntityCreateLock> {
    const participantIds = normalizeIds(input.participantIds);
    const ownedParticipantIds = new Set(this.getHeldParticipantIds(groupId, input.draftId));
    const additionalParticipantIds = participantIds.filter((id) => !ownedParticipantIds.has(id));
    if (additionalParticipantIds.length === 0) {
      return this.buildNoopLock();
    }

    while (true) {
      const reservationChangePromise = this.reservationChangePromise;
      const conflictingGroupIds = this.getActiveConflictGroupIds(groupId, input.draftId, additionalParticipantIds);
      if (conflictingGroupIds.length === 0) {
        break;
      }
      await reservationChangePromise;
    }

    this.addGroupScopedParticipantIds(groupId, input.draftId, additionalParticipantIds);
    this.reserveGroupParticipantIds(groupId);
    const scopedLocksByGroup = new Map<number, BatchEntityCreateLock[]>();
    try {
      await this.acquireScopedParticipantLocks([{
        groupId,
        input: {
          ...input,
          finderIds: [],
          participantIds: additionalParticipantIds,
          type: '',
        },
        reject: () => undefined,
        retention: 'lock',
        resolve: () => undefined,
      }], scopedLocksByGroup);
      return this.buildScopedParticipantLock(
        groupId,
        input.draftId,
        additionalParticipantIds,
        scopedLocksByGroup.get(groupId) ?? [],
      );
    } catch (cause) {
      await this.releasePhysicalLocks(Array.from(scopedLocksByGroup.values()).flat());
      this.removeGroupScopedParticipantIds(groupId, input.draftId, additionalParticipantIds);
      throw cause;
    }
  }

  private buildNoopLock(): BatchEntityCreateLock {
    return {
      signal: this.sharedAbortController.signal,
      unlock: async () => undefined,
    };
  }

  private buildScopedParticipantLock(
    groupId: number,
    draftId: string | undefined,
    participantIds: string[],
    physicalLocks: BatchEntityCreateLock[] = [],
  ): BatchEntityCreateLock {
    let unlocked = false;
    return {
      signal: this.sharedAbortController.signal,
      unlock: async () => {
        if (unlocked) {
          return;
        }
        unlocked = true;
        await this.releasePhysicalLocks(physicalLocks);
        this.removeGroupScopedParticipantIds(groupId, draftId, participantIds);
      },
    };
  }

  private resetReservationChangePromise(): void {
    this.reservationChangePromise = new Promise<void>((resolve) => {
      this.reservationChangeResolver = resolve;
    });
  }

  private notifyReservationChange(): void {
    this.reservationChangeResolver();
    this.resetReservationChangePromise();
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

  private async acquireParticipantLocks(
    lookups: PendingBatchEntityCreateLookup[],
    scopedLocksByGroup: Map<number, BatchEntityCreateLock[]>,
  ): Promise<void> {
    await this.acquireRetainedParticipantLocks(lookups.filter((lookup) => lookup.retention === 'group'));
    await this.acquireScopedParticipantLocks(lookups.filter((lookup) => lookup.retention === 'lock'), scopedLocksByGroup);
  }

  private async acquireRetainedParticipantLocks(lookups: PendingBatchEntityCreateLookup[]): Promise<void> {
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
      await this.acquirePhysicalLock(Array.from(ids), draftKey || undefined, true);
    }
  }

  private async acquireScopedParticipantLocks(
    lookups: PendingBatchEntityCreateLookup[],
    scopedLocksByGroup: Map<number, BatchEntityCreateLock[]>,
  ): Promise<void> {
    for (const lookup of lookups) {
      const draftKey = lookup.input.draftId ?? '';
      const heldIds = this.heldParticipantIdsByDraft.get(draftKey) ?? new Set<string>();
      const participantIds = lookup.input.participantIds.filter((id) => !heldIds.has(id));
      if (participantIds.length === 0) {
        continue;
      }
      const lock = await this.acquirePhysicalLock(participantIds, lookup.input.draftId, false);
      const groupLocks = scopedLocksByGroup.get(lookup.groupId) ?? [];
      groupLocks.push(lock);
      scopedLocksByGroup.set(lookup.groupId, groupLocks);
    }
  }

  private async acquirePhysicalLock(
    participantIds: string[],
    draftId: string | undefined,
    retained: boolean,
  ): Promise<BatchEntityCreateLock> {
    const normalizedParticipantIds = normalizeIds(participantIds);
    const draftKey = draftId ?? '';
    const lock = await lockResources(normalizedParticipantIds, { draftId }) as BatchEntityCreateLock;
    if (lock.signal.aborted) {
      this.sharedAbortController.abort(lock.signal.reason);
    } else {
      lock.signal.addEventListener('abort', () => this.sharedAbortController.abort(lock.signal.reason), { once: true });
    }
    this.addHeldParticipantIds(draftKey, normalizedParticipantIds);
    let unlocked = false;
    const trackedLock: BatchEntityCreateLock = {
      draftId,
      participantIds: normalizedParticipantIds,
      signal: lock.signal,
      unlock: async () => {
        if (unlocked) {
          return;
        }
        unlocked = true;
        try {
          await lock.unlock();
        } finally {
          this.removeHeldParticipantIds(draftKey, normalizedParticipantIds);
          this.scopedLocks.delete(trackedLock);
        }
      },
    };
    if (retained) {
      this.heldLocks.push(trackedLock);
    } else {
      this.scopedLocks.add(trackedLock);
    }
    return trackedLock;
  }

  private addHeldParticipantIds(draftKey: string, participantIds: string[]): void {
    const heldIds = this.heldParticipantIdsByDraft.get(draftKey) ?? new Set<string>();
    participantIds.forEach((id) => heldIds.add(id));
    this.heldParticipantIdsByDraft.set(draftKey, heldIds);
  }

  private removeHeldParticipantIds(draftKey: string, participantIds: string[]): void {
    const heldIds = this.heldParticipantIdsByDraft.get(draftKey);
    if (!heldIds) {
      return;
    }
    participantIds.forEach((id) => heldIds.delete(id));
    if (heldIds.size === 0) {
      this.heldParticipantIdsByDraft.delete(draftKey);
    }
  }

  private async releasePhysicalLocks(locks: BatchEntityCreateLock[]): Promise<void> {
    await Promise.allSettled(Array.from(new Set(locks)).reverse().map((lock) => lock.unlock()));
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
  return scope?.coordinator.acquireParticipantLock(scope.groupId, input);
};

export const getBatchEntityCreateCoordinatorGroupId = (): number | undefined => {
  return batchEntityCreateCoordinatorStorage.getStore()?.groupId;
};

export const waitForBatchEntityCreateCoordinatorPromise = <T>(promise: Promise<T>): Promise<T> | undefined => {
  const scope = batchEntityCreateCoordinatorStorage.getStore();
  return scope?.coordinator.waitForPromise(scope.groupId, promise);
};

export const getBatchEntityCreateCoordinatorHeldParticipantIds = (draftId?: string): string[] => {
  const scope = batchEntityCreateCoordinatorStorage.getStore();
  return scope?.coordinator.getHeldParticipantIds(scope.groupId, draftId) ?? [];
};
