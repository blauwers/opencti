import { AsyncLocalStorage } from 'node:async_hooks';
import { booleanConf, logApp } from '../../config/conf';
import { internalFindByIds } from '../../database/middleware-loader';
import { isNotEmptyField } from '../../database/utils';
import { lockResources } from '../../lock/master-lock';
import { getInstanceIds } from '../../schema/identifier';
import type { BasicStoreObject } from '../../types/store';
import type { AuthContext } from '../../types/user';
import { SYSTEM_USER } from '../../utils/access';
import { getBatchExecutionPerformanceTraceId } from './batch-executor';
import { getBatchAwareLockOptions, getBatchRetainedLockIds, retainBatchLockUntilCommit } from './batch-lock-retention';
import { hasBatchCreatedEntityParticipant } from './batch-relation-lookup';

type BatchEntityCreateGroupState = 'collecting' | 'waiting' | 'ready' | 'active' | 'parked' | 'completed';
type BatchEntityCreateLookupRetention = 'group' | 'lock';
type BatchEntityCreateGroupWaitReason = 'coalesced_promise' | 'follow_on_conflict' | 'follow_on_physical_lock';
type BatchEntityCreatePhysicalLockSource = 'retained_lookup_wave' | 'scoped_lookup' | 'follow_on_scoped';

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

type BatchEntityCreateGroupWait = {
  conflictingGroupIds?: number[];
  participantIds?: string[];
  reason: BatchEntityCreateGroupWaitReason;
  startedAt: number;
};

type PendingBatchEntityCreatePhysicalLock = {
  draftId?: string;
  groupIds: number[];
  id: number;
  participantIds: string[];
  retained: boolean;
  source: BatchEntityCreatePhysicalLockSource;
  startedAt: number;
};

type BatchEntityCreateCoordinatorScope = {
  active: boolean;
  coordinator: BatchEntityCreateCoordinator;
  groupId: number;
};

const MAX_DIRECT_ID_LOOKUP_SIZE = 5000;
const BATCH_ENTITY_CREATE_COORDINATOR_PERFORMANCE_LOG = booleanConf('app:performance_logger', false);
const BATCH_ENTITY_CREATE_COORDINATOR_LOG_MESSAGE = '[BATCH] Entity create coordinator';
const BATCH_ENTITY_CREATE_COORDINATOR_SNAPSHOT_INTERVAL_MS = 5000;
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

  private readonly lookupTailsByGroup = new Map<number, Promise<BatchEntityCreateLookupResolution>>();

  private readonly readyActivations: ReadyBatchEntityCreateActivation[] = [];

  private readonly sharedAbortController = new AbortController();

  private readonly groupWaits = new Map<number, BatchEntityCreateGroupWait>();

  private readonly pendingPhysicalLocks = new Map<number, PendingBatchEntityCreatePhysicalLock>();

  private readonly performanceTraceId = getBatchExecutionPerformanceTraceId();

  private readonly snapshotTimer: NodeJS.Timeout | undefined;

  private activeGroupCount = 0;

  private flushPromise: Promise<void> | undefined;

  private closed = false;

  private flushCount = 0;

  private followOnWaitCount = 0;

  private parkCount = 0;

  private resumeCount = 0;

  private nextPhysicalLockId = 0;

  private lastFollowOnWait: Record<string, unknown> | undefined;

  private reservationChangePromise!: Promise<void>;

  private reservationChangeResolver!: () => void;

  constructor(context: AuthContext, groupIds: number[], maxActiveGroups = Number.POSITIVE_INFINITY) {
    this.context = context;
    this.groupOrder = [...groupIds];
    this.maxActiveGroups = Math.max(1, maxActiveGroups);
    this.resetReservationChangePromise();
    this.groupOrder.forEach((groupId) => this.groupStates.set(groupId, 'collecting'));
    this.snapshotTimer = BATCH_ENTITY_CREATE_COORDINATOR_PERFORMANCE_LOG
      ? setInterval(() => this.logState('snapshot'), BATCH_ENTITY_CREATE_COORDINATOR_SNAPSHOT_INTERVAL_MS)
      : undefined;
    this.snapshotTimer?.unref();
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
    const priorLookupTail = this.lookupTailsByGroup.get(groupId);
    const lookupPromise = priorLookupTail
      ? priorLookupTail.then(() => this.registerLookupImmediately(groupId, input, retention))
      : this.registerLookupImmediately(groupId, input, retention);
    this.lookupTailsByGroup.set(groupId, lookupPromise);
    void lookupPromise.then(
      () => {
        if (this.lookupTailsByGroup.get(groupId) === lookupPromise) {
          this.lookupTailsByGroup.delete(groupId);
        }
      },
      () => {
        if (this.lookupTailsByGroup.get(groupId) === lookupPromise) {
          this.lookupTailsByGroup.delete(groupId);
        }
      },
    );
    return lookupPromise;
  }

  private registerLookupImmediately(
    groupId: number,
    input: BatchEntityCreateLookupInput,
    retention: BatchEntityCreateLookupRetention,
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
    this.groupWaits.delete(groupId);
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
    this.setGroupWait(groupId, 'coalesced_promise');
    this.parkGroup(groupId);
    return promise.then(
      async (value) => {
        try {
          await this.resumeParkedGroup(groupId);
        } finally {
          this.clearGroupWait(groupId, 'coalesced_promise');
        }
        return value;
      },
      async (cause) => {
        try {
          await this.resumeParkedGroup(groupId);
        } finally {
          this.clearGroupWait(groupId, 'coalesced_promise');
        }
        throw cause;
      },
    );
  }

  async close(): Promise<void> {
    if (this.snapshotTimer) {
      clearInterval(this.snapshotTimer);
    }
    // Lookup tails can enqueue another ready flush while the previous flush is
    // settling. Let already-admitted work drain before closing the coordinator,
    // otherwise a later tail is rejected even though its parent group completed.
    this.scheduleFlushIfReady();
    while (this.flushPromise) {
      await this.flushPromise.catch(() => undefined);
      this.scheduleFlushIfReady();
    }
    this.closed = true;
    const pending = Array.from(this.pendingLookups.values());
    this.pendingLookups.clear();
    pending.forEach((lookup) => lookup.reject(new Error('Batch entity create phase ended before lookup resolution')));
    const ready = this.readyActivations.splice(0);
    ready.forEach(({ reject }) => reject(new Error('Batch entity create phase ended before lookup resolution')));
    const retainedLocks = this.heldLocks.filter((lock) => {
      const participantIds = lock.participantIds ?? [];
      return hasBatchCreatedEntityParticipant(participantIds)
        && retainBatchLockUntilCommit(lock, participantIds, lock.draftId);
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
    this.flushCount += 1;
    const flushStartedAt = Date.now();
    this.logState('flush_started', {
      flush_count: this.flushCount,
      selected_lookup_count: selectedLookups.length,
    });
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
      this.logState('flush_completed', {
        duration_ms: Date.now() - flushStartedAt,
        flush_count: this.flushCount,
        selected_lookup_count: selectedLookups.length,
      });
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
    this.parkCount += 1;
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
      this.resumeCount += 1;
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

    const conflictingGroupIds = this.getActiveConflictGroupIds(groupId, input.draftId, additionalParticipantIds);
    this.addGroupScopedParticipantIds(groupId, input.draftId, additionalParticipantIds);
    if (conflictingGroupIds.length > 0) {
      this.followOnWaitCount += 1;
      this.lastFollowOnWait = {
        additional_participant_count: additionalParticipantIds.length,
        conflicting_group_ids: conflictingGroupIds.slice(0, 5),
        group_id: groupId,
        sample_participant_ids: additionalParticipantIds.slice(0, 5),
      };
      // A group that keeps its current reservations while waiting for another
      // active group can deadlock with a crossed follow-on lock request.
      this.setGroupWait(groupId, 'follow_on_conflict', additionalParticipantIds, conflictingGroupIds);
      this.parkGroup(groupId);
      try {
        await this.resumeParkedGroup(groupId);
      } catch (cause) {
        this.removeGroupScopedParticipantIds(groupId, input.draftId, additionalParticipantIds);
        throw cause;
      } finally {
        this.clearGroupWait(groupId, 'follow_on_conflict');
      }
    } else {
      this.reserveGroupParticipantIds(groupId);
    }

    const scopedLocksByGroup = new Map<number, BatchEntityCreateLock[]>();
    try {
      this.setGroupWait(groupId, 'follow_on_physical_lock', additionalParticipantIds);
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
        }], scopedLocksByGroup, 'follow_on_scoped');
      } finally {
        this.clearGroupWait(groupId, 'follow_on_physical_lock');
      }
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
    const groupIdsByDraft = new Map<string, Set<number>>();
    lookups.forEach((lookup) => {
      const draftKey = lookup.input.draftId ?? '';
      const draftIds = idsByDraft.get(draftKey) ?? new Set<string>();
      const draftGroupIds = groupIdsByDraft.get(draftKey) ?? new Set<number>();
      const heldIds = new Set([
        ...Array.from(this.heldParticipantIdsByDraft.get(draftKey) ?? []),
        ...getBatchRetainedLockIds(lookup.input.draftId),
      ]);
      lookup.input.participantIds.forEach((id) => {
        if (!heldIds.has(id)) {
          draftIds.add(id);
        }
      });
      idsByDraft.set(draftKey, draftIds);
      draftGroupIds.add(lookup.groupId);
      groupIdsByDraft.set(draftKey, draftGroupIds);
    });

    for (const [draftKey, ids] of idsByDraft.entries()) {
      if (ids.size === 0) {
        continue;
      }
      await this.acquirePhysicalLock(Array.from(ids), draftKey || undefined, true, {
        groupIds: Array.from(groupIdsByDraft.get(draftKey) ?? []),
        source: 'retained_lookup_wave',
      });
    }
  }

  private async acquireScopedParticipantLocks(
    lookups: PendingBatchEntityCreateLookup[],
    scopedLocksByGroup: Map<number, BatchEntityCreateLock[]>,
    source: BatchEntityCreatePhysicalLockSource = 'scoped_lookup',
  ): Promise<void> {
    for (const lookup of lookups) {
      const draftKey = lookup.input.draftId ?? '';
      const heldIds = new Set([
        ...Array.from(this.heldParticipantIdsByDraft.get(draftKey) ?? []),
        ...getBatchRetainedLockIds(lookup.input.draftId),
      ]);
      const participantIds = lookup.input.participantIds.filter((id) => !heldIds.has(id));
      if (participantIds.length === 0) {
        continue;
      }
      const lock = await this.acquirePhysicalLock(participantIds, lookup.input.draftId, false, {
        groupIds: [lookup.groupId],
        source,
      });
      const groupLocks = scopedLocksByGroup.get(lookup.groupId) ?? [];
      groupLocks.push(lock);
      scopedLocksByGroup.set(lookup.groupId, groupLocks);
    }
  }

  private async acquirePhysicalLock(
    participantIds: string[],
    draftId: string | undefined,
    retained: boolean,
    metadata: {
      groupIds: number[];
      source: BatchEntityCreatePhysicalLockSource;
    },
  ): Promise<BatchEntityCreateLock> {
    const normalizedParticipantIds = normalizeIds(participantIds);
    const draftKey = draftId ?? '';
    const pendingLock = this.trackPendingPhysicalLock(normalizedParticipantIds, draftId, retained, metadata);
    let lock: BatchEntityCreateLock;
    try {
      lock = await lockResources(normalizedParticipantIds, getBatchAwareLockOptions(draftId)) as BatchEntityCreateLock;
      const durationMs = Date.now() - pendingLock.startedAt;
      if (durationMs >= BATCH_ENTITY_CREATE_COORDINATOR_SNAPSHOT_INTERVAL_MS) {
        this.logState('physical_lock_acquired', {
          duration_ms: durationMs,
          physical_lock_group_ids: pendingLock.groupIds.slice(0, 5),
          physical_lock_id: pendingLock.id,
          physical_lock_participant_count: pendingLock.participantIds.length,
          physical_lock_retained: pendingLock.retained,
          physical_lock_sample_participant_ids: pendingLock.participantIds.slice(0, 5),
          physical_lock_source: pendingLock.source,
        });
      }
    } catch (cause) {
      this.logState('physical_lock_failed', {
        duration_ms: Date.now() - pendingLock.startedAt,
        physical_lock_error: cause instanceof Error ? cause.message : String(cause),
        physical_lock_group_ids: pendingLock.groupIds.slice(0, 5),
        physical_lock_id: pendingLock.id,
        physical_lock_participant_count: pendingLock.participantIds.length,
        physical_lock_retained: pendingLock.retained,
        physical_lock_sample_participant_ids: pendingLock.participantIds.slice(0, 5),
        physical_lock_source: pendingLock.source,
      });
      throw cause;
    } finally {
      this.pendingPhysicalLocks.delete(pendingLock.id);
    }
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

  private setGroupWait(
    groupId: number,
    reason: BatchEntityCreateGroupWaitReason,
    participantIds?: string[],
    conflictingGroupIds?: number[],
  ): void {
    this.groupWaits.set(groupId, {
      conflictingGroupIds,
      participantIds,
      reason,
      startedAt: Date.now(),
    });
  }

  private clearGroupWait(groupId: number, reason: BatchEntityCreateGroupWaitReason): void {
    if (this.groupWaits.get(groupId)?.reason === reason) {
      this.groupWaits.delete(groupId);
    }
  }

  private trackPendingPhysicalLock(
    participantIds: string[],
    draftId: string | undefined,
    retained: boolean,
    metadata: {
      groupIds: number[];
      source: BatchEntityCreatePhysicalLockSource;
    },
  ): PendingBatchEntityCreatePhysicalLock {
    const pendingLock = {
      draftId,
      groupIds: metadata.groupIds,
      id: this.nextPhysicalLockId += 1,
      participantIds,
      retained,
      source: metadata.source,
      startedAt: Date.now(),
    };
    this.pendingPhysicalLocks.set(pendingLock.id, pendingLock);
    return pendingLock;
  }

  private logState(event: string, extra: Record<string, unknown> = {}): void {
    if (!BATCH_ENTITY_CREATE_COORDINATOR_PERFORMANCE_LOG) {
      return;
    }
    const groupStateCounts = new Map<BatchEntityCreateGroupState, number>();
    this.groupStates.forEach((state) => {
      groupStateCounts.set(state, (groupStateCounts.get(state) ?? 0) + 1);
    });
    const heldParticipantCount = Array.from(this.heldParticipantIdsByDraft.values())
      .reduce((count, ids) => count + ids.size, 0);
    const groupWaitReasonCounts = new Map<BatchEntityCreateGroupWaitReason, number>();
    this.groupWaits.forEach((wait) => {
      groupWaitReasonCounts.set(wait.reason, (groupWaitReasonCounts.get(wait.reason) ?? 0) + 1);
    });
    const sampleActiveGroups = this.groupOrder
      .filter((groupId) => this.groupStates.get(groupId) === 'active')
      .slice(0, 5)
      .map((groupId) => {
        const ownedParticipantIds = Array.from(this.ownedParticipantIdsByGroup.get(groupId)?.values() ?? [])
          .flatMap((ids) => Array.from(ids));
        const scopedParticipantIds = Array.from(this.scopedParticipantIdCountsByGroup.get(groupId)?.values() ?? [])
          .flatMap((counts) => Array.from(counts.keys()));
        const reservedParticipantIds = Array.from(this.reservedParticipantIdsByGroup.get(groupId)?.values() ?? [])
          .flatMap((ids) => Array.from(ids));
        const wait = this.groupWaits.get(groupId);
        return {
          group_id: groupId,
          owned_participant_count: ownedParticipantIds.length,
          reserved_participant_count: reservedParticipantIds.length,
          sample_owned_participant_ids: ownedParticipantIds.slice(0, 3),
          sample_scoped_participant_ids: scopedParticipantIds.slice(0, 3),
          scoped_participant_count: scopedParticipantIds.length,
          wait_duration_ms: wait ? Date.now() - wait.startedAt : undefined,
          wait_reason: wait?.reason,
        };
      });
    const sampleGroupWaits = Array.from(this.groupWaits.entries())
      .sort(([, first], [, second]) => first.startedAt - second.startedAt)
      .slice(0, 5)
      .map(([groupId, wait]) => ({
        conflicting_group_ids: wait.conflictingGroupIds?.slice(0, 5),
        duration_ms: Date.now() - wait.startedAt,
        group_id: groupId,
        reason: wait.reason,
        sample_participant_ids: wait.participantIds?.slice(0, 5),
        state: this.groupStates.get(groupId),
      }));
    const samplePendingPhysicalLocks = Array.from(this.pendingPhysicalLocks.values())
      .sort((first, second) => first.startedAt - second.startedAt)
      .slice(0, 5)
      .map((pendingLock) => ({
        duration_ms: Date.now() - pendingLock.startedAt,
        group_ids: pendingLock.groupIds.slice(0, 5),
        id: pendingLock.id,
        participant_count: pendingLock.participantIds.length,
        retained: pendingLock.retained,
        sample_participant_ids: pendingLock.participantIds.slice(0, 5),
        source: pendingLock.source,
      }));
    logApp.info(BATCH_ENTITY_CREATE_COORDINATOR_LOG_MESSAGE, {
      active_group_count: this.activeGroupCount,
      event,
      execution_id: this.performanceTraceId,
      flush_count: this.flushCount,
      follow_on_wait_count: this.followOnWaitCount,
      group_state_counts: Object.fromEntries(groupStateCounts),
      group_wait_reason_counts: Object.fromEntries(groupWaitReasonCounts),
      held_lock_count: this.heldLocks.length,
      held_participant_count: heldParticipantCount,
      last_follow_on_wait: this.lastFollowOnWait,
      park_count: this.parkCount,
      pending_lookup_count: this.pendingLookups.size,
      pending_physical_lock_count: this.pendingPhysicalLocks.size,
      ready_activation_count: this.readyActivations.length,
      resume_count: this.resumeCount,
      sample_active_groups: sampleActiveGroups,
      sample_group_waits: sampleGroupWaits,
      sample_pending_group_ids: Array.from(this.pendingLookups.keys()).slice(0, 5),
      sample_pending_physical_locks: samplePendingPhysicalLocks,
      ...extra,
    });
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
  const scope: BatchEntityCreateCoordinatorScope = {
    active: true,
    coordinator,
    groupId,
  };
  return batchEntityCreateCoordinatorStorage.run(scope, async () => {
    try {
      return await execute();
    } finally {
      scope.active = false;
      coordinator.completeGroup(groupId);
    }
  });
};

const getActiveBatchEntityCreateCoordinatorScope = (): BatchEntityCreateCoordinatorScope | undefined => {
  const scope = batchEntityCreateCoordinatorStorage.getStore();
  return scope?.active ? scope : undefined;
};

export const resolveBatchEntityCreateLookup = (
  input: BatchEntityCreateLookupInput,
): Promise<BatchEntityCreateLookupResolution> | undefined => {
  const scope = getActiveBatchEntityCreateCoordinatorScope();
  return scope?.coordinator.registerLookup(scope.groupId, input);
};

export const resolveBatchParticipantLock = (
  input: BatchParticipantLockInput,
): Promise<BatchEntityCreateLock> | undefined => {
  const scope = getActiveBatchEntityCreateCoordinatorScope();
  return scope?.coordinator.acquireParticipantLock(scope.groupId, input);
};

export const getBatchEntityCreateCoordinatorGroupId = (): number | undefined => {
  return getActiveBatchEntityCreateCoordinatorScope()?.groupId;
};

export const waitForBatchEntityCreateCoordinatorPromise = <T>(promise: Promise<T>): Promise<T> | undefined => {
  const scope = getActiveBatchEntityCreateCoordinatorScope();
  return scope?.coordinator.waitForPromise(scope.groupId, promise);
};

export const getBatchEntityCreateCoordinatorHeldParticipantIds = (draftId?: string): string[] => {
  const scope = getActiveBatchEntityCreateCoordinatorScope();
  return scope?.coordinator.getHeldParticipantIds(scope.groupId, draftId) ?? [];
};
