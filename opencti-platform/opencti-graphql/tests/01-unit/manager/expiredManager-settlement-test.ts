import { beforeEach, describe, expect, it, vi } from 'vitest';

const mocks = vi.hoisted(() => ({
  elList: vi.fn(),
  executionContext: vi.fn(() => ({})),
  lockResources: vi.fn(),
  logApp: {
    debug: vi.fn(),
    error: vi.fn(),
    info: vi.fn(),
  },
  patchAttribute: vi.fn(),
  unlock: vi.fn(),
  userEditField: vi.fn(),
}));

vi.mock('../../../src/lock/master-lock', () => ({
  lockResources: mocks.lockResources,
}));

vi.mock('../../../src/database/engine', () => ({
  elList: mocks.elList,
  ES_MAX_CONCURRENCY: 2,
}));

vi.mock('../../../src/database/middleware', () => ({
  patchAttribute: mocks.patchAttribute,
}));

vi.mock('../../../src/domain/user', () => ({
  userEditField: mocks.userEditField,
}));

vi.mock('../../../src/utils/access', () => ({
  executionContext: mocks.executionContext,
  EXPIRATION_MANAGER_USER: { id: 'expiration-manager' },
}));

vi.mock('../../../src/config/conf', async (importOriginal) => {
  const actual = await importOriginal<typeof import('../../../src/config/conf')>();
  return {
    ...actual,
    logApp: mocks.logApp,
  };
});

const { expireHandler, revokedInstances } = await import('../../../src/manager/expiredManager');

const deferred = <T>() => {
  let resolve: (value: T) => void = () => {};
  let reject: (reason?: unknown) => void = () => {};
  const promise = new Promise<T>((promiseResolve, promiseReject) => {
    resolve = promiseResolve;
    reject = promiseReject;
  });
  return { promise, reject, resolve };
};

describe('expiration manager settlement', () => {
  beforeEach(() => {
    vi.clearAllMocks();
    mocks.lockResources.mockResolvedValue({ unlock: mocks.unlock });
  });

  it('keeps the manager lock until sibling branches settle after a rejection', async () => {
    const rejectedBranch = deferred<void>();
    const siblingBranch = deferred<void>();
    const error = new Error('revocation failed');

    mocks.elList
      .mockImplementationOnce(() => rejectedBranch.promise)
      .mockImplementationOnce(() => siblingBranch.promise);

    const running = expireHandler();
    rejectedBranch.reject(error);
    await Promise.resolve();
    await Promise.resolve();

    expect(mocks.elList).toHaveBeenCalledTimes(2);
    expect(mocks.unlock).not.toHaveBeenCalled();

    siblingBranch.resolve();
    await running;

    expect(mocks.unlock).toHaveBeenCalledTimes(1);
    expect(mocks.logApp.error).toHaveBeenCalledWith(
      '[OPENCTI-MODULE] Expiration manager handling error',
      { cause: error, manager: 'EXPIRATION_SCHEDULER' },
    );
  });

  it('waits for already-started revocation updates before surfacing a mapper failure', async () => {
    const firstUpdate = deferred<void>();
    const secondUpdate = deferred<void>();
    const error = new Error('patch failed');
    let rejected = false;

    mocks.elList.mockImplementation(async (_context, _user, _indices, opts) => {
      await opts.callback([
        { entity_type: 'Malware', id: 'entity-1' },
        { entity_type: 'Malware', id: 'entity-2' },
        { entity_type: 'Malware', id: 'entity-3' },
      ]);
    });
    mocks.patchAttribute
      .mockImplementationOnce(() => firstUpdate.promise)
      .mockImplementationOnce(() => secondUpdate.promise)
      .mockResolvedValue(undefined);

    const running = revokedInstances({} as any).catch((reason) => {
      rejected = true;
      throw reason;
    });
    await Promise.resolve();
    await Promise.resolve();
    expect(mocks.patchAttribute).toHaveBeenCalledTimes(2);

    firstUpdate.reject(error);
    await Promise.resolve();
    await Promise.resolve();
    expect(rejected).toBe(false);
    expect(mocks.patchAttribute).toHaveBeenCalledTimes(2);

    secondUpdate.resolve();
    await expect(running).rejects.toBe(error);
    expect(mocks.patchAttribute).toHaveBeenCalledTimes(2);
  });
});
