import { JobStorage } from '../storage/types.js';

export class DistributedLockError extends Error {
  constructor(message: string) {
    super(message);
    this.name = this.constructor.name;
    Error.captureStackTrace(this, this.constructor);
  }
}

export interface DistributedLockOptions {
  /**
   * Storage adapter for persisting locks
   **/
  storage: JobStorage;

  /**
   * Unique identifier for this node
   **/
  worker: string;

  /**
   * How long a lock is valid before needing renewal (in milliseconds)
   **/
  lockTTL: number;
}

export class Lock {
  constructor(
    private readonly opts: DistributedLockOptions,
    public readonly name: string,
    private readonly lockTTL: number
  ) {}

  /**
   * Release a lock.
   */
  async release(): Promise<void> {
    try {
      await this.opts.storage.releaseLock(this.name, this.opts.worker);
    } catch (error) {
      throw new DistributedLockError(
        error instanceof Error ? error.message : 'Failed to release lock'
      );
    }
  }

  /**
   * Renew an existing lock.
   */
  async renew(): Promise<boolean> {
    try {
      return await this.opts.storage.renewLock(
        this.name,
        this.opts.worker,
        this.lockTTL ?? this.opts.lockTTL
      );
    } catch (error) {
      throw new DistributedLockError(
        error instanceof Error ? error.message : 'Failed to renew lock'
      );
    }
  }
}

export class DistributedLock {
  private opts: DistributedLockOptions;

  constructor(opts: DistributedLockOptions) {
    this.opts = opts;
  }

  /**
   * Try to acquire a lock with the given name.
   * @param lockName Name of the lock to acquire
   * @param lockTTL How long the lock should be valid (in milliseconds)
   */
  async acquire(lockName: string, lockTTL?: number): Promise<Lock | undefined> {
    try {
      const acquired = await this.opts.storage.acquireLock(
        lockName,
        this.opts.worker,
        lockTTL ?? this.opts.lockTTL
      );
      if (acquired) {
        return new Lock(this.opts, lockName, lockTTL ?? this.opts.lockTTL);
      } else {
        return undefined;
      }
    } catch (error) {
      throw new DistributedLockError(
        error instanceof Error ? error.message : 'Failed to acquire lock'
      );
    }
  }

  /**
   * Wait for a lock to be available.
   * @param lockName Name of the lock to acquire
   * @param lockTTL How long the lock should be valid (in milliseconds)
   */
  async waitForLock(lockName: string, lockTTL?: number): Promise<Lock | undefined> {
    let retries = 0;

    while (retries < 4) {
      const lock = await this.acquire(lockName, lockTTL);
      if (lock) {
        return lock;
      }

      // Wait before retrying
      await this.delay(250);
      retries += 1;
    }

    return undefined;
  }

  /**
   * Execute the given function while holding a lock.
   * @param lockName Name of the lock to acquire
   * @param fn Function to execute while holding the lock
   */
  async execute(lockName: string, fn: () => Promise<void>): Promise<void> {
    const lock = await this.waitForLock(lockName);
    if (!lock) {
      throw new DistributedLockError('Failed to acquire lock');
    }

    try {
      await fn();
    } finally {
      await lock.release();
    }
  }

  /**
   * Utility function to delay execution.
   */
  private delay(ms: number): Promise<void> {
    return new Promise((resolve) => setTimeout(resolve, ms));
  }
}
