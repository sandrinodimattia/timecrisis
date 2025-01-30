import { Lock } from './lock.js';
import { JobStorage } from '../storage/types.js';

export class DistributedLockError extends Error {
  constructor(message: string) {
    super(message);
    this.name = this.constructor.name;
    Error.captureStackTrace(this, this.constructor);
  }
}

export interface DistributedLockConfig {
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

export class DistributedLock {
  private cfg: DistributedLockConfig;

  constructor(config: DistributedLockConfig) {
    this.cfg = config;
  }

  /**
   * Try to acquire a lock with the given name.
   * @param lockName Name of the lock to acquire
   * @param lockTTL How long the lock should be valid (in milliseconds)
   */
  async acquire(lockName: string, lockTTL?: number): Promise<Lock | undefined> {
    try {
      const acquired = await this.cfg.storage.acquireLock(
        lockName,
        this.cfg.worker,
        lockTTL ?? this.cfg.lockTTL
      );
      if (acquired) {
        return new Lock(lockName, lockTTL ?? this.cfg.lockTTL, this.cfg);
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
