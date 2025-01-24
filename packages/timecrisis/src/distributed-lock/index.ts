import { JobStorage } from '../storage/types';

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
  node: string;

  /**
   * How long a lock is valid before needing renewal (in milliseconds)
   **/
  lockTTL: number;
}

export class DistributedLock {
  private opts: DistributedLockOptions;

  constructor(opts: DistributedLockOptions) {
    this.opts = opts;
  }

  /**
   * Try to acquire a lock with the given name.
   */
  async acquire(lockName: string): Promise<boolean> {
    try {
      return await this.opts.storage.acquireLock(lockName, this.opts.node, this.opts.lockTTL);
    } catch (error) {
      throw new DistributedLockError(
        error instanceof Error ? error.message : 'Failed to acquire lock'
      );
    }
  }

  /**
   * Renew an existing lock.
   */
  async renew(lockName: string): Promise<boolean> {
    try {
      return await this.opts.storage.renewLock(lockName, this.opts.node, this.opts.lockTTL);
    } catch (error) {
      throw new DistributedLockError(
        error instanceof Error ? error.message : 'Failed to renew lock'
      );
    }
  }

  /**
   * Release a lock.
   */
  async release(lockName: string): Promise<void> {
    await this.opts.storage.releaseLock(lockName, this.opts.node);
  }
}
