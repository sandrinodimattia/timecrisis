import { DistributedLockError, DistributedLockConfig } from './distributed-lock.js';

/**
 * A lock acquired by a node.
 */
export class Lock {
  public name: string;
  private lockTTL: number;
  private opts: DistributedLockConfig;

  constructor(name: string, lockTTL: number, opts: DistributedLockConfig) {
    this.name = name;
    this.lockTTL = lockTTL;
    this.opts = opts;
  }

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
