import { JobStorage } from '../storage/types.js';
import { DistributedLock, Lock } from './distributed-lock.js';

/**
 * Options for creating a LeaderElection instance.
 */
export interface LeaderOptions {
  /**
   * Storage adapter for persisting leader locks
   **/
  storage: JobStorage;

  /**
   * Unique identifier for this node
   **/
  node: string;

  /**
   * How long a leader lock is valid before needing renewal (ms)
   **/
  lockTTL: number;

  /**
   * Called whenever this node *acquires* leadership
   */
  onAcquired?: () => void | Promise<void>;

  /**
   * Called whenever this node *loses* leadership
   */
  onLost?: () => void | Promise<void>;
}

/**
 * LeaderElection orchestrates repeated attempts to acquire/renew
 * a distributed lock. If a node successfully acquires the lock,
 * it is the leader. If it fails to renew or another node takes
 * over, it loses leadership.
 */
export class LeaderElection {
  private readonly LOCK_NAME = 'chronotrigger/leader';
  private readonly opts: LeaderOptions;
  private readonly distributedLock: DistributedLock;

  private lock?: Lock;
  private isLeader: boolean = false;
  private checkInterval?: NodeJS.Timeout;

  constructor(opts: LeaderOptions) {
    this.opts = opts;
    this.distributedLock = new DistributedLock({
      worker: opts.node,
      lockTTL: opts.lockTTL,
      storage: opts.storage,
    });
  }

  /**
   * Internal method to attempt leadership acquisition or renewal.
   * @returns Whether this node is currently leader after the attempt.
   */
  private async tryBecomeLeader(): Promise<boolean> {
    const wasLeader = this.isLeader;

    try {
      // If we're already leader, try to renew instead of acquire
      if (wasLeader) {
        const renewed = await this.lock!.renew();
        if (!renewed) {
          this.lock = undefined;
        }
      } else {
        this.lock = await this.distributedLock.acquire(this.LOCK_NAME);
      }

      const succeeded = !!this.lock;
      this.isLeader = succeeded;

      // Handle state changes
      if (wasLeader && !succeeded) {
        // Lost leadership
        if (this.opts.onLost) {
          await Promise.resolve(this.opts.onLost());
        }
      } else if (!wasLeader && succeeded) {
        // Gained leadership
        if (this.opts.onAcquired) {
          await Promise.resolve(this.opts.onAcquired());
        }
      }

      return this.isLeader;
    } catch (err) {
      // If we were leader or trying to become leader, handle the loss
      this.isLeader = false;
      if (wasLeader && this.opts.onLost) {
        await Promise.resolve(this.opts.onLost());
      }
      // Now we can throw
      throw err;
    }
  }

  /**
   * Explicitly release leadership if we are leader.
   */
  private async releaseLeadership(): Promise<void> {
    const wasLeader = this.isLeader;

    try {
      if (wasLeader) {
        await this.lock!.release();
        this.isLeader = false;
        if (this.opts.onLost) {
          await Promise.resolve(this.opts.onLost());
        }
      }
    } catch (err) {
      // Even if the actual storage operation fails,
      // we consider ourselves no longer the leader.
      this.isLeader = false;
      if (wasLeader && this.opts.onLost) {
        await Promise.resolve(this.opts.onLost());
      }
      throw err;
    }
  }

  /**
   * Start the leader election loop. Immediately tries to become leader,
   * then periodically attempts to renew (acquire) the lock again.
   */
  async start(): Promise<void> {
    // Initial attempt.
    await this.tryBecomeLeader();

    // Clear any existing interval.
    if (this.checkInterval) {
      clearInterval(this.checkInterval);
      this.checkInterval = undefined;
    }

    // Then schedule periodic renewals, but only if we're the leader
    const interval = Math.floor(this.opts.lockTTL / 2);
    this.checkInterval = setInterval(async () => {
      await this.tryBecomeLeader();
    }, interval);
  }

  /**
   * Stop the leader election loop. Clears the interval and releases the lock.
   */
  async stop(): Promise<void> {
    if (this.checkInterval) {
      clearInterval(this.checkInterval);
      this.checkInterval = undefined;
    }
    await this.releaseLeadership();
  }

  /**
   * Returns whether this node is currently the leader.
   */
  isCurrentLeader(): boolean {
    return this.isLeader;
  }
}
