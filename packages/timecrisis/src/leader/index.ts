import { JobStorage } from '../storage/types.js';
import { DistributedLock } from '../distributed-lock/index.js';

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
  private readonly LOCK_NAME = 'SCHEDULER_LEADER';
  private opts: LeaderOptions;
  private lock: DistributedLock;
  private isLeader: boolean = false;
  private checkInterval?: NodeJS.Timeout;

  constructor(opts: LeaderOptions) {
    this.opts = opts;
    this.lock = new DistributedLock({
      node: opts.node,
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
      const succeeded = wasLeader
        ? await this.lock.renew(this.LOCK_NAME)
        : await this.lock.acquire(this.LOCK_NAME);

      // First update our state
      const hadStateChange = succeeded !== wasLeader;
      this.isLeader = succeeded;

      // Then handle callbacks if there was a real state change
      if (hadStateChange) {
        if (succeeded) {
          // We just gained leadership
          if (this.opts.onAcquired) {
            await Promise.resolve(this.opts.onAcquired());
          }
        } else {
          // We just lost leadership
          if (this.opts.onLost) {
            await Promise.resolve(this.opts.onLost());
          }
        }
      }

      return this.isLeader;
    } catch (err) {
      // If we were leader and an error occurred, we must assume we lost leadership
      if (wasLeader) {
        this.isLeader = false;
        if (this.opts.onLost) {
          await Promise.resolve(this.opts.onLost());
        }
      }
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
        await this.lock.release(this.LOCK_NAME);
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
    if (this.checkInterval) {
      clearInterval(this.checkInterval);
      this.checkInterval = undefined;
    }

    // Initial attempt
    await this.tryBecomeLeader();

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
