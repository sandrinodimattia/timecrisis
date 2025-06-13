import { Lock } from './lock.js';
import { Logger } from '../logger/index.js';
import { JobStorage } from '../storage/types.js';
import { DistributedLock } from './distributed-lock.js';

/**
 * Options for creating a LeaderElection instance.
 */
export interface LeaderElectionConfig {
  /**
   * Logger instance.
   */
  logger: Logger;

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
  private readonly LOCK_NAME = 'timecrisis/leader';
  private readonly cfg: LeaderElectionConfig;
  private readonly distributedLock: DistributedLock;

  private isRunning: boolean = false;
  private logger: Logger;
  private leaderLock?: Lock;
  private isLeader: boolean = false;
  private checkInterval?: NodeJS.Timeout;

  constructor(config: LeaderElectionConfig) {
    this.cfg = config;
    this.logger = config.logger.child('leader-election');
    this.distributedLock = new DistributedLock({
      worker: config.node,
      lockTTL: config.lockTTL,
      storage: config.storage,
    });
  }

  /**
   * Internal method to attempt leadership acquisition or renewal.
   * @returns Whether this node is currently leader after the attempt.
   */
  private async tryBecomeLeader(): Promise<boolean> {
    if (this.isRunning) {
      return false;
    }
    this.isRunning = true;

    // If we are the leader, we "were" the leader in this scope.
    const wasLeader = this.isLeader;
    if (!wasLeader) {
      this.logger.debug('Attempting to become leader');
    }

    try {
      // If we're already leader, try to renew instead of acquire
      if (wasLeader) {
        const renewed = await this.leaderLock!.renew();
        if (!renewed) {
          this.leaderLock = undefined;
          this.logger.debug('Leader lock expired, lost leadership');
        } else {
          this.logger.debug('Leader lock renewed');
        }
      } else {
        this.leaderLock = await this.distributedLock.acquire(this.LOCK_NAME);
        if (this.leaderLock) {
          this.logger.info('Acquired leader lock');
        }
      }

      // Handle state changes
      const succeeded = !!this.leaderLock;
      if (wasLeader && !succeeded) {
        // Lost leadership
        if (this.cfg.onLost) {
          await Promise.resolve(this.cfg.onLost());
        }
      } else if (!wasLeader && succeeded) {
        // Gained leadership
        if (this.cfg.onAcquired) {
          await Promise.resolve(this.cfg.onAcquired());
        }
      }

      this.isLeader = succeeded;
      return this.isLeader;
    } catch (err) {
      this.logger.error('Error acquiring leader lock', { err });

      // If we were leader or trying to become leader, handle the loss
      this.isLeader = false;
      if (wasLeader && this.cfg.onLost) {
        await Promise.resolve(this.cfg.onLost());
      }
      // Now we can throw
      throw err;
    } finally {
      this.isRunning = false;
    }
  }

  /**
   * Explicitly release leadership if we are leader.
   */
  private async releaseLeadership(): Promise<void> {
    const wasLeader = this.isLeader;

    try {
      if (wasLeader) {
        this.logger.debug('Releasing leader lock');

        await this.leaderLock!.release();
        this.isLeader = false;
        if (this.cfg.onLost) {
          await Promise.resolve(this.cfg.onLost());
        }

        this.logger.info('Leader lock released');
      }
    } catch (err) {
      this.logger.error('Error releasing leader lock', { err });

      // Even if the actual storage operation fails,
      // we consider ourselves no longer the leader.
      this.isLeader = false;
      if (wasLeader && this.cfg.onLost) {
        await Promise.resolve(this.cfg.onLost());
      }
      throw err;
    }
  }

  /**
   * Start the leader election loop. Immediately tries to become leader,
   * then periodically attempts to renew (acquire) the lock again.
   */
  async start(): Promise<void> {
    // Clear any existing interval.
    if (this.checkInterval) {
      clearInterval(this.checkInterval);
      this.checkInterval = undefined;
    }

    // Then schedule periodic renewals, but only if we're the leader
    const interval = Math.floor(this.cfg.lockTTL / 2);
    this.checkInterval = setInterval(async () => {
      await this.tryBecomeLeader();
    }, interval);

    this.logger.info('Leader election started', {
      interval,
    });

    // Initial attempt.
    await this.tryBecomeLeader();
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
