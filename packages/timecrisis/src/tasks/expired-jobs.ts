import { Logger } from '../logger/index.js';
import { JobStorage } from '../storage/types.js';
import { JobStateMachine } from '../state-machine/index.js';
import { LeaderElection } from '../concurrency/leader-election.js';
import { getJobId, isJobLock } from '../concurrency/job-lock.js';

interface ExpiredJobsTaskConfig {
  /**
   * Logger.
   */
  logger: Logger;

  /**
   * Storage backend.
   */
  storage: JobStorage;

  /**
   * State machine.
   */
  stateMachine: JobStateMachine;

  /**
   * Leader election mechanism to determine the current leader.
   */
  leaderElection: LeaderElection;

  /**
   * The time a job can remain locked after it was created, without receiving an update.
   */
  jobLockTTL: number;

  /**
   * Interval at which expired jobs are checked.
   */
  pollInterval: number;
}

export class ExpiredJobsTask {
  private timer: NodeJS.Timeout | null = null;

  private cfg: ExpiredJobsTaskConfig;
  private logger: Logger;

  constructor(config: ExpiredJobsTaskConfig) {
    this.cfg = config;
    this.logger = config.logger.child('expired-jobs');
  }

  /**
   * Start the expired jobs task.
   * This will begin checking for expired jobs and jobs with a lock that has expired.
   */
  async start(): Promise<void> {
    // Execute immediately on start
    try {
      await this.execute();
    } catch (err) {
      this.cfg.logger.error(`Failed to execute expired jobs check`, {
        error: err instanceof Error ? err.message : String(err),
        error_stack: err instanceof Error ? err.stack : undefined,
      });
    }

    // Start the check timer
    this.timer = setInterval(async () => {
      try {
        await this.execute();
      } catch (err) {
        this.cfg.logger.error(`Failed to execute expired jobs check`, {
          error: err instanceof Error ? err.message : String(err),
          error_stack: err instanceof Error ? err.stack : undefined,
        });
      }
    }, this.cfg.pollInterval);
  }

  /**
   * Stop the expired jobs task.
   */
  stop(): void {
    if (this.timer) {
      clearInterval(this.timer);
      this.timer = null;
    }
  }

  /**
   * Check for expired jobs and mark them as failed
   */
  public async execute(): Promise<void> {
    // Only run this task if we are the leader
    if (!this.cfg.leaderElection.isCurrentLeader()) {
      return;
    }

    const now = new Date();

    try {
      const locks = await this.cfg.storage.listLocks();

      this.logger.debug('Checking for expired locks', {
        locks: locks.length,
      });

      // Look for jobs that are locked beyond their TTL.
      for (const lock of locks) {
        try {
          if (lock.expiresAt.getTime() < now.getTime()) {
            if (isJobLock(lock.lockId)) {
              // Fail the job.
              const job = await this.cfg.storage.getJob(getJobId(lock.lockId)!);
              if (job) {
                await this.cfg.stateMachine.fail(
                  job,
                  undefined,
                  true,
                  `Job lock expired (expiresAt=${lock.expiresAt.toISOString()})`
                );
              }

              // Delete the lock.
              await this.cfg.storage.releaseLock(lock.lockId, lock.worker);
            }
          }
        } catch (err) {
          this.logger.error('Error processing locked job', {
            jobId: getJobId(lock.lockId),
            error: err instanceof Error ? err.message : String(err),
            error_stack: err instanceof Error ? err.stack : undefined,
          });
        }
      }

      const jobs = await this.cfg.storage.listJobs({ status: ['pending'] });

      this.logger.debug('Checking for expired jobs', {
        jobs: jobs.length,
      });

      // Check jobs which might be expired.
      for (const job of jobs) {
        try {
          // Check job expiration - no retry for expired jobs.
          if (job.expiresAt && job.expiresAt < now) {
            // Fail the job.
            await this.cfg.stateMachine.fail(
              job,
              undefined,
              false,
              `Job expired (expiresAt=${job.expiresAt.toISOString()})`
            );
          }
        } catch (err) {
          this.logger.error('Error processing expired job', {
            jobId: job.id,
            error: err instanceof Error ? err.message : String(err),
            error_stack: err instanceof Error ? err.stack : undefined,
          });
        }
      }
    } catch (err) {
      this.logger.error('Error checking expired jobs', {
        error: err instanceof Error ? err.message : String(err),
        error_stack: err instanceof Error ? err.stack : undefined,
      });
      throw err;
    }
  }
}
