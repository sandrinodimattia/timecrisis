import { Logger } from '../logger/index.js';
import { JobStorage } from '../storage/types.js';
import { JobStateMachine } from '../state-machine/index.js';
import { LeaderElection } from '../concurrency/leader-election.js';
import { getJobId, isJobLock } from '../concurrency/job-lock.js';
import { JobExpiredError, JobLockExpiredError } from '../scheduler/types.js';

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
      // Get only expired locks from the DB
      const expiredLocks = await this.cfg.storage.listLocks({ expiredBefore: now });

      this.logger.debug('Checking for expired locks', {
        locks: expiredLocks.length,
      });

      // Look for jobs that are locked beyond their TTL.
      for (const lock of expiredLocks) {
        try {
          if (isJobLock(lock.lockId)) {
            // Fail the job.
            const job = await this.cfg.storage.getJob(getJobId(lock.lockId)!);
            if (job) {
              const err = new JobLockExpiredError(
                `Job "${job.id}" lock expired at ${lock.expiresAt.toISOString()}`
              );
              await this.cfg.stateMachine.fail(job, undefined, true, err, err.message, err.stack);
            }

            // Delete the lock.
            await this.cfg.storage.releaseLock(lock.lockId, lock.worker);
          }
        } catch (err) {
          this.logger.error('Error processing locked job', {
            job_id: getJobId(lock.lockId),
            error: err instanceof Error ? err.message : String(err),
            error_stack: err instanceof Error ? err.stack : undefined,
          });
        }
      }

      // We only need to check pending jobs, running jobs will be handled by the dead workers task.
      const expiredJobs = await this.cfg.storage.listJobs({
        status: ['pending'],
        expiresAtBefore: now,
      });

      this.logger.debug('Checking for expired jobs', {
        jobs: expiredJobs.length,
      });

      // Check jobs which might be expired.
      for (const job of expiredJobs) {
        try {
          // Fail the job.
          const err = new JobExpiredError(
            `Job "${job.id}" expired at ${job.expiresAt!.toISOString()}`
          );
          await this.cfg.stateMachine.fail(job, undefined, false, err, err.message, err.stack);
        } catch (err) {
          this.logger.error('Error processing expired job', {
            job_id: job.id,
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
