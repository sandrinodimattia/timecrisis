import { Logger } from '../logger/index.js';
import { JobStorage } from '../storage/types.js';
import { LeaderElection } from '../leader/index.js';

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

    try {
      // Check running jobs
      const jobs = await this.cfg.storage.listJobs({ status: ['pending', 'running'] });

      this.logger.debug('Checking for expired jobs', {
        runningJobs: jobs.length,
      });

      const now = new Date();
      for (const job of jobs) {
        try {
          if (job.lockedAt) {
            // Check lock expiration
            const lockAge = now.getTime() - job.lockedAt.getTime();
            if (lockAge > this.cfg.jobLockTTL) {
              this.logger.warn('Job lock expired', {
                jobId: job.id,
                type: job.type,
                lockAge,
                lockLifetime: this.cfg.jobLockTTL,
              });

              // Get the current job run
              const runs = await this.cfg.storage.listJobRuns(job.id);
              const currentRun = runs.find((r) => r.status === 'running');

              // Update the job run if it exists
              if (currentRun) {
                await this.cfg.storage.updateJobRun(currentRun.id, {
                  status: 'failed',
                  error: 'Job lock expired',
                  finishedAt: now,
                });

                this.logger.debug('Setting job run to failed due to lock expiration', {
                  jobId: job.id,
                  runId: currentRun.id,
                  status: 'failed',
                });
              }

              // Check if we should retry.
              if (job.attempts < job.maxRetries) {
                // Reset the job to pending for retry.
                await this.cfg.storage.updateJob(job.id, {
                  status: 'pending',
                  failReason: 'Job lock expired',
                  failCount: job.failCount + 1,
                  lockedAt: null,
                });

                this.logger.info('Job reset for retry after lock expiration', {
                  jobId: job.id,
                  type: job.type,
                  attempt: job.attempts + 1,
                  maxRetries: job.maxRetries,
                });
              } else {
                // Mark as failed if we've exceeded retries.
                await this.cfg.storage.updateJob(job.id, {
                  status: 'failed',
                  failReason: 'Job lock expired',
                  failCount: job.failCount + 1,
                  lockedAt: null,
                });

                this.logger.warn('Job failed permanently due to lock expiration', {
                  jobId: job.id,
                  type: job.type,
                  attempts: job.attempts,
                  maxRetries: job.maxRetries,
                });
              }
            }
          }

          // Check job expiration - no retry for expired jobs.
          if (job.expiresAt && job.expiresAt < now) {
            this.logger.warn('Job expired', {
              jobId: job.id,
              type: job.type,
              expiresAt: job.expiresAt,
            });

            // Get the current job run
            const runs = await this.cfg.storage.listJobRuns(job.id);
            const currentRun = runs.find((r) => r.status === 'running');

            // Update the job run if it exists.
            if (currentRun) {
              await this.cfg.storage.updateJobRun(currentRun.id, {
                status: 'failed',
                error: 'Job expired',
                finishedAt: now,
              });
              this.logger.debug('Updated job run status to failed due to job expiration', {
                jobId: job.id,
                runId: currentRun.id,
                status: 'failed',
              });
            }

            // Mark as failed immediately without retry.
            await this.cfg.storage.updateJob(job.id, {
              status: 'failed',
              failReason: 'Job expired',
              failCount: job.failCount + 1,
              lockedAt: null,
            });
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
