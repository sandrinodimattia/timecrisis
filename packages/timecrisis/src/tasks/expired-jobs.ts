import { Logger } from '../logger/index.js';
import { JobStorage } from '../storage/types.js';
import { LeaderElection } from '../leader/index.js';

export class ExpiredJobsTask {
  private storage: JobStorage;
  private leaderElection: LeaderElection;
  private logger: Logger;
  private opts: { lockLifetime: number };

  constructor(
    storage: JobStorage,
    leaderElection: LeaderElection,
    logger: Logger,
    opts: { lockLifetime: number }
  ) {
    this.storage = storage;
    this.leaderElection = leaderElection;
    this.logger = logger.child('expired-jobs');
    this.opts = opts;
  }

  /**
   * Check for expired jobs and mark them as failed
   */
  public async execute(): Promise<void> {
    // This only runs on the leader.
    if (!this.leaderElection.isCurrentLeader()) {
      return;
    }

    const now = new Date();

    // Check running jobs
    const jobs = await this.storage.listJobs({ status: ['pending', 'running'] });

    this.logger.debug('Checking for expired jobs', {
      runningJobs: jobs.length,
    });

    try {
      for (const job of jobs) {
        try {
          if (job.lockedAt) {
            // Check lock expiration
            const lockAge = now.getTime() - job.lockedAt.getTime();
            if (lockAge > this.opts.lockLifetime!) {
              this.logger.warn('Job lock expired', {
                jobId: job.id,
                type: job.type,
                lockAge,
                lockLifetime: this.opts.lockLifetime,
              });

              // Get the current job run
              const runs = await this.storage.listJobRuns(job.id);
              const currentRun = runs.find((r) => r.status === 'running');

              // Update the job run if it exists
              if (currentRun) {
                await this.storage.updateJobRun(currentRun.id, {
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

              // Check if we should retry
              if (job.attempts < job.maxRetries) {
                // Reset the job to pending for retry
                await this.storage.updateJob(job.id, {
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
                // Mark as failed if we've exceeded retries
                await this.storage.updateJob(job.id, {
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
              continue;
            }
          }

          // Check job expiration - no retry for expired jobs
          if (job.expiresAt && job.expiresAt < now) {
            this.logger.warn('Job expired', {
              jobId: job.id,
              type: job.type,
              expiresAt: job.expiresAt,
            });

            // Get the current job run
            const runs = await this.storage.listJobRuns(job.id);
            const currentRun = runs.find((r) => r.status === 'running');

            // Update the job run if it exists
            if (currentRun) {
              await this.storage.updateJobRun(currentRun.id, {
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

            // Mark as failed immediately without retry
            await this.storage.updateJob(job.id, {
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
