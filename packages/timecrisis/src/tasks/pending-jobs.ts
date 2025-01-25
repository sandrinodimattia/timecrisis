import { z } from 'zod';

import { Logger } from '../logger/index.js';
import { Job } from '../storage/schemas/index.js';
import { JobStorage } from '../storage/types.js';
import { JobContextImpl } from '../scheduler/context.js';
import { GlobalConcurrencyManager } from '../concurrency/global-concurrency.js';
import { JobContext, JobDefinition, JobDefinitionNotFoundError } from '../scheduler/types.js';

export interface PendingJobsConfig {
  /**
   * Logger.
   */
  logger: Logger;

  /**
   * Job definitions.
   */
  jobs: Map<string, JobDefinition>;

  /**
   * Storage backend.
   */
  storage: JobStorage;

  /**
   * Name of the worker that is performing the job.
   */
  worker: string;

  /**
   * Maximum number of concurrent jobs that can be running at once.
   */
  maxConcurrentJobs: number;

  /**
   * Lock lifetime in milliseconds.
   */
  lockLifetime: number;

  /**
   * Function to touch a job in the storage.
   * @param jobId
   * @returns
   */
  touchJob: (jobId: string) => Promise<void>;

  /**
   * Execute the job in fork mode.
   * @param jobDef Job definition
   * @param job Job
   * @param ctx Job context
   * @returns
   */
  executeForkMode: (jobDef: JobDefinition, job: Job, ctx: JobContext) => Promise<void>;
}

export class PendingJobsTask {
  private readonly cfg: PendingJobsConfig;
  private readonly logger: Logger;
  private readonly globalConcurrency: GlobalConcurrencyManager;

  constructor(cfg: PendingJobsConfig) {
    this.cfg = cfg;
    this.logger = cfg.logger.child('pending-jobs');
    this.globalConcurrency = new GlobalConcurrencyManager(this.logger, cfg);
  }

  /**
   * Get how many tasks are currently running.
   */
  public getRunningCount(): number {
    return this.globalConcurrency.getRunningCount();
  }

  /**
   * Process pending jobs.
   */
  public async execute(): Promise<void> {
    try {
      const now = new Date();

      // Get pending jobs.
      const pendingJobs = await this.cfg.storage.listJobs({
        status: ['pending'],
        runAtBefore: now,
        limit: this.cfg.maxConcurrentJobs * 2,
      });

      this.logger.debug('Processing pending jobs', {
        jobs: pendingJobs.length,
      });

      // Process valid to respect concurrency limits.
      const promises = pendingJobs
        .filter((job) => {
          const jobDef = this.cfg.jobs.get(job.type);
          if (!jobDef) {
            this.logger.warn('Invalid job type, skipping job', {
              jobId: job.id,
              type: job.type,
            });
            return false;
          }

          // Try to acquire global concurrency slot.
          if (!this.globalConcurrency.acquire(job.id)) {
            this.logger.debug('Failed to acquire global concurrency slot for job', {
              jobId: job.id,
              type: job.type,
            });
            return;
          }

          return true;
        })
        .map(async (job) => {
          try {
            const jobDef = this.cfg.jobs.get(job.type);
            const maxForType = jobDef!.concurrency ?? this.cfg.maxConcurrentJobs;

            // Check if concurrency slot is available for this job type.
            const jobTypeLock = await this.cfg.storage.acquireConcurrencySlot(job.type, maxForType);
            if (!jobTypeLock) {
              this.logger.debug('Failed to acquire concurrency slot (type limit) for job', {
                jobId: job.id,
                type: job.type,
              });
              return;
            }

            try {
              // Try to acquire lock for the job
              const locked = await this.tryAcquireLock(job);
              if (!locked) {
                this.logger.debug('Failed to acquire lock for job', {
                  jobId: job.id,
                  type: job.type,
                });
                await this.cfg.storage.releaseConcurrencySlot(job.type);
                return;
              }

              // Process the job
              await this.processJob(job);
            } catch (err) {
              this.logger.error('Error processing job', {
                jobId: job.id,
                type: job.type,
                error: err instanceof Error ? err.message : String(err),
                error_stack: err instanceof Error ? err.stack : undefined,
              });
            } finally {
              // Release concurrency slot regardless of job success or failure
              await this.cfg.storage.releaseConcurrencySlot(job.type);
            }
          } catch (err) {
            this.logger.error('Error processing job', {
              jobId: job.id,
              type: job.type,
              error: err instanceof Error ? err.message : String(err),
              error_stack: err instanceof Error ? err.stack : undefined,
            });
          } finally {
            // Release global concurrency slot
            this.globalConcurrency.release(job.id);
          }
        });

      // Wait for all jobs to complete
      await Promise.all(promises);
    } catch (err) {
      this.logger.error('Error processing pending jobs', {
        error: err instanceof Error ? err.message : String(err),
        error_stack: err instanceof Error ? err.stack : undefined,
      });
      throw err;
    }
  }

  /**
   * Process a job.
   */
  private async processJob(job: Job): Promise<void> {
    // Get the job definition.
    const jobDef = this.cfg.jobs.get(job.type);
    if (!jobDef) {
      throw new JobDefinitionNotFoundError(job.type);
    }

    const now = new Date();
    const attempt = job.attempts + 1;

    // Update job status to running and increment retry count.
    await this.cfg.storage.updateJob(job.id, {
      status: 'running',
      attempts: attempt,
      startedAt: now,
    });

    // Create a new job run.
    const jobRunId = await this.cfg.storage.createJobRun({
      jobId: job.id,
      status: 'running',
      startedAt: now,
      attempt,
    });

    this.logger.debug(`Processing job`, {
      jobId: job.id,
      jobRunId: jobRunId,
      type: job.type,
      attempt,
      maxAttempts: job.maxRetries,
    });

    // Create job context with touch function.
    const ctx = new JobContextImpl(
      this.cfg.storage,
      jobDef,
      job.id,
      jobRunId,
      attempt,
      job.maxRetries,
      job.data,
      async () => await this.cfg.touchJob(job.id)
    );

    const startTime = Date.now();
    try {
      if (jobDef.forkMode === true) {
        this.logger.debug('Executing job in fork mode', {
          jobId: job.id,
          type: job.type,
          forkHelperPath: jobDef.forkHelperPath,
        });
        await this.cfg.executeForkMode(jobDef, job, ctx);
      } else {
        this.logger.debug('Executing job in process', {
          jobId: job.id,
          type: job.type,
        });

        await jobDef.handle(job.data as typeof jobDef.schema, ctx);
      }

      const duration = Date.now() - startTime;

      // Mark success.
      await this.cfg.storage.updateJob(job.id, {
        status: 'completed',
        progress: 100,
        executionDuration: duration,
        lockedAt: null,
        lockedBy: null,
        finishedAt: new Date(),
      });

      // Mark success.
      await this.cfg.storage.updateJobRun(jobRunId, {
        status: 'completed',
        progress: 100,
        finishedAt: new Date(),
      });

      this.logger.info('Job completed successfully', {
        jobId: job.id,
        type: job.type,
        executionDuration: duration,
      });

      await ctx.log('info', `Job completed successfully`);
    } catch (error: unknown) {
      const durationMs = Date.now() - startTime;
      let errorMessage = error instanceof Error ? error.message : String(error);
      if (error instanceof z.ZodError) {
        const flat = error.errors.map((err) => `${err.message}`).join(',');
        errorMessage = `Zod validation error: ${flat}`;
      }

      this.logger.error('Job failed', {
        jobId: job.id,
        type: job.type,
        error: errorMessage,
        error_stack: error instanceof Error ? error.stack : undefined,
        executionDuration: durationMs,
      });

      // Update job run status.
      await this.cfg.storage.updateJobRun(jobRunId, {
        status: 'failed',
        error: errorMessage,
        finishedAt: new Date(),
      });

      // Handle job failure.
      if (attempt >= job.maxRetries) {
        // Move to dead letter queue.
        await this.cfg.storage.createDeadLetterJob({
          jobId: job.id,
          jobType: job.type,
          data: job.data,
          failedAt: new Date(),
          reason: errorMessage,
        });

        // Job failed permanently.
        await this.cfg.storage.updateJob(job.id, {
          status: 'failed',
          failReason: errorMessage,
          failCount: job.failCount + 1,
          lockedAt: null,
          lockedBy: null,
          executionDuration: durationMs,
        });

        this.logger.error('Job failed permanently', {
          jobId: job.id,
          type: job.type,
          error: errorMessage,
          error_stack: error instanceof Error ? error.stack : undefined,
          durationMs,
        });

        await ctx.log('error', `Job failed permanently after ${attempt} attempts: ${errorMessage}`);
      } else {
        // Calculate backoff delay.
        let delayMs = 10000;
        if (job.backoffStrategy === 'exponential') {
          delayMs = Math.min(attempt * delayMs, 24 * 60 * 60 * 1000); // Max 24 hours
        }

        this.logger.warn(
          `Scheduling retry for failed job in ${delayMs} ms (next attempt ${attempt + 1}/${job.maxRetries})`,
          {
            jobId: job.id,
            type: job.type,
            error: errorMessage,
            durationMs,
            delayMs,
          }
        );

        const nextRun = new Date(Date.now() + delayMs);
        await this.cfg.storage.updateJob(job.id, {
          status: 'pending',
          failReason: errorMessage,
          failCount: job.failCount + 1,
          lockedAt: null,
          lockedBy: null,
          executionDuration: durationMs,
          runAt: nextRun,
        });

        await ctx.log('warn', `Job failed, retrying in ${delayMs}ms: ${errorMessage}`);
      }

      throw error;
    }
  }

  /**
   * Try to acquire a lock for a job.
   */
  private async tryAcquireLock(job: Job): Promise<boolean> {
    const now = new Date();
    const lockLifetime = this.cfg.lockLifetime;

    // Check if job is already locked.
    if (job.lockedAt) {
      const lockAge = now.getTime() - job.lockedAt.getTime();
      if (lockAge < lockLifetime) {
        this.logger.debug('Job is already locked', {
          jobId: job.id,
          type: job.type,
          lockAge,
          lockLifetime,
        });
        return false;
      }
    }

    try {
      await this.cfg.storage.updateJob(job.id, {
        lockedAt: now,
        lockedBy: this.cfg.worker,
      });

      return true;
    } catch (err) {
      this.logger.error(`Failed to acquire lock for job`, {
        jobId: job.id,
        error: err instanceof Error ? err.message : String(err),
        error_stack: err instanceof Error ? err.stack : undefined,
      });
      return false;
    }
  }
}
