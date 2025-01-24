import { z } from 'zod';

import { Logger } from '../logger/index.js';
import { Job } from '../storage/schemas/index.js';
import { JobStorage } from '../storage/types.js';
import { JobContextImpl } from '../scheduler/context.js';
import { JobContext, JobDefinition, JobDefinitionNotFoundError } from '../scheduler/types.js';

export interface PendingJobsConfig {
  /**
   * Maximum number of concurrent jobs that can be running at once
   * Default: 20
   */
  maxConcurrentJobs?: number;

  /**
   * Lock lifetime in milliseconds
   * Default: 5 minutes
   */
  lockLifetime?: number;
}

export class PendingJobsTask {
  private runningCount = 0;
  private executionCounter: Record<string, number> = {};
  private isExecuting: boolean = false;
  private logger: Logger;
  private maxConcurrentJobs: number;

  constructor(
    private readonly storage: JobStorage,
    private readonly jobs: Map<string, JobDefinition>,
    private readonly executeForkMode: (
      jobDef: JobDefinition,
      job: Job,
      ctx: JobContext
    ) => Promise<void>,
    private readonly touchJob: (jobId: string) => Promise<void>,
    logger: Logger,
    private readonly opts: PendingJobsConfig = {}
  ) {
    this.logger = logger.child('pending-jobs');
    this.maxConcurrentJobs = opts.maxConcurrentJobs ?? 20;
  }

  /**
   * Get how many tasks are currently running.
   */
  public getRunningCount(): number {
    return this.runningCount;
  }

  /**
   * Process pending jobs.
   */
  public async execute(): Promise<void> {
    // Skip if already running.
    if (this.isExecuting) {
      return;
    } else {
      this.isExecuting = true;
    }

    try {
      const now = new Date();

      // Get pending jobs.
      const pendingJobs = await this.storage.listJobs({
        status: ['pending'],
        limit: this.maxConcurrentJobs,
        runAtBefore: now,
      });

      this.logger.debug('Processing pending jobs', {
        jobs: pendingJobs.length,
        maxConcurrentJobs: this.maxConcurrentJobs,
      });

      // Process each pending job.
      const promises = pendingJobs.map(async (job) => {
        try {
          // Skip if we've hit global concurrency limit.
          if (this.runningCount >= this.maxConcurrentJobs) {
            this.logger.debug('Skipping job due to global concurrency limit', {
              jobId: job.id,
              runningCount: this.runningCount,
              maxConcurrentJobs: this.maxConcurrentJobs,
            });

            return;
          }

          // Skip jobs that we don't support anymore.
          const jobDef = this.jobs.get(job.type);
          if (!jobDef) {
            this.logger.warn('Unknown job type', {
              jobId: job.id,
              type: job.type,
            });

            await this.handleInvalidJob(job);
            return;
          }

          // Skip if we've hit concurrency limit for this job type.
          const currentConcurrency = this.executionCounter[job.type] ?? 0;
          if (currentConcurrency >= (jobDef.concurrency ?? 20)) {
            this.logger.debug('Skipping job due to type concurrency limit', {
              jobId: job.id,
              type: job.type,
              currentConcurrency,
              maxConcurrency: jobDef.concurrency ?? 20,
            });
            return;
          }

          // Increment concurrency counters before acquiring lock
          this.runningCount++;
          this.executionCounter[job.type] = (this.executionCounter[job.type] || 0) + 1;

          // Try to acquire lock for the job
          const locked = await this.tryAcquireLock(job);
          if (!locked) {
            this.logger.debug('Failed to acquire lock for job', {
              jobId: job.id,
              type: job.type,
            });
            return;
          }

          // Process the job.
          await this.processJob(job);
        } catch (err) {
          this.logger.error('Error processing job', {
            jobId: job.id,
            type: job.type,
            error: err instanceof Error ? err.message : String(err),
            error_stack: err instanceof Error ? err.stack : undefined,
          });
        } finally {
          // Decrement concurrency counters
          this.runningCount = Math.max(0, this.runningCount - 1);
          this.executionCounter[job.type] = Math.max(0, (this.executionCounter[job.type] || 0) - 1);
        }
      });

      await Promise.allSettled(promises);
    } catch (err) {
      this.logger.error('Error processing pending jobs', {
        error: err instanceof Error ? err.message : String(err),
        error_stack: err instanceof Error ? err.stack : undefined,
      });
      throw err;
    } finally {
      this.isExecuting = false;
    }
  }

  /**
   * Process a job
   */
  private async processJob(job: Job): Promise<void> {
    // Get the job definition.
    const jobDef = this.jobs.get(job.type);
    if (!jobDef) {
      throw new JobDefinitionNotFoundError(job.type);
    }

    const attempt = job.attempts + 1;

    // Update job status to running and increment retry count
    await this.storage.updateJob(job.id, {
      status: 'running',
      attempts: attempt,
    });

    // Create a new job run
    const jobRunId = await this.storage.createJobRun({
      jobId: job.id,
      status: 'running',
      startedAt: new Date(),
      attempt,
    });

    this.logger.debug(`Processing job`, {
      jobId: job.id,
      jobRunId: jobRunId,
      type: job.type,
      attempt,
      maxAttempts: job.maxRetries,
    });

    // Create job context with touch function
    const ctx = new JobContextImpl(
      this.storage,
      jobDef,
      job.id,
      jobRunId,
      attempt,
      job.maxRetries,
      job.data,
      async () => await this.touchJob(job.id)
    );

    const startTime = Date.now();
    try {
      if (jobDef.forkMode === true) {
        this.logger.debug('Executing job in fork mode', {
          jobId: job.id,
          type: job.type,
          forkHelperPath: jobDef.forkHelperPath,
        });
        await this.executeForkMode(jobDef, job, ctx);
      } else {
        this.logger.debug('Executing job in process', {
          jobId: job.id,
          type: job.type,
        });

        await jobDef.handle(job.data as typeof jobDef.schema, ctx);
      }

      // Mark success
      const durationMs = Date.now() - startTime;
      await this.storage.updateJob(job.id, {
        status: 'completed',
        progress: 100,
        executionDuration: durationMs,
        lockedAt: null,
      });

      // Create success result
      await this.storage.updateJobRun(jobRunId, {
        status: 'completed',
        progress: 100,
        finishedAt: new Date(),
      });

      this.logger.info('Job completed successfully', {
        jobId: job.id,
        type: job.type,
        durationMs,
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
        durationMs,
      });

      // Update job run status
      await this.storage.updateJobRun(jobRunId, {
        status: 'failed',
        finishedAt: new Date(),
        error: errorMessage,
      });

      // Handle job failure
      if (attempt >= job.maxRetries) {
        // Move to dead letter queue
        await this.storage.createDeadLetterJob({
          jobId: job.id,
          jobType: job.type,
          data: job.data,
          failedAt: new Date(),
          reason: errorMessage,
        });

        await this.storage.updateJob(job.id, {
          status: 'failed',
          failReason: errorMessage,
          failCount: job.failCount + 1,
          lockedAt: null,
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
        // Calculate backoff delay
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
        await this.storage.updateJob(job.id, {
          status: 'pending',
          failReason: errorMessage,
          failCount: job.failCount + 1,
          lockedAt: null,
          executionDuration: durationMs,
          runAt: nextRun,
        });

        await ctx.log('warn', `Job failed, retrying in ${delayMs}ms: ${errorMessage}`);
      }

      throw error;
    }
  }

  /**
   * Try to acquire a lock for a job
   */
  private async tryAcquireLock(job: Job): Promise<boolean> {
    const now = new Date();
    const lockLifetime = this.opts.lockLifetime ?? 5 * 60 * 1000; // 5 minutes default

    // Check if job is already locked
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
      await this.storage.updateJob(job.id, {
        lockedAt: now,
      });
      this.logger.debug('Lock acquired for job', {
        jobId: job.id,
        type: job.type,
      });
      return true;
    } catch (err) {
      this.logger.debug('Failed to acquire lock for job', {
        jobId: job.id,
        type: job.type,
        error: err instanceof Error ? err.message : String(err),
        error_stack: err instanceof Error ? err.stack : undefined,
      });
      return false;
    }
  }

  /**
   * Handle invalid job types by moving them to dead letter queue
   */
  private async handleInvalidJob(job: Job): Promise<void> {
    await this.storage.createDeadLetterJob({
      jobId: job.id,
      jobType: job.type,
      data: job.data,
      failedAt: new Date(),
      reason: `Unknown job type: ${job.type}`,
    });

    await this.storage.updateJob(job.id, {
      status: 'failed',
      failReason: `Unknown job type: ${job.type}`,
      failCount: job.failCount + 1,
      lockedAt: null,
    });

    this.logger.warn('Moved invalid job to dead letter queue', {
      jobId: job.id,
      type: job.type,
    });
  }
}
