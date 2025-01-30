import { Logger } from '../../logger/index.js';
import { PendingJobsContext } from './types.js';
import { createJobPipeline } from './middlewares.js';
import { Job } from '../../storage/schemas/index.js';
import { JobDefinition } from '../../scheduler/types.js';
import { JobContextImpl } from '../../scheduler/context.js';

export class PendingJobsTask {
  private timer: NodeJS.Timeout | null = null;
  private readonly shutdownState = { isShuttingDown: false };
  private readonly shutdownRef = new WeakRef(this.shutdownState);

  private readonly ctx: PendingJobsContext;
  private readonly logger: Logger;

  private readonly processJob: (
    taskCtx: PendingJobsContext,
    job: Job,
    jobDef: JobDefinition
  ) => Promise<void>;

  constructor(ctx: PendingJobsContext) {
    this.ctx = ctx;
    this.logger = ctx.logger.child('pending-jobs');

    // Create our pipeline:
    this.processJob = createJobPipeline((jobDef, job, jobRun) =>
      this.createJobContext(jobDef, job, jobRun)
    );
  }

  /**
   * Start task to plan for scheduled jobs.
   */
  async start(): Promise<void> {
    // Start the check timer
    this.timer = setInterval(async () => {
      try {
        await this.execute();
      } catch (err) {
        this.logger.error('Error processing pending jobs', {
          error: err instanceof Error ? err.message : String(err),
          error_stack: err instanceof Error ? err.stack : undefined,
        });
      }
    }, this.ctx.pollInterval);
  }

  /**
   * Stop the scheduled jobs planning task.
   */
  async stop(): Promise<void> {
    // Clear the timer
    if (this.timer) {
      clearInterval(this.timer);
      this.timer = null;
    }

    // Signal shutdown
    this.shutdownState.isShuttingDown = true;
    this.logger.info('Signaled shutdown to running jobs');
  }

  /**
   * Process pending jobs.
   */
  public async execute(): Promise<void> {
    // Shutting down.
    if (this.shutdownState.isShuttingDown) {
      this.logger.debug('Skipping execution due to shutdown');
      return;
    }

    // Max running jobs already hit.
    if (!this.ctx.concurrency.canRunMore()) {
      this.logger.debug('Skipping execution due to concurrency limit being hit');
      return;
    }

    const now = new Date();

    // Get pending jobs.
    const pendingJobs = await this.ctx.storage.listJobs({
      status: ['pending'],
      runAtBefore: now,
      limit: this.ctx.concurrency.maxConcurrentJobs * 2,
    });

    this.logger.debug('Found pending jobs to be processed', {
      jobs: pendingJobs.length,
    });

    // Get all valid jobs.
    const validPendingJobs = pendingJobs
      .map((job) => {
        const definition = this.ctx.jobs.get(job.type);
        if (!definition) {
          this.logger.warn('Invalid job type, skipping job', {
            jobId: job.id,
            type: job.type,
          });
          return null;
        }
        return { job, jobDef: definition };
      })
      .filter((entry): entry is { job: Job; jobDef: JobDefinition } => !!entry);

    const ctx: PendingJobsContext = {
      ...this.ctx,
      logger: this.logger,
    };

    // Process them in parallel (or in series, if you prefer)
    await Promise.all(
      validPendingJobs.map(({ job, jobDef }) => {
        // The pipeline itself handles concurrency checks & errors
        return this.processJob(ctx, job, jobDef);
      })
    );
  }

  /**
   * Create a new job context.
   */
  private createJobContext(
    jobDef: JobDefinition,
    job: Job,
    jobRun: { id: string; attempt: number; startedAt: Date }
  ): JobContextImpl {
    return new JobContextImpl(
      this.logger.child(`job-${job.id}`),
      this.ctx.storage,
      jobDef,
      this.ctx.worker,
      this.ctx.jobLockTTL,
      job.id,
      jobRun.id,
      jobRun.attempt,
      job.maxRetries,
      job.data,
      this.shutdownRef
    );
  }
}
