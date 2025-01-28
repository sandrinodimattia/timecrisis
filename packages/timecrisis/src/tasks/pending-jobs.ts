import { z } from 'zod';

import { Logger } from '../logger/index.js';
import { Job } from '../storage/schemas/index.js';
import { JobStorage } from '../storage/types.js';
import { JobContextImpl } from '../scheduler/context.js';
import { JobStateMachine } from '../state-machine/index.js';
import { formatLockName } from '../concurrency/job-lock.js';
import { JobContext, JobDefinition } from '../scheduler/types.js';
import { DistributedLock } from '../concurrency/distributed-lock.js';
import { GlobalConcurrencyManager } from '../concurrency/global-concurrency.js';

export interface PendingJobsConfig {
  /**
   * Name of the worker that is performing the job.
   */
  worker: string;

  /**
   * Logger.
   */
  logger: Logger;

  /**
   * Job state machine.
   */
  stateMachine: JobStateMachine;

  /**
   * Distributed lock.
   */
  distributedLock: DistributedLock;

  /**
   * Job definitions.
   */
  jobs: Map<string, JobDefinition>;

  /**
   * Storage backend.
   */
  storage: JobStorage;

  /**
   * Maximum number of concurrent jobs that can be running at once.
   */
  maxConcurrentJobs: number;

  /**
   * Lock lifetime in milliseconds.
   */
  jobLockTTL: number;

  /**
   * Poll interval in milliseconds.
   */
  pollInterval: number;

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
  private timer: NodeJS.Timeout | null = null;
  private readonly shutdownState = { isShuttingDown: false };
  private readonly shutdownRef = new WeakRef(this.shutdownState);

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
    }, this.cfg.pollInterval);
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
    if (!this.globalConcurrency.canRunMore()) {
      this.logger.debug('Skipping execution due to global concurrency limit');
      return;
    }

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

    // Get all valid jobs.
    const validPendingJobs = pendingJobs
      .map((job) => {
        const definition = this.cfg.jobs.get(job.type);
        if (!definition) {
          this.logger.warn('Invalid job type, skipping job', {
            jobId: job.id,
            type: job.type,
          });
          return {
            job,
            jobDefintion: undefined,
          };
        }

        return {
          job,
          jobDefintion: definition,
        };
      })
      .filter((pending) => typeof pending.jobDefintion !== 'undefined') as {
      job: Job;
      jobDefintion: JobDefinition;
    }[];

    // Job execution.
    const executionPromises = validPendingJobs.map(async ({ job, jobDefintion }) => {
      // Acquire a global concurrency slot.
      if (!this.globalConcurrency.acquire(job.id)) {
        this.logger.debug('Failed to acquire global concurrency slot for job', {
          jobId: job.id,
          type: job.type,
        });
        return;
      }

      try {
        // Acquire a lock for the job.
        const jobLock = await this.cfg.distributedLock.acquire(formatLockName(job.id));
        if (!jobLock) {
          this.logger.debug(`Failed to acquire lock to start job ${job.id}`, {
            jobId: job.id,
            type: job.type,
          });
          return;
        } else {
          this.logger.debug('Acquired job lock', {
            jobId: job.id,
            type: job.type,
          });
        }

        try {
          // Check if concurrency slot is available for this job type.
          const canExecuteJobType = await this.cfg.storage.acquireJobTypeSlot(
            job.type,
            this.cfg.worker,
            jobDefintion!.concurrency ?? this.cfg.maxConcurrentJobs
          );
          if (!canExecuteJobType) {
            this.logger.debug(`Failed to acquire concurrency slot for job type "${job.type}"`, {
              jobId: job.id,
              type: job.type,
            });
            return;
          } else {
            this.logger.debug(`Acquired concurrency slot for job type "${job.type}"`, {
              jobId: job.id,
              type: job.type,
            });
          }

          // Job expired.
          if (job.expiresAt && job.expiresAt < now) {
            // Fail the job.
            await this.cfg.stateMachine.fail(
              job,
              undefined,
              false,
              `Job expired (expiresAt=${job.expiresAt})`
            );
            return;
          }

          // Process the job by starting it.
          const { jobRunId, attempt } = await this.cfg.stateMachine.start(job);

          // Create job context with touch function.
          const ctx = this.createJobContext(jobDefintion!, job, {
            id: jobRunId,
            attempt,
            startedAt: new Date(),
          });

          try {
            if (jobDefintion!.forkMode === true) {
              this.logger.debug('Executing job in fork mode', {
                jobId: job.id,
                type: job.type,
                forkHelperPath: jobDefintion!.forkHelperPath,
              });

              await this.cfg.executeForkMode(jobDefintion!, job, ctx);
            } else {
              this.logger.debug('Executing job handler in process', {
                jobId: job.id,
                type: job.type,
              });

              await jobDefintion!.handle(job.data as typeof jobDefintion.schema, ctx);
            }

            this.logger.debug('Job completed', {
              jobId: job.id,
              type: job.type,
            });

            // Mark job as completed.
            await this.cfg.stateMachine.complete(job, jobRunId);
          } catch (error: unknown) {
            let errorMessage = error instanceof Error ? error.message : String(error);
            if (error instanceof z.ZodError) {
              const flat = error.errors.map((err) => `${err.message}`).join(',');
              errorMessage = `Zod validation error: ${flat}`;
            }

            // Mark job as failed.
            await this.cfg.stateMachine.fail(
              job,
              jobRunId,
              true,
              errorMessage,
              error instanceof Error ? error.stack : undefined
            );

            throw error;
          }
        } catch (err) {
          this.logger.error('Error processing job', {
            jobId: job.id,
            type: job.type,
            error: err instanceof Error ? err.message : String(err),
            error_stack: err instanceof Error ? err.stack : undefined,
          });
        } finally {
          this.logger.debug('Release job type slot', {
            jobId: job.id,
            type: job.type,
            worker: this.cfg.worker,
          });

          // Release concurrency slot regardless of job success or failure
          await this.cfg.storage.releaseJobTypeSlot(job.type, this.cfg.worker);

          this.logger.debug('Release job lock', {
            jobId: job.id,
            type: job.type,
          });

          // Release the lock on the job.
          await jobLock.release();
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
    await Promise.all(executionPromises);
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
      this.cfg.storage,
      jobDef,
      this.cfg.worker,
      this.cfg.jobLockTTL,
      job.id,
      jobRun.id,
      jobRun.attempt,
      job.maxRetries,
      job.data,
      this.shutdownRef
    );
  }
}
