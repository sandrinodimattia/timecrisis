import { z } from 'zod';
import { Logger } from '../logger/index.js';
import { JobStorage } from '../storage/types.js';
import { formatLockName } from '../concurrency/job-lock.js';
import { JobContext, JobDefinition, InvalidJobDataError } from './types.js';

/**
 * Implementation of the JobContext interface.
 */
export class JobContextImpl implements JobContext {
  public readonly logger: Logger;
  public readonly jobId: string;
  public readonly jobRunId: string;
  public readonly attempt: number;
  public readonly maxRetries: number;
  public readonly payload: unknown;

  private readonly worker: string;
  private readonly jobLockTTL: number;
  private readonly storage: JobStorage;
  private readonly jobDefinition: JobDefinition;
  private readonly shutdownRef: WeakRef<{ isShuttingDown: boolean }>;

  constructor(
    logger: Logger,
    storage: JobStorage,
    jobDefinition: JobDefinition,
    worker: string,
    jobLockTTL: number,
    jobId: string,
    jobRunId: string,
    attempt: number,
    maxRetries: number,
    payload: unknown,
    shutdownRef: WeakRef<{ isShuttingDown: boolean }>
  ) {
    this.logger = logger.child(`job/${jobDefinition.type}`);
    this.storage = storage;
    this.jobDefinition = jobDefinition;
    this.worker = worker;
    this.jobLockTTL = jobLockTTL;
    this.jobId = jobId;
    this.jobRunId = jobRunId;
    this.attempt = attempt;
    this.maxRetries = maxRetries;
    this.payload = payload;
    this.shutdownRef = shutdownRef;
  }

  /**
   * Indicates if the scheduler is shutting down and the job should try to gracefully terminate
   */
  get isShuttingDown(): boolean {
    const ref = this.shutdownRef.deref();
    return ref ? ref.isShuttingDown : false;
  }

  /**
   * Log a message at the specified level.
   */
  async persistLog(
    level: 'info' | 'warn' | 'error',
    message: string,
    metadata?: Record<string, unknown>
  ): Promise<void> {
    await this.storage.createJobLog({
      jobId: this.jobId,
      jobRunId: this.jobRunId,
      level,
      message,
      metadata,
      timestamp: new Date(),
    });
  }

  /**
   * Keep the job lock alive (for long-running jobs).
   */
  async touch(): Promise<void> {
    await Promise.all([
      this.storage.updateJobRun(this.jobRunId, { touchedAt: new Date() }),
      this.storage.renewLock(formatLockName(this.jobId), this.worker, this.jobLockTTL),
    ]);
  }

  /**
   * Update the progress of the job (0-100).
   */
  async updateProgress(progress: number): Promise<void> {
    // Validate progress value
    if (progress < 0 || progress > 100) {
      throw new Error('Progress value must be between 0 and 100');
    }

    // Update both the job run and the parent job with the new progress
    await Promise.all([
      this.storage.updateJobRun(this.jobRunId, {
        progress,
        touchedAt: new Date(),
      }),
      this.storage.renewLock(formatLockName(this.jobId), this.worker, this.jobLockTTL),
    ]);
  }

  /**
   * Update the job data.
   */
  async updateData(data: Record<string, unknown>): Promise<void> {
    try {
      const parsed = this.jobDefinition.schema.partial().strict().parse(data);
      await this.storage.updateJob(this.jobId, { data: parsed });
    } catch (error) {
      if (error instanceof z.ZodError) {
        throw new InvalidJobDataError(
          `Validation error while updating job ${this.jobId}: ${error.errors && error.errors.length > 0 ? error.errors[0].message : error.message}`
        );
      }

      throw error;
    }
  }
}
