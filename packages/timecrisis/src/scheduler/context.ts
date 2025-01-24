import { JobStorage } from '../storage/types.js';
import { JobContext, JobDefinition } from './types.js';

/**
 * Implementation of the JobContext interface.
 */
export class JobContextImpl implements JobContext {
  public readonly jobId: string;
  public readonly jobRunId: string;
  public readonly attempt: number;
  public readonly maxRetries: number;
  public readonly payload: unknown;
  private readonly touchFn: () => Promise<void>;
  private readonly storage: JobStorage;
  private readonly jobDefinition: JobDefinition;

  constructor(
    storage: JobStorage,
    jobDefinition: JobDefinition,
    jobId: string,
    jobRunId: string,
    attempt: number,
    maxRetries: number,
    payload: unknown,
    touchFn: () => Promise<void>
  ) {
    this.storage = storage;
    this.jobDefinition = jobDefinition;
    this.jobId = jobId;
    this.jobRunId = jobRunId;
    this.attempt = attempt;
    this.maxRetries = maxRetries;
    this.payload = payload;
    this.touchFn = touchFn;
  }

  /**
   * Log a message at the specified level.
   */
  async log(
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
    await this.touchFn();
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
      this.storage.updateJobRun(this.jobRunId, { progress }),
      this.storage.updateJob(this.jobId, { progress }),
    ]);
  }

  /**
   * Update the job data.
   */
  async updateData(data: Record<string, unknown>): Promise<void> {
    const parsed = this.jobDefinition.schema.partial().parse(data);

    // Update the parent job with the new data
    await this.storage.updateJob(this.jobId, { data: parsed });
  }
}
