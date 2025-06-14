import { z } from 'zod';

import { Logger } from '../logger/index.js';
import { JobStorage } from '../storage/types.js';
import { Job, JobStorageMetrics } from '../storage/schemas/index.js';

/**
 * Options for configuring the job scheduler.
 */
export interface SchedulerConfig {
  /**
   * Unique identifier for this scheduler instance.
   **/
  worker?: string;

  /**
   * Storage adapter for persisting jobs and related data.
   **/
  storage: JobStorage;

  /**
   * Logger instance for logging job-related events.
   **/
  logger?: Logger;

  /**
   * Maximum number of jobs that can run concurrently across all types.
   * Default: 20.
   **/
  maxConcurrentJobs?: number;

  /**
   * How long a job can run before being considered stuck (in milliseconds).
   * Default: 60 seconds.
   **/
  jobLockTTL?: number;

  /**
   * How long a leader lock is valid before needing renewal (in milliseconds).
   * Default: 30 seconds.
   **/
  leaderLockTTL?: number;

  /**
   * How often to check for jobs that need to be processed.
   * Default: 5 seconds.
   **/
  jobProcessingInterval?: number;

  /**
   * How often to check for jobs that need to be scheduled.
   * Default: 60 seconds.
   **/
  jobSchedulingInterval?: number;

  /**
   * Maximum age of a job before it is considered stale (in milliseconds).
   * This is used by the scheduler to determine if a scheduled job should be planned or skipped.
   * Default: 1 hour.
   **/
  scheduledJobMaxStaleAge?: number;

  /**
   * The interval in milliseconds at which to check for expired jobs.
   * Default: 60 seconds.
   */
  expiredJobCheckInterval?: number;

  /**
   * Maximum amount of time to wait for running tasks to complete before shutting down the scheduler.
   * Default: 15 seconds.
   */
  shutdownTimeout?: number;

  /**
   * The interval in milliseconds at which to send heartbeats.
   * This is an indication that the worker is still alive.
   * Default: 15 seconds.
   */
  workerHeartbeatInterval?: number;

  /**
   * The interval in milliseconds at which to check for inactive workers.
   * Inactive workers are deleted and their locks removed.
   * Default: 60 seconds.
   */
  workerInactiveCheckInterval?: number;

  /**
   * Callback that is invoked when a job starts.
   */
  onJobStarted?: (job: Job) => void;

  /**
   * Callback that is invoked when a job completes.
   */
  onJobCompleted?: (job: Job) => void;

  /**
   * Callback that is invoked when a job fails.
   */
  onJobFailed?: (job: Job, error: Error) => void;

  /**
   * Callback that is invoked when the scheduler gains leadership.
   */
  onLeadershipAcquired?: () => void;

  /**
   * Callback that is invoked when the scheduler loses leadership.
   */
  onLeadershipLost?: () => void;
}

/**
 * Metrics about the current state of the scheduler.
 */
export interface SchedulerMetrics {
  /**
   * Number of jobs currently executing.
   **/
  running: number;

  /**
   * Number of jobs waiting to be executed.
   **/
  pending: number;

  /**
   * Number of jobs that completed successfully.
   **/
  completed: number;

  /**
   * Number of jobs that failed.
   **/
  failed: number;

  /**
   * Number of jobs in the dead letter queue.
   **/
  deadLetterJobs: number;

  /**
   * Metrics broken down by job type.
   **/
  types: Record<
    string,
    {
      /**
       * Number of jobs of this type currently running.
       **/
      running: number;
      /**
       * Number of jobs of this type waiting to be executed.
       **/
      pending: number;
      /**
       * Number of jobs of this type that completed successfully.
       **/
      completed: number;
      /**
       * Number of jobs of this type that failed.
       **/
      failed: number;
    }
  >;

  /**
   * Storage metrics.
   */
  storage: JobStorageMetrics;
}

/**
 * Options for enqueueing a new job.
 */
export interface EnqueueOptions {
  /**
   * Maximum number of retries before moving to dead letter queue.
   **/
  maxRetries?: number;

  /**
   * Strategy for calculating retry delays.
   **/
  backoffStrategy?: 'exponential' | 'linear';

  /**
   * Priority of the job (1 to 100, lower is more important).
   **/
  priority?: number;

  /**
   * Reference ID for grouping jobs (e.g., by user or entity).
   **/
  referenceId?: string | null;

  /**
   * ID of the scheduled job that created this job, if any.
   **/
  scheduledJobId?: string;

  /**
   * When the job should expire.
   **/
  expiresAt?: Date;

  /**
   * How long until the job expires (alternative to expiresAt).
   **/
  expiresIn?: string;
}

/**
 * Options for scheduling a recurring job.
 */
export interface ScheduleOptions {
  /**
   * Value of the schedule (e.g. an exact ISO string, '1m' for 1 minute, '0 * * * *' for a cron expression).
   */
  scheduleValue: string;

  /**
   * Schedule type (exact, interval, cron).
   */
  scheduleType: 'exact' | 'interval' | 'cron';

  /**
   * IANA time zone for cron schedules (e.g., 'Europe/Paris').
   * If not provided, cron schedules default to UTC.
   */
  timeZone?: string;

  /**
   * Whether the schedule should be active immediately.
   **/
  enabled?: boolean;

  /**
   * Reference ID for grouping jobs (e.g., by user or entity).
   **/
  referenceId?: string;
}

/**
 * Job handler interface for processing jobs
 */
export interface JobHandler<T = unknown> {
  /**
   * Process a job.
   **/
  process(job: Job & { data: T }): Promise<void>;

  /**
   * Maximum number of concurrent jobs of this type.
   **/
  concurrency?: number;

  /**
   * Schema for validating job data.
   **/
  schema?: z.ZodType<T>;

  /**
   * Whether to run the job in a separate process.
   **/
  forkMode?: boolean;

  /**
   * Path to the worker script if running in fork mode.
   **/
  forkHelperPath?: string;
}

/**
 * Interface for job definitions.
 */
// eslint-disable-next-line @typescript-eslint/no-explicit-any
export interface JobDefinition<T extends z.ZodObject<any> = z.ZodObject<any>> {
  /**
   * Type of job.
   */
  type: string;

  /**
   * Schema for validating job data.
   */
  schema: T;

  /**
   * Function to process the job.
   */
  handle: (data: z.infer<T>, ctx: JobContext) => Promise<void>;

  /**
   * Maximum number of retries before moving to dead letter queue.
   **/
  maxRetries?: number;

  /**
   * Strategy for calculating retry delays.
   **/
  backoffStrategy?: 'exponential' | 'linear';

  /**
   * How long until the job expires.
   **/
  expiresAfter?: string;

  /**
   * How long until the running job is failed as expired.
   **/
  lockTTL?: string;

  /**
   * Maximum number of concurrent executions.
   */
  concurrency: number;

  /**
   * Job priority (1 to 100, from most important to least important).
   */
  priority?: number;

  /**
   * Whether to run in a separate process.
   */
  forkMode?: boolean;

  /**
   * Path to the worker script if running in fork mode.
   */
  forkHelperPath?: string;
}

/**
 * Interface for job handlers
 */
export interface JobContext {
  /**
   * The logger instance for the job.
   */
  logger: Logger;

  /**
   * The unique identifier of the job being executed
   */
  jobId: string;

  /**
   * The job payload
   */
  payload: unknown;

  /**
   * Indicates if the scheduler is shutting down and the job should try to gracefully terminate
   */
  isShuttingDown: boolean;

  /**
   * Log a message at the specified level.
   * @param level - Log level
   * @param message - Message to log
   * @param metadata - Optional metadata to include with the log
   */
  persistLog(
    level: 'info' | 'warn' | 'error',
    message: string,
    metadata?: Record<string, unknown>
  ): Promise<void>;

  /**
   * Keep the job lock alive (for long-running jobs).
   */
  touch(): Promise<void>;

  /**
   * Update the progress of the job (0-100).
   * @param progress - Progress value between 0 and 100
   */
  updateProgress(progress: number): Promise<void>;

  /**
   * Update the job data.
   * @param data - Updated job data
   */
  updateData(data: Record<string, unknown>): Promise<void>;
}

/**
 * Base error class for all Scheduler errors.
 */
export class SchedulerError extends Error {
  constructor(message: string) {
    super(message);
    this.name = this.constructor.name;
    Error.captureStackTrace(this, this.constructor);
  }
}

/**
 * Thrown when a job type is already registered.
 */
export class JobAlreadyRegisteredError extends SchedulerError {
  constructor(type: string) {
    super(`Job type '${type}' already registered`);
  }
}

/**
 * Thrown when a job definition is not found.
 */
export class JobDefinitionNotFoundError extends SchedulerError {
  constructor(type: string) {
    super(`Job type '${type}' not found`);
  }
}

/**
 * Thrown when a job's data is invalid.
 */
export class InvalidJobDataError extends SchedulerError {
  constructor(message: string) {
    super(message);
  }
}

/**
 * Thrown when a fork helper path is missing for a fork mode job.
 */
export class ForkHelperPathMissingError extends SchedulerError {
  constructor(type: string) {
    super(`forkHelperPath is required for job type '${type}' in fork mode`);
  }
}

/**
 * Thrown when a schedule value is invalid.
 */
export class InvalidScheduleError extends SchedulerError {
  constructor(value: string, scheduleType: string) {
    super(`Invalid ${scheduleType}: ${value}`);
  }
}

/**
 * Thrown when a process exits with a non-zero code.
 */
export class ProcessExitError extends SchedulerError {
  constructor(code: number) {
    super(`Process exited with code ${code}`);
  }
}

/**
 * Thrown when a job expires.
 */
export class JobExpiredError extends SchedulerError {
  constructor(message: string) {
    super(message);
  }
}

/**
 * Thrown when a job lock expires.
 */
export class JobLockExpiredError extends SchedulerError {
  constructor(message: string) {
    super(message);
  }
}

/**
 * Throw when a worker was terminated.
 */
export class WorkerTerminatedError extends SchedulerError {
  constructor(message: string) {
    super(message);
  }
}
