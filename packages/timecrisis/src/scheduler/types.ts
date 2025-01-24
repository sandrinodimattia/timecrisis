import { z } from 'zod';

import { Logger } from '../logger/index.js';
import { JobStorage } from '../storage/types.js';
import { Job, JobStorageMetrics } from '../storage/schemas/index.js';

/**
 * Options for configuring the job scheduler.
 */
export interface SchedulerOptions {
  /**
   * Unique identifier for this scheduler instance.
   **/
  node?: string;

  /**
   * Storage adapter for persisting jobs and related data.
   **/
  storage: JobStorage;

  /**
   * Logger instance for logging job-related events.
   **/
  logger?: Logger;

  /**
   * Maximum number of jobs that can run concurrently across all types
   **/
  maxConcurrentJobs?: number;

  /**
   * How often to check for new jobs (in milliseconds)
   **/
  pollInterval?: number;

  /**
   * How long a job can run before being considered stuck (in milliseconds).
   **/
  jobLockTTL?: number;

  /**
   * How long a leader lock is valid before needing renewal (in milliseconds).
   **/
  leaderLockTTL?: number;
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
   * Average duration of job execution in milliseconds.
   **/
  averageDuration: number;

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
   * Priority of the job (-20 to 20, higher is more important).
   **/
  priority?: number;

  /**
   * Reference ID for grouping jobs (e.g., by user or entity).
   **/
  referenceId?: string;

  /**
   * When the job should expire.
   **/
  expiresAt?: Date;

  /**
   * How long until the job expires (alternative to expiresAt).
   **/
  expiresIn?: string;

  /**
   * Strategy for calculating retry delays.
   **/
  backoffStrategy?: 'exponential' | 'linear';
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
   * Whether the schedule should be active immediately.
   **/
  enabled?: boolean;
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
export interface JobDefinition<T extends z.ZodType = z.ZodType> {
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
   * Maximum number of concurrent executions.
   */
  concurrency?: number;

  /**
   * Job priority (-20 to 20, higher is more important).
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
   * ID of the current job.
   */
  jobId: string;

  /**
   * Job payload data.
   */
  payload: unknown;

  /**
   * Log a message at the specified level.
   */
  log(
    level: 'info' | 'warn' | 'error',
    message: string,
    metadata?: Record<string, unknown>
  ): Promise<void>;

  /**
   * Keep the job lock alive (for long-running jobs).
   */
  touch(): Promise<void>;
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
 * Thrown when a fork helper path is missing.
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
