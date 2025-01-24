import { JobStorageMetrics } from './schemas/metrics';
import { CreateJob, Job, UpdateJob } from './schemas/job';
import { CreateJobLog, JobLogEntry } from './schemas/job-log';
import { CreateJobRun, JobRun, UpdateJobRun } from './schemas/job-run';
import { CreateDeadLetterJob, DeadLetterJob } from './schemas/dead-letter';
import { CreateScheduledJob, ScheduledJob, UpdateScheduledJob } from './schemas/scheduled-job';

interface CleanupOptions {
  /**
   * Number of days to retain completed jobs
   */
  jobRetention: number;

  /**
   * Number of days to retain failed jobs
   */
  failedJobRetention: number;

  /**
   * Number of days to retain dead letter jobs
   */
  deadLetterRetention: number;
}

/**
 * Job storage interface defining required storage operations.
 */
export interface JobStorage {
  /**
   * Initialize the storage adapter.
   * @returns Promise that resolves when initialization is complete
   */
  init(): Promise<void>;

  /**
   * Run operations in a transaction.
   * @param fn - Function to execute within the transaction
   * @returns Promise that resolves with the result of the transaction
   */
  transaction<T>(fn: (trx: unknown) => Promise<T>): Promise<T>;

  /**
   * Create a new job.
   * @param job - Job data excluding id, createdAt, and updatedAt
   * @returns Promise that resolves with the ID of the created job
   */
  createJob(job: CreateJob): Promise<string>;

  /**
   * Get a job by ID.
   * @param id - ID of the job to retrieve
   * @returns Promise that resolves with the job or null if not found
   */
  getJob(id: string): Promise<Job | null>;

  /**
   * Update a job's properties.
   * @param id - ID of the job to update
   * @param updates - Partial job object containing properties to update
   * @returns Promise that resolves when the update is complete
   */
  updateJob(id: string, updates: UpdateJob): Promise<void>;

  /**
   * List jobs matching certain criteria.
   * @param filter - Optional filter criteria for jobs
   * @param filter.status - Filter by job status
   * @param filter.type - Filter by job type
   * @param filter.referenceId - Filter by reference ID
   * @param filter.lockedBefore - Filter by lock date
   * @param filter.runAtBefore - Filter by run date
   * @param filter.limit - Maximum number of jobs to return
   * @returns Promise that resolves with an array of matching jobs
   */
  listJobs(filter?: {
    status?: string[];
    type?: string;
    referenceId?: string;
    lockedBefore?: Date;
    runAtBefore?: Date;
    limit?: number;
  }): Promise<Job[]>;

  /**
   * Create a job run.
   * @param run - Job run data excluding id
   * @returns Promise that resolves with the ID of the created job run
   */
  createJobRun(run: CreateJobRun): Promise<string>;

  /**
   * Update a job run.
   * @param id - ID of the job run to update
   * @param updates - Partial job run object containing properties to update
   * @returns Promise that resolves when the update is complete
   */
  updateJobRun(id: string, updates: UpdateJobRun): Promise<void>;

  /**
   * List runs for a job.
   * @param jobId - ID of the job to list runs for
   * @returns Promise that resolves with an array of job runs
   */
  listJobRuns(jobId: string): Promise<JobRun[]>;

  /**
   * Create a log entry.
   * @param log - Log entry data excluding id
   * @returns Promise that resolves when the log entry is created
   */
  createJobLog(log: CreateJobLog): Promise<void>;

  /**
   * List logs for a job.
   * @param jobId - ID of the job to list logs for
   * @param runId - Optional ID of specific run to list logs for
   * @returns Promise that resolves with an array of log entries
   */
  listJobLogs(jobId: string, runId?: string): Promise<JobLogEntry[]>;

  /**
   * Create a scheduled job.
   * @param job - Scheduled job data excluding id, createdAt, and updatedAt
   * @returns Promise that resolves with the ID of the created scheduled job
   */
  createScheduledJob(job: CreateScheduledJob): Promise<string>;

  /**
   * Get a scheduled job by ID.
   * @param id - ID of the scheduled job to retrieve
   * @returns Promise that resolves with the scheduled job or null if not found
   */
  getScheduledJob(id: string): Promise<ScheduledJob | null>;

  /**
   * Update a scheduled job.
   * @param id - ID of the scheduled job to update
   * @param updates - Partial scheduled job object containing properties to update
   * @returns Promise that resolves when the update is complete
   */
  updateScheduledJob(id: string, updates: UpdateScheduledJob): Promise<void>;

  /**
   * List scheduled jobs matching certain criteria.
   * @param filter - Optional filter criteria for scheduled jobs
   * @param filter.enabled - Filter by enabled status
   * @param filter.nextRunBefore - Filter by next run date
   * @returns Promise that resolves with an array of matching scheduled jobs
   */
  listScheduledJobs(filter?: { enabled?: boolean; nextRunBefore?: Date }): Promise<ScheduledJob[]>;

  /**
   * Create a dead letter entry.
   * @param job - Dead letter job data excluding id
   * @returns Promise that resolves when the dead letter entry is created
   */
  createDeadLetterJob(job: CreateDeadLetterJob): Promise<void>;

  /**
   * List dead letter entries.
   * @returns Promise that resolves with an array of dead letter jobs
   */
  listDeadLetterJobs(): Promise<DeadLetterJob[]>;

  /**
   * Acquire a distributed lock.
   * @param lockId - ID of the lock to acquire
   * @param owner - ID of the owner trying to acquire the lock
   * @param ttl - Time-to-live duration for the lock in milliseconds
   * @returns Promise that resolves with true if lock was acquired, false otherwise
   */
  acquireLock(lockId: string, owner: string, ttl: number): Promise<boolean>;

  /**
   * Extend an existing lock if owned by the specified owner.
   * @param lockId - ID of the lock to renew
   * @param owner - ID of the owner trying to renew the lock
   * @param ttl - New time-to-live duration for the lock in milliseconds
   * @returns Promise that resolves with true if lock was renewed, false otherwise
   */
  renewLock(lockId: string, owner: string, ttl: number): Promise<boolean>;

  /**
   * Release a distributed lock.
   * @param lockId - ID of the lock to release
   * @param owner - ID of the owner trying to release the lock
   * @returns Promise that resolves with true if lock was released, false otherwise
   */
  releaseLock(lockId: string, owner: string): Promise<boolean>;

  /**
   * Clean up old job data.
   * @param options - Cleanup configuration options
   * @param options.jobRetention - Number of days to retain completed jobs
   * @param options.failedJobRetention - Number of days to retain failed jobs
   * @param options.deadLetterRetention - Number of days to retain dead letter jobs
   * @returns Promise that resolves when cleanup is complete
   */
  cleanup(options: CleanupOptions): Promise<void>;

  /**
   * Get metrics about jobs in the system.
   * @returns Promise that resolves with job storage metrics
   */
  getMetrics(): Promise<JobStorageMetrics>;

  /**
   * Close the storage adapter and clean up any resources.
   * @returns Promise that resolves when the adapter is closed
   */
  close(): Promise<void>;
}

/**
 * Base error class for all storage errors.
 */
export class JobStorageError extends Error {
  constructor(message: string) {
    super(message);
    this.name = this.constructor.name;
    Error.captureStackTrace(this, this.constructor);
  }
}

/**
 * Thrown when a job type is not found.
 */
export class JobNotFoundError extends JobStorageError {
  constructor(id: string) {
    super(`Job with id ${id} not found`);
  }
}

/**
 * Thrown when a job run is not found.
 */
export class JobRunNotFoundError extends JobStorageError {
  constructor(id: string) {
    super(`JobRun with id ${id} not found`);
  }
}

/**
 * Thrown when a scheduled job is not found.
 */
export class ScheduledJobNotFoundError extends JobStorageError {
  constructor(id: string) {
    super(`ScheduledJob with id ${id} not found`);
  }
}
