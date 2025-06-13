import { JobStorageMetrics } from './schemas/metrics.js';
import { CreateJob, Job, UpdateJob } from './schemas/job.js';
import { CreateJobLog, JobLogEntry } from './schemas/job-log.js';
import { CreateJobRun, JobRun, UpdateJobRun } from './schemas/job-run.js';
import { CreateDeadLetterJob, DeadLetterJob } from './schemas/dead-letter.js';
import { Worker, RegisterWorker, UpdateWorkerHeartbeat } from './schemas/worker.js';
import { CreateScheduledJob, ScheduledJob, UpdateScheduledJob } from './schemas/scheduled-job.js';

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
   * Create a new job.
   * @param job - Job data excluding id, createdAt, and updatedAt
   * @returns Promise that resolves with the ID of the created job
   */
  createJob(job: CreateJob): Promise<string>;

  /**
   * Get a job by ID.
   * @param id - ID of the job to retrieve
   * @returns Promise that resolves with the job or undefined if not found
   */
  getJob(id: string): Promise<Job | undefined>;

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
   * @param filter.lockedBy - Filter by worker ID that has locked the job
   * @param filter.runAtBefore - Filter by run date
   * @param filter.limit - Maximum number of jobs to return
   * @param filter.expiresAtBefore - Filter by expiration date
   * @returns Promise that resolves with an array of matching jobs
   */
  listJobs(filter?: {
    status?: string[];
    type?: string;
    referenceId?: string;
    runAtBefore?: Date;
    limit?: number;
    expiresAtBefore?: Date;
  }): Promise<Job[]>;

  /**
   * Create a job run.
   * @param run - Job run data excluding id
   * @returns Promise that resolves with the ID of the created job run
   */
  createJobRun(run: CreateJobRun): Promise<string>;

  /**
   * Get a job run by ID.
   * @param jobId - ID of the job to retrieve
   * @param jobRunId - ID of the job run to retrieve
   * @returns Promise that resolves with the job run or undefined if not found
   */
  getJobRun(jobId: string, jobRunId: string): Promise<JobRun | undefined>;

  /**
   * Update a job run.
   * @param jobId - ID of the job to update
   * @param jobRunId - ID of the job run to update
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
   * @param filter.referenceId - Filter by reference ID
   * @returns Promise that resolves with an array of matching scheduled jobs
   */
  listScheduledJobs(filter?: {
    enabled?: boolean;
    nextRunBefore?: Date;
    referenceId?: string;
  }): Promise<ScheduledJob[]>;

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
   * Register a new worker instance.
   * @param worker - Worker registration data
   * @returns Promise that resolves with the ID of the registered worker
   */
  registerWorker(worker: RegisterWorker): Promise<string>;

  /**
   * Update a worker's heartbeat.
   * @param id - ID of the worker to update
   * @param heartbeat - Heartbeat update data
   */
  updateWorkerHeartbeat(id: string, heartbeat: UpdateWorkerHeartbeat): Promise<void>;

  /**
   * Get a worker by ID.
   * @param id - ID of the worker to retrieve
   * @returns Promise that resolves with the worker data
   * @throws WorkerNotFoundError if the worker doesn't exist
   */
  getWorker(id: string): Promise<Worker | null>;

  /**
   * Get all workers that haven't sent a heartbeat since the given time.
   * @param lastHeartbeatBefore - Time threshold for considering workers inactive
   * @returns Promise that resolves with an array of inactive workers
   */
  getInactiveWorkers(lastHeartbeatBefore: Date): Promise<Worker[]>;

  /**
   * Get all registered workers.
   * @returns Promise that resolves with an array of all workers
   */
  getWorkers(): Promise<Worker[]>;

  /**
   * Delete a worker by ID.
   * @param id - ID of the worker to delete
   * @returns Promise that resolves when the worker is deleted
   * @throws WorkerNotFoundError if the worker doesn't exist
   */
  deleteWorker(id: string): Promise<void>;

  /**
   * Acquire a distributed lock.
   * @param lockId - ID of the lock to acquire
   * @param worker - The worker trying to acquire the lock
   * @param ttl - Time-to-live duration for the lock in milliseconds
   * @returns Promise that resolves with true if lock was acquired, false otherwise
   */
  acquireLock(lockId: string, worker: string, ttl: number): Promise<boolean>;

  /**
   * Extend an existing lock if owned by the specified worker.
   * @param lockId - ID of the lock to renew
   * @param worker - The worker trying to renew the lock
   * @param ttl - New time-to-live duration for the lock in milliseconds
   * @returns Promise that resolves with true if lock was renewed, false otherwise
   */
  renewLock(lockId: string, worker: string, ttl: number): Promise<boolean>;

  /**
   * Release a distributed lock.
   * @param lockId - ID of the lock to release
   * @param worker - The worker trying to release the lock
   * @returns Promise that resolves with true if lock was released, false otherwise
   */
  releaseLock(lockId: string, worker: string): Promise<boolean>;

  /**
   * List all locks held by a specific worker
   * @param worker - The worker to list locks for
   * @param expiredBefore - Filter by expiration date
   * @returns Promise that resolves with an array of lock data
   */
  listLocks(filters?: {
    worker?: string;
    expiredBefore?: Date;
  }): Promise<{ lockId: string; worker: string; expiresAt: Date }[]>;

  /**
   * Acquire a slot for a specific job type and worker
   * @param jobType - The type of the job
   * @param worker - The worker requesting the slot
   * @param maxConcurrent - Maximum allowed concurrent jobs for this type
   * @returns True if the slot was acquired, false otherwise
   */
  acquireTypeSlot(jobType: string, worker: string, maxConcurrent: number): Promise<boolean>;

  /**
   * Release a slot for a specific job type and worker
   * @param jobType - The type of the job
   * @param worker - The worker releasing the slot
   */
  releaseTypeSlot(jobType: string, worker: string): Promise<void>;

  /**
   * Release all slots held by a specific worker
   * @param worker - The worker to release all slots for
   */
  releaseAllTypeSlots(worker: string): Promise<void>;

  /**
   * Get the current running count for a specific job type or total across all types
   * @param jobType - Optional, the type of the job. If not provided, returns total across all types
   * @returns Number of currently running jobs
   */
  getRunningCount(jobType?: string): Promise<number>;

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

/**
 * Thrown when a worker is not found.
 */
export class WorkerNotFoundError extends JobStorageError {
  constructor(id: string) {
    super(`Worker with id ${id} not found`);
  }
}
