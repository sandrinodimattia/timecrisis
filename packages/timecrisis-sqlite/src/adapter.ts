import * as path from 'path';
import { fileURLToPath } from 'url';
import { randomUUID } from 'crypto';
import { Migrator } from 'sqlite-up';
import { Database } from 'better-sqlite3';

import {
  CreateJob,
  CreateJobSchema,
  UpdateJob,
  UpdateJobSchema,
  JobSchema,
  Job,
  CreateJobRun,
  CreateJobRunSchema,
  UpdateJobRun,
  JobRunSchema,
  JobRun,
  CreateJobLog,
  CreateJobLogSchema,
  JobLogEntry,
  JobLogEntrySchema,
  CreateScheduledJob,
  CreateScheduledJobSchema,
  UpdateScheduledJob,
  UpdateScheduledJobSchema,
  ScheduledJobSchema,
  ScheduledJob,
  CreateDeadLetterJob,
  CreateDeadLetterJobSchema,
  DeadLetterJob,
  DeadLetterJobSchema,
  JobStorageMetrics,
  JobStorage,
  JobNotFoundError,
  JobRunNotFoundError,
  ScheduledJobNotFoundError,
} from 'timecrisis';

import { SQLiteStatements as SQL } from './statements.js';
import { fromBoolean, fromDate, parseJSON, serializeData, toBoolean, toDate } from './utils.js';

/**
 * SQLite storage adapter that implements the JobStorage interface.
 * This storage provider uses SQLite as the backing store.
 */
export class SQLiteJobStorage implements JobStorage {
  /**
   * The SQLite database instance used for all operations
   */
  private db: Database;

  /**
   * Prepared statement for inserting a new job
   */
  private stmtInsertJob!: ReturnType<Database['prepare']>;

  /**
   * Prepared statement for selecting a job by its ID
   */
  private stmtSelectJobById!: ReturnType<Database['prepare']>;

  /**
   * Prepared statement for updating an existing job
   */
  private stmtUpdateJob!: ReturnType<Database['prepare']>;

  /**
   * Prepared statement for deleting a job
   */
  private stmtDeleteJob!: ReturnType<Database['prepare']>;

  /**
   * Prepared statement for selecting jobs based on filters
   */
  private stmtSelectFilteredJobs!: ReturnType<Database['prepare']>;

  /**
   * Prepared statement for inserting a new job run
   */
  private stmtInsertJobRun!: ReturnType<Database['prepare']>;

  /**
   * Prepared statement for selecting a job run by its ID
   */
  private stmtSelectJobRunById!: ReturnType<Database['prepare']>;

  /**
   * Prepared statement for updating a job run
   */
  private stmtUpdateJobRun!: ReturnType<Database['prepare']>;

  /**
   * Prepared statement for selecting all runs of a specific job
   */
  private stmtSelectJobRunsByJobId!: ReturnType<Database['prepare']>;

  /**
   * Prepared statement for deleting all runs of a specific job
   */
  private stmtDeleteJobRunsByJobId!: ReturnType<Database['prepare']>;

  /**
   * Prepared statement for inserting a new job log entry
   */
  private stmtInsertJobLog!: ReturnType<Database['prepare']>;

  /**
   * Prepared statement for selecting logs by job ID
   */
  private stmtSelectJobLogsByJobId!: ReturnType<Database['prepare']>;

  /**
   * Prepared statement for selecting logs by job and run ID
   */
  private stmtSelectJobLogsByJobAndRun!: ReturnType<Database['prepare']>;

  /**
   * Prepared statement for deleting logs of a specific job
   */
  private stmtDeleteJobLogsByJobId!: ReturnType<Database['prepare']>;

  /**
   * Prepared statement for inserting a new scheduled job
   */
  private stmtInsertScheduledJob!: ReturnType<Database['prepare']>;

  /**
   * Prepared statement for selecting a scheduled job by its ID
   */
  private stmtSelectScheduledJobById!: ReturnType<Database['prepare']>;

  /**
   * Prepared statement for updating a scheduled job
   */
  private stmtUpdateScheduledJob!: ReturnType<Database['prepare']>;

  /**
   * Prepared statement for deleting a scheduled job
   */
  private stmtDeleteScheduledJob!: ReturnType<Database['prepare']>;

  /**
   * Prepared statement for selecting all scheduled jobs
   */
  private stmtSelectAllScheduledJobs!: ReturnType<Database['prepare']>;

  /**
   * Prepared statement for selecting scheduled jobs based on filters
   */
  private stmtSelectFilteredScheduledJobs!: ReturnType<Database['prepare']>;

  /**
   * Prepared statement for inserting a job into the dead letter queue
   */
  private stmtInsertDeadLetterJob!: ReturnType<Database['prepare']>;

  /**
   * Prepared statement for selecting all dead letter jobs
   */
  private stmtSelectAllDeadLetterJobs!: ReturnType<Database['prepare']>;

  /**
   * Prepared statement for deleting dead letter jobs before a certain date
   */
  private stmtDeleteDeadLetterBefore!: ReturnType<Database['prepare']>;

  /**
   * Prepared statement for selecting a lock
   */
  private stmtSelectLock!: ReturnType<Database['prepare']>;

  /**
   * Prepared statement for inserting a new lock
   */
  private stmtInsertLock!: ReturnType<Database['prepare']>;

  /**
   * Prepared statement for updating an existing lock
   */
  private stmtUpdateLock!: ReturnType<Database['prepare']>;

  /**
   * Prepared statement for deleting a lock
   */
  private stmtDeleteLock!: ReturnType<Database['prepare']>;

  /**
   * Prepared statement for deleting expired locks
   */
  private stmtDeleteExpiredLocks!: ReturnType<Database['prepare']>;

  /**
   * Prepared statement for cleaning up completed jobs
   */
  private stmtCleanupCompleted!: ReturnType<Database['prepare']>;

  /**
   * Prepared statement for cleaning up failed jobs
   */
  private stmtCleanupFailed!: ReturnType<Database['prepare']>;

  /**
   * Prepared statement for getting job counts by status
   */
  private stmtJobCounts!: ReturnType<Database['prepare']>;

  /**
   * Prepared statement for calculating average job duration
   */
  private stmtAvgDuration!: ReturnType<Database['prepare']>;

  /**
   * Prepared statement for calculating job failure rate
   */
  private stmtFailureRate!: ReturnType<Database['prepare']>;

  constructor(db: Database) {
    this.db = db;
  }

  /**
   * Initialize the SQLite storage provider
   * This will run any pending migrations and prepare all SQL statements
   * for improved performance
   */
  async init(): Promise<void> {
    const __filename = fileURLToPath(import.meta.url);
    const __dirname = path.dirname(__filename);
    const migrator = new Migrator({
      db: this.db,
      migrationsDir: path.join(__dirname, 'migrations'),
    });

    const result = await migrator.apply();
    if (result.error) {
      throw result.error;
    }

    // Now prepare statements
    this.stmtInsertJob = this.db.prepare(SQL.insertJob);
    this.stmtSelectJobById = this.db.prepare(SQL.selectJobById);
    this.stmtUpdateJob = this.db.prepare(SQL.updateJob);
    this.stmtDeleteJob = this.db.prepare(SQL.deleteJob);
    this.stmtSelectFilteredJobs = this.db.prepare(SQL.selectFilteredJobs);

    this.stmtInsertJobRun = this.db.prepare(SQL.insertJobRun);
    this.stmtSelectJobRunById = this.db.prepare(SQL.selectJobRunById);
    this.stmtUpdateJobRun = this.db.prepare(SQL.updateJobRun);
    this.stmtSelectJobRunsByJobId = this.db.prepare(SQL.selectJobRunsByJobId);
    this.stmtDeleteJobRunsByJobId = this.db.prepare(SQL.deleteJobRunsByJobId);

    this.stmtInsertJobLog = this.db.prepare(SQL.insertJobLog);
    this.stmtSelectJobLogsByJobId = this.db.prepare(SQL.selectJobLogsByJobId);
    this.stmtSelectJobLogsByJobAndRun = this.db.prepare(SQL.selectJobLogsByJobAndRun);
    this.stmtDeleteJobLogsByJobId = this.db.prepare(SQL.deleteJobLogsByJobId);

    this.stmtInsertScheduledJob = this.db.prepare(SQL.insertScheduledJob);
    this.stmtSelectScheduledJobById = this.db.prepare(SQL.selectScheduledJobById);
    this.stmtUpdateScheduledJob = this.db.prepare(SQL.updateScheduledJob);
    this.stmtDeleteScheduledJob = this.db.prepare(SQL.deleteScheduledJob);
    this.stmtSelectAllScheduledJobs = this.db.prepare(SQL.selectAllScheduledJobs);
    this.stmtSelectFilteredScheduledJobs = this.db.prepare(SQL.selectFilteredScheduledJobs);

    this.stmtInsertDeadLetterJob = this.db.prepare(SQL.insertDeadLetterJob);
    this.stmtSelectAllDeadLetterJobs = this.db.prepare(SQL.selectAllDeadLetterJobs);
    this.stmtDeleteDeadLetterBefore = this.db.prepare(SQL.deleteDeadLetterBefore);

    this.stmtSelectLock = this.db.prepare(SQL.selectLock);
    this.stmtInsertLock = this.db.prepare(SQL.insertLock);
    this.stmtUpdateLock = this.db.prepare(SQL.updateLock);
    this.stmtDeleteLock = this.db.prepare(SQL.deleteLock);
    this.stmtDeleteExpiredLocks = this.db.prepare(SQL.deleteExpiredLocks);

    this.stmtCleanupCompleted = this.db.prepare(SQL.cleanupCompleted);
    this.stmtCleanupFailed = this.db.prepare(SQL.cleanupFailed);

    this.stmtJobCounts = this.db.prepare(SQL.jobCounts);
    this.stmtAvgDuration = this.db.prepare(SQL.avgDuration);
    this.stmtFailureRate = this.db.prepare(SQL.failureRate);
  }

  /**
   * Close the database connection and clean up resources
   * This should be called when the storage provider is no longer needed
   */
  async close(): Promise<void> {
    this.db.close();
  }

  /**
   * Execute a function within a transaction
   * This ensures ACID properties for all operations within the transaction
   * @param fn - Function to execute within the transaction
   * @returns Promise resolving to the function's result
   */
  async transaction<T>(fn: (trx: unknown) => Promise<T>): Promise<T> {
    const wrapped = this.db.transaction((_: unknown) => {
      return fn(null);
    });
    return wrapped(null);
  }

  /**
   * Create a new job in the database
   * @param job - Job data to create
   * @returns Unique identifier of the created job
   */
  async createJob(job: CreateJob): Promise<string> {
    // Parse and validate the input using Zod schema
    const validJob = CreateJobSchema.parse(job);

    // Generate a new unique identifier and timestamp
    const id = randomUUID();
    const now = new Date();

    // Create and validate the full job object
    const newJob = JobSchema.parse({
      ...validJob,
      id,
      createdAt: now,
      updatedAt: now,
    });

    // Insert the job into the database
    this.stmtInsertJob.run({
      id: newJob.id,
      type: newJob.type,
      referenceId: newJob.referenceId,
      status: newJob.status,
      data: serializeData(newJob.data),
      priority: newJob.priority,
      attempts: newJob.attempts,
      max_retries: newJob.maxRetries,
      backoff_strategy: newJob.backoffStrategy,
      fail_reason: newJob.failReason ?? null,
      fail_count: newJob.failCount,
      execution_duration: newJob.executionDuration ?? null,
      reference_id: newJob.referenceId ?? null,
      expires_at: fromDate(newJob.expiresAt),
      locked_at: fromDate(newJob.lockedAt),
      started_at: fromDate(newJob.startedAt),
      run_at: fromDate(newJob.runAt),
      finished_at: fromDate(newJob.finishedAt),
      created_at: newJob.createdAt.toISOString(),
      updated_at: newJob.updatedAt.toISOString(),
    });

    return newJob.id;
  }

  /**
   * Retrieve a job by its unique identifier
   * @param id - Unique identifier of the job to retrieve
   * @returns Job data or null if not found
   */
  async getJob(id: string): Promise<Job | null> {
    const row = this.stmtSelectJobById.get(id);
    if (!row) {
      return null;
    }
    return this.mapRowToJob(row);
  }

  /**
   * Update an existing job with new data
   * @param id - Unique identifier of the job to update
   * @param updates - Updated job data
   */
  async updateJob(id: string, updates: UpdateJob): Promise<void> {
    // Verify job exists
    const existing = await this.getJob(id);
    if (!existing) {
      throw new JobNotFoundError(id);
    }

    // Parse and validate the updates
    const validUpdates = UpdateJobSchema.parse(updates);

    // Create and validate the updated job
    const now = new Date();
    const updatedJob = JobSchema.parse({
      ...existing,
      ...validUpdates,
      updatedAt: now,
    });

    // Update the job in the database
    this.stmtUpdateJob.run({
      id: updatedJob.id,
      type: updatedJob.type,
      referenceId: updatedJob.referenceId,
      status: updatedJob.status,
      data: serializeData(updatedJob.data),
      priority: updatedJob.priority,
      attempts: updatedJob.attempts,
      max_retries: updatedJob.maxRetries,
      backoff_strategy: updatedJob.backoffStrategy,
      fail_reason: updatedJob.failReason ?? null,
      fail_count: updatedJob.failCount,
      execution_duration: updatedJob.executionDuration ?? null,
      reference_id: updatedJob.referenceId ?? null,
      expires_at: fromDate(updatedJob.expiresAt),
      locked_at: fromDate(updatedJob.lockedAt),
      started_at: fromDate(updatedJob.startedAt),
      run_at: fromDate(updatedJob.runAt),
      finished_at: fromDate(updatedJob.finishedAt),
      updated_at: updatedJob.updatedAt.toISOString(),
    });
  }

  /**
   * Create a new job run for an existing job
   * @param jobRun - Job run data to create
   * @returns Unique identifier of the created job run
   */
  async createJobRun(jobRun: CreateJobRun): Promise<string> {
    // Parse and validate the input
    const valid = CreateJobRunSchema.parse(jobRun);

    // Generate a new unique identifier and timestamp
    const id = randomUUID();

    // Create and validate the full job run object
    const newRun = JobRunSchema.parse({
      ...valid,
      id,
    });

    this.stmtInsertJobRun.run({
      id: newRun.id,
      job_id: newRun.jobId,
      status: newRun.status,
      started_at: fromDate(newRun.startedAt),
      finished_at: fromDate(newRun.finishedAt),
      attempt: newRun.attempt,
      error: newRun.error ?? null,
      error_stack: newRun.error_stack ?? null,
    });

    return newRun.id;
  }

  /**
   * Update an existing job run with new data
   * @param id - Unique identifier of the job run to update
   * @param updates - Updated job run data
   */
  async updateJobRun(id: string, updates: UpdateJobRun): Promise<void> {
    // Verify job run exists
    const existingRow = this.stmtSelectJobRunById.get(id);
    if (!existingRow) {
      throw new JobRunNotFoundError(id);
    }

    // Get the existing job run
    const existing = this.mapRowToJobRun(existingRow);

    // Create and validate the updated run
    const updatedRun = JobRunSchema.parse({
      ...existing,
      ...updates,
    });

    // Update the job run in the database
    this.stmtUpdateJobRun.run({
      id: updatedRun.id,
      status: updatedRun.status,
      started_at: fromDate(updatedRun.startedAt),
      finished_at: fromDate(updatedRun.finishedAt),
      attempt: updatedRun.attempt,
      error: updatedRun.error ?? null,
      error_stack: updatedRun.error_stack ?? null,
    });
  }

  /**
   * List job runs for a specific job
   * @param jobId - Unique identifier of the job
   * @returns Array of job runs for the specified job
   */
  async listJobRuns(jobId: string): Promise<JobRun[]> {
    const rows = this.stmtSelectJobRunsByJobId.all(jobId);
    return rows.map((r) => this.mapRowToJobRun(r));
  }

  /**
   * List jobs based on the provided filter criteria
   * @param filter - Filter criteria (optional)
   * @returns Array of jobs matching the filter criteria
   */
  async listJobs(filter?: {
    status?: string[];
    type?: string;
    referenceId?: string;
    lockedBefore?: Date;
    runAtBefore?: Date;
    limit?: number;
  }): Promise<Job[]> {
    // eslint-disable-next-line @typescript-eslint/no-explicit-any
    const params: any = {
      type: filter?.type || null,
      referenceId: filter?.referenceId || null,
      lockedBefore: filter?.lockedBefore ? fromDate(filter.lockedBefore) : null,
      runAtBefore: filter?.runAtBefore ? fromDate(filter.runAtBefore) : null,
      limit: filter?.limit || null,
      status: filter?.status ? JSON.stringify(filter.status) : null,
    };

    const rows = this.stmtSelectFilteredJobs.all(params);
    return rows.map((row) => this.mapRowToJob(row));
  }

  /**
   * Create a new log entry for a job run
   * @param log - Log entry data to create
   */
  async createJobLog(log: CreateJobLog): Promise<void> {
    const validLog = CreateJobLogSchema.parse(log);

    const newLog = JobLogEntrySchema.parse({
      ...validLog,
      id: randomUUID(),
    });

    this.stmtInsertJobLog.run({
      id: newLog.id,
      job_id: newLog.jobId,
      job_run_id: newLog.jobRunId ?? null,
      timestamp: newLog.timestamp.toISOString(),
      level: newLog.level,
      message: newLog.message,
      metadata: newLog.metadata ? JSON.stringify(newLog.metadata) : null,
    });
  }

  /**
   * List log entries for a specific job run
   * @param jobId - Unique identifier of the job
   * @param runId - Unique identifier of the job run (optional)
   * @returns Array of log entries for the specified job run
   */
  async listJobLogs(jobId: string, runId?: string): Promise<JobLogEntry[]> {
    if (runId) {
      const rows = this.stmtSelectJobLogsByJobAndRun.all([jobId, runId]);
      return rows.map((r) => this.mapRowToJobLog(r));
    }
    const rows = this.stmtSelectJobLogsByJobId.all([jobId]);
    return rows.map((r) => this.mapRowToJobLog(r));
  }

  /**
   * Create a new scheduled job
   * @param job - Scheduled job data to create
   * @returns Unique identifier of the created scheduled job
   */
  async createScheduledJob(job: CreateScheduledJob): Promise<string> {
    const validJob = CreateScheduledJobSchema.parse(job);

    const now = new Date();
    const id = randomUUID();
    const newJob = ScheduledJobSchema.parse({
      ...validJob,
      id,
      createdAt: now,
      updatedAt: now,
    });

    const dataStr = serializeData(newJob.data);

    this.stmtInsertScheduledJob.run({
      id: newJob.id,
      name: newJob.name,
      type: newJob.type,
      schedule_type: newJob.scheduleType,
      schedule_value: newJob.scheduleValue,
      data: dataStr,
      enabled: fromBoolean(newJob.enabled),
      last_scheduled_at: fromDate(newJob.lastScheduledAt),
      next_run_at: fromDate(newJob.nextRunAt),
      created_at: newJob.createdAt.toISOString(),
      updated_at: newJob.updatedAt.toISOString(),
    });

    return newJob.id;
  }

  /**
   * Update an existing scheduled job with new data
   * @param id - Unique identifier of the scheduled job to update
   * @param updates - Updated scheduled job data
   */
  async updateScheduledJob(id: string, updates: UpdateScheduledJob): Promise<void> {
    const row = this.stmtSelectScheduledJobById.get(id);
    if (!row) {
      throw new ScheduledJobNotFoundError(`Scheduled job with id=${id} not found`);
    }

    const existing = this.mapRowToScheduledJob(row);

    const validUpdates = UpdateScheduledJobSchema.parse(updates);
    const updated = ScheduledJobSchema.parse({
      ...existing,
      ...validUpdates,
      updatedAt: new Date(),
    });

    const dataStr = serializeData(updated.data);

    this.stmtUpdateScheduledJob.run({
      id,
      name: updated.name,
      type: updated.type,
      schedule_type: updated.scheduleType,
      schedule_value: updated.scheduleValue,
      data: dataStr,
      enabled: fromBoolean(updated.enabled),
      last_scheduled_at: fromDate(updated.lastScheduledAt),
      next_run_at: fromDate(updated.nextRunAt),
      updated_at: updated.updatedAt.toISOString(),
    });
  }

  /**
   * Retrieve a scheduled job by its unique identifier
   * @param id - Unique identifier of the scheduled job to retrieve
   * @returns Scheduled job data or null if not found
   */
  async getScheduledJob(id: string): Promise<ScheduledJob | null> {
    const row = this.stmtSelectScheduledJobById.get(id);
    if (!row) {
      return null;
    }
    return this.mapRowToScheduledJob(row);
  }

  /**
   * List scheduled jobs based on the provided filter criteria
   * @param filter - Filter criteria (optional)
   * @returns Array of scheduled jobs matching the filter criteria
   */
  async listScheduledJobs(filter?: {
    enabled?: boolean;
    nextRunBefore?: Date;
  }): Promise<ScheduledJob[]> {
    if (!filter || (filter.enabled === undefined && !filter.nextRunBefore)) {
      // eslint-disable-next-line @typescript-eslint/no-explicit-any
      const rows: any[] = this.stmtSelectAllScheduledJobs.all([]);
      return rows.map((r) => this.mapRowToScheduledJob(r));
    }

    // eslint-disable-next-line @typescript-eslint/no-explicit-any
    const rows: any[] = this.stmtSelectFilteredScheduledJobs.all({
      enabled: filter.enabled !== undefined ? fromBoolean(filter.enabled) : null,
      next_run_before: filter.nextRunBefore ? filter.nextRunBefore.toISOString() : null,
    });
    return rows.map((r) => this.mapRowToScheduledJob(r));
  }

  /**
   * Create a new dead letter job
   * @param job - Dead letter job data to create
   */
  async createDeadLetterJob(job: CreateDeadLetterJob): Promise<void> {
    const validJob = CreateDeadLetterJobSchema.parse(job);

    const newJob = DeadLetterJobSchema.parse({
      ...validJob,
      id: randomUUID(),
    });

    const dataStr = serializeData(newJob.data);

    this.stmtInsertDeadLetterJob.run({
      id: newJob.id,
      job_id: newJob.jobId,
      job_type: newJob.jobType,
      data: dataStr,
      failed_at: newJob.failedAt.toISOString(),
      reason: newJob.reason,
    });
  }

  /**
   * List dead letter jobs
   * @returns Array of dead letter jobs
   */
  async listDeadLetterJobs(): Promise<DeadLetterJob[]> {
    // eslint-disable-next-line @typescript-eslint/no-explicit-any
    const rows: any[] = this.stmtSelectAllDeadLetterJobs.all([]);
    return rows.map((r) =>
      DeadLetterJobSchema.parse({
        id: r.id,
        jobId: r.job_id,
        jobType: r.job_type,
        data: parseJSON(r.data),
        failedAt: toDate(r.failed_at)!,
        reason: r.reason,
      })
    );
  }

  /**
   * Acquire a lock for a specific job
   * @param lockId - Unique identifier of the lock
   * @param owner - Identifier of the lock owner
   * @param ttlMs - Time to live for the lock (in milliseconds)
   * @returns True if the lock was acquired, false otherwise
   */
  async acquireLock(lockId: string, owner: string, ttlMs: number): Promise<boolean> {
    return this.transaction(async () => {
      // Calculate lock expiry time
      const now = new Date();
      const expiresAt = new Date(now.getTime() + ttlMs);

      // Clean up any expired locks first
      this.stmtDeleteExpiredLocks.run({ expires_at: fromDate(now) });

      // Check if lock already exists
      const existing = this.stmtSelectLock.get(lockId);
      if (existing) {
        return false;
      }

      // Create new lock
      this.stmtInsertLock.run({
        id: lockId,
        owner,
        acquired_at: fromDate(now),
        expires_at: fromDate(expiresAt),
        created_at: fromDate(now),
      });

      return true;
    });
  }

  /**
   * Renew an existing lock for a specific job
   * @param lockId - Unique identifier of the lock
   * @param owner - Identifier of the lock owner
   * @param ttlMs - Time to live for the lock (in milliseconds)
   * @returns True if the lock was renewed, false otherwise
   */
  async renewLock(lockId: string, owner: string, ttlMs: number): Promise<boolean> {
    return this.transaction(async () => {
      // Calculate new lock expiry time
      const now = new Date();
      const expiresAt = new Date(now.getTime() + ttlMs);

      // Clean up any expired locks first
      this.stmtDeleteExpiredLocks.run({ expires_at: fromDate(now) });

      // Check if lock exists and belongs to owner
      // eslint-disable-next-line @typescript-eslint/no-explicit-any
      const existing: any = this.stmtSelectLock.get(lockId);
      if (!existing || existing.owner !== owner) {
        return false;
      }

      // Update lock expiry
      this.stmtUpdateLock.run({
        id: lockId,
        owner,
        now: fromDate(now),
        expires_at: fromDate(expiresAt),
      });

      return true;
    });
  }

  /**
   * Release a lock for a specific job
   * @param lockId - Unique identifier of the lock
   * @param owner - Identifier of the lock owner
   * @returns True if the lock was released, false otherwise
   */
  async releaseLock(lockId: string, owner: string): Promise<boolean> {
    return this.transaction(async () => {
      const result = this.stmtDeleteLock.run({
        id: lockId,
        owner,
      });
      return result.changes > 0;
    });
  }

  /**
   * Clean up jobs and related data based on the provided retention periods
   * @param options - Retention periods for jobs, failed jobs, and dead letter jobs
   */
  async cleanup(options: {
    jobRetention: number;
    failedJobRetention: number;
    deadLetterRetention: number;
  }): Promise<void> {
    return this.transaction(async () => {
      const now = new Date();
      const DAY = 24 * 60 * 60 * 1000;

      // Clean up completed jobs based on retention period
      this.stmtCleanupCompleted.run({
        updated_at: fromDate(new Date(now.getTime() - options.jobRetention * DAY)),
      });

      // Clean up failed jobs based on retention period
      this.stmtCleanupFailed.run({
        updated_at: fromDate(new Date(now.getTime() - options.failedJobRetention * DAY)),
      });

      // Clean up dead letter jobs based on retention period
      this.stmtDeleteDeadLetterBefore.run({
        failed_at: fromDate(new Date(now.getTime() - options.deadLetterRetention * DAY)),
      });
    });
  }

  /**
   * Delete a job and its related data (runs, logs)
   * @param jobId - Unique identifier of the job to delete
   */
  async deleteJobAndRelatedData(jobId: string): Promise<void> {
    return this.transaction(async () => {
      // Delete all related data in a single transaction
      this.stmtDeleteJobLogsByJobId.run(jobId);
      this.stmtDeleteJobRunsByJobId.run(jobId);
      this.stmtDeleteJob.run(jobId);
    });
  }

  /**
   * Get storage metrics including job counts and performance metrics
   * @returns Storage metrics
   */
  async getMetrics(): Promise<JobStorageMetrics> {
    // eslint-disable-next-line @typescript-eslint/no-explicit-any
    const jobCounts: any = this.stmtJobCounts.get([]);
    const avgDurationRows = this.stmtAvgDuration.all([]);
    const failureRateRows = this.stmtFailureRate.all([]);

    const averageDurationByType: Record<string, number> = {};
    const failureRateByType: Record<string, number> = {};

    // eslint-disable-next-line @typescript-eslint/no-explicit-any
    avgDurationRows.forEach((row: any) => {
      averageDurationByType[row.type] = row.avg_duration;
    });

    // eslint-disable-next-line @typescript-eslint/no-explicit-any
    failureRateRows.forEach((row: any) => {
      failureRateByType[row.type] = row.failure_rate;
    });

    return {
      averageDurationByType,
      failureRateByType,
      jobs: {
        total: jobCounts.total ?? 0,
        pending: jobCounts.pending ?? 0,
        completed: jobCounts.completed ?? 0,
        failed: jobCounts.failed ?? 0,
        deadLetter: jobCounts.dead_letter ?? 0,
        scheduled: jobCounts.scheduled ?? 0,
      },
    };
  }

  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  private mapRowToJob(row: any): Job {
    return JobSchema.parse({
      id: row.id,
      type: row.type,
      status: row.status,
      data: parseJSON(row.data),
      priority: row.priority,
      attempts: row.attempts,
      maxRetries: row.max_retries,
      backoffStrategy: row.backoff_strategy,
      failReason: row.fail_reason ?? undefined,
      failCount: row.fail_count,
      executionDuration: row.execution_duration ?? undefined,
      referenceId: row.reference_id ?? undefined,
      expiresAt: toDate(row.expires_at) ?? undefined,
      lockedAt: toDate(row.locked_at) ?? undefined,
      startedAt: toDate(row.started_at) ?? undefined,
      runAt: toDate(row.run_at) ?? undefined,
      finishedAt: toDate(row.finished_at) ?? undefined,
      createdAt: new Date(row.created_at),
      updatedAt: new Date(row.updated_at),
    });
  }

  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  private mapRowToJobRun(row: any): JobRun {
    return JobRunSchema.parse({
      id: row.id,
      jobId: row.job_id,
      status: row.status,
      startedAt: toDate(row.started_at),
      finishedAt: toDate(row.finished_at),
      attempt: row.attempt,
      error: row.error ?? undefined,
      error_stack: row.error_stack ?? undefined,
    });
  }

  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  private mapRowToJobLog(row: any): JobLogEntry {
    return JobLogEntrySchema.parse({
      id: row.id,
      jobId: row.job_id,
      jobRunId: row.job_run_id ?? undefined,
      timestamp: new Date(row.timestamp),
      level: row.level,
      message: row.message,
      metadata: row.metadata ? parseJSON(row.metadata) : undefined,
    });
  }

  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  private mapRowToScheduledJob(row: any): ScheduledJob {
    return ScheduledJobSchema.parse({
      id: row.id,
      name: row.name,
      type: row.type,
      scheduleType: row.schedule_type,
      scheduleValue: row.schedule_value,
      data: parseJSON(row.data),
      enabled: toBoolean(row.enabled),
      lastScheduledAt: toDate(row.last_scheduled_at) ?? undefined,
      nextRunAt: toDate(row.next_run_at) ?? undefined,
      createdAt: toDate(row.created_at),
      updatedAt: toDate(row.updated_at),
    });
  }
}
