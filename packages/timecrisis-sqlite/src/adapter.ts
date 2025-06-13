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
  Worker,
  WorkerSchema,
  RegisterWorker,
  UpdateWorkerHeartbeat,
  WorkerNotFoundError,
} from '@timecrisis/timecrisis';

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
   * Prepared statements.
   */
  private stmtInsertJob!: ReturnType<Database['prepare']>;
  private stmtSelectJobById!: ReturnType<Database['prepare']>;
  private stmtUpdateJob!: ReturnType<Database['prepare']>;
  private stmtDeleteJob!: ReturnType<Database['prepare']>;
  private stmtSelectFilteredJobs!: ReturnType<Database['prepare']>;
  private stmtInsertJobRun!: ReturnType<Database['prepare']>;
  private stmtSelectJobRunById!: ReturnType<Database['prepare']>;
  private stmtUpdateJobRun!: ReturnType<Database['prepare']>;
  private stmtSelectJobRunsByJobId!: ReturnType<Database['prepare']>;
  private stmtDeleteJobRunsByJobId!: ReturnType<Database['prepare']>;
  private stmtInsertJobLog!: ReturnType<Database['prepare']>;
  private stmtSelectJobLogsByJobId!: ReturnType<Database['prepare']>;
  private stmtSelectJobLogsByJobAndRun!: ReturnType<Database['prepare']>;
  private stmtDeleteJobLogsByJobId!: ReturnType<Database['prepare']>;
  private stmtInsertScheduledJob!: ReturnType<Database['prepare']>;
  private stmtSelectScheduledJobById!: ReturnType<Database['prepare']>;
  private stmtUpdateScheduledJob!: ReturnType<Database['prepare']>;
  private stmtSelectAllScheduledJobs!: ReturnType<Database['prepare']>;
  private stmtSelectFilteredScheduledJobs!: ReturnType<Database['prepare']>;
  private stmtInsertDeadLetterJob!: ReturnType<Database['prepare']>;
  private stmtSelectAllDeadLetterJobs!: ReturnType<Database['prepare']>;
  private stmtDeleteDeadLetterBefore!: ReturnType<Database['prepare']>;
  private stmtListLocks!: ReturnType<Database['prepare']>;
  private stmtInsertLock!: ReturnType<Database['prepare']>;
  private stmtUpdateLock!: ReturnType<Database['prepare']>;
  private stmtDeleteLock!: ReturnType<Database['prepare']>;
  private stmtCleanupCompleted!: ReturnType<Database['prepare']>;
  private stmtCleanupFailed!: ReturnType<Database['prepare']>;
  private stmtJobCounts!: ReturnType<Database['prepare']>;
  private stmtAvgDuration!: ReturnType<Database['prepare']>;
  private stmtFailureRate!: ReturnType<Database['prepare']>;
  private stmtInsertTypeSlot!: ReturnType<Database['prepare']>;
  private stmtDecrementTypeSlot!: ReturnType<Database['prepare']>;
  private stmtDeleteEmptyTypeSlots!: ReturnType<Database['prepare']>;
  private stmtDeleteWorkerTypeSlots!: ReturnType<Database['prepare']>;
  private stmtGetTotalRunningJobs!: ReturnType<Database['prepare']>;
  private stmtGetTotalRunningJobsByType!: ReturnType<Database['prepare']>;
  private stmtInsertWorker!: ReturnType<Database['prepare']>;
  private stmtUpdateWorkerHeartbeat!: ReturnType<Database['prepare']>;
  private stmtSelectWorkerByName!: ReturnType<Database['prepare']>;
  private stmtSelectInactiveWorkers!: ReturnType<Database['prepare']>;
  private stmtSelectAllWorkers!: ReturnType<Database['prepare']>;
  private stmtDeleteWorker!: ReturnType<Database['prepare']>;

  constructor(db: Database) {
    this.db = db;
  }

  /**
   * Initialize the SQLite storage provider
   * This will run any pending migrations and prepare all SQL statements
   * for improved performance
   */
  async init(
    { runMigrations }: { runMigrations?: boolean } = { runMigrations: true }
  ): Promise<void> {
    if (runMigrations) {
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
    this.stmtSelectAllScheduledJobs = this.db.prepare(SQL.selectAllScheduledJobs);
    this.stmtSelectFilteredScheduledJobs = this.db.prepare(SQL.selectFilteredScheduledJobs);

    this.stmtInsertDeadLetterJob = this.db.prepare(SQL.insertDeadLetterJob);
    this.stmtSelectAllDeadLetterJobs = this.db.prepare(SQL.selectAllDeadLetterJobs);
    this.stmtDeleteDeadLetterBefore = this.db.prepare(SQL.deleteDeadLetterBefore);

    this.stmtListLocks = this.db.prepare(SQL.listLocks);
    this.stmtInsertLock = this.db.prepare(SQL.insertLock);
    this.stmtUpdateLock = this.db.prepare(SQL.updateLock);
    this.stmtDeleteLock = this.db.prepare(SQL.deleteLock);

    this.stmtCleanupCompleted = this.db.prepare(SQL.cleanupCompleted);
    this.stmtCleanupFailed = this.db.prepare(SQL.cleanupFailed);

    this.stmtJobCounts = this.db.prepare(SQL.jobCounts);
    this.stmtAvgDuration = this.db.prepare(SQL.avgDuration);
    this.stmtFailureRate = this.db.prepare(SQL.failureRate);

    this.stmtInsertTypeSlot = this.db.prepare(SQL.upsertJobTypeSlot);
    this.stmtDecrementTypeSlot = this.db.prepare(SQL.decrementJobTypeSlot);
    this.stmtDeleteEmptyTypeSlots = this.db.prepare(SQL.deleteEmptyJobTypeSlots);
    this.stmtDeleteWorkerTypeSlots = this.db.prepare(SQL.deleteWorkerJobTypeSlots);
    this.stmtGetTotalRunningJobs = this.db.prepare(SQL.getTotalRunningJobs);
    this.stmtGetTotalRunningJobsByType = this.db.prepare(SQL.getTotalRunningJobsByType);
    this.stmtInsertWorker = this.db.prepare(SQL.insertWorker);
    this.stmtUpdateWorkerHeartbeat = this.db.prepare(SQL.updateWorkerHeartbeat);
    this.stmtSelectWorkerByName = this.db.prepare(SQL.selectWorkerByName);
    this.stmtSelectInactiveWorkers = this.db.prepare(SQL.selectInactiveWorkers);
    this.stmtSelectAllWorkers = this.db.prepare(SQL.selectAllWorkers);
    this.stmtDeleteWorker = this.db.prepare(SQL.deleteWorker);
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
  async transaction<T>(fn: (trx: unknown) => T): Promise<T> {
    return new Promise((resolve, reject) => {
      try {
        const wrapped = this.db.transaction((_: unknown) => {
          try {
            // Execute the function synchronously
            const result = fn(null);
            // Return the result directly
            return result;
          } catch (error) {
            reject(error);
            throw error; // Re-throw to ensure transaction rollback
          }
        });

        // Execute the transaction and handle the Promise
        const result = wrapped(null);
        resolve(result);
      } catch (error) {
        reject(error);
      }
    });
  }

  /**
   * Create a new job in the database
   * @param job - Job data to create
   * @returns Unique identifier of the created job
   */
  async createJob(job: CreateJob): Promise<string> {
    // Parse and validate the input using Zod schema
    const validJob = CreateJobSchema.strict().parse(job);

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
      status: newJob.status,
      data: serializeData(newJob.data),
      priority: newJob.priority,
      max_retries: newJob.maxRetries,
      backoff_strategy: newJob.backoffStrategy,
      fail_reason: newJob.failReason ?? null,
      fail_count: newJob.failCount,
      entity_id: newJob.entityId ?? null,
      expires_at: fromDate(newJob.expiresAt),
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
  async getJob(id: string): Promise<Job | undefined> {
    const row = this.stmtSelectJobById.get(id);
    if (!row) {
      return undefined;
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
    const validUpdates = UpdateJobSchema.strict().parse(updates);

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
      status: updatedJob.status,
      data: serializeData(updatedJob.data),
      priority: updatedJob.priority,
      max_retries: updatedJob.maxRetries,
      backoff_strategy: updatedJob.backoffStrategy,
      fail_reason: updatedJob.failReason ?? null,
      fail_count: updatedJob.failCount,
      entity_id: updatedJob.entityId ?? null,
      expires_at: fromDate(updatedJob.expiresAt),
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
    const valid = CreateJobRunSchema.strict().parse(jobRun);

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
      progress: newRun.progress,
      finished_at: fromDate(newRun.finishedAt),
      execution_duration: newRun.executionDuration,
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
    const updatedRun = JobRunSchema.strict().parse({
      ...existing,
      ...updates,
    });

    // Update the job run in the database
    this.stmtUpdateJobRun.run({
      id: updatedRun.id,
      status: updatedRun.status,
      started_at: fromDate(updatedRun.startedAt),
      progress: updatedRun.progress,
      finished_at: fromDate(updatedRun.finishedAt),
      execution_duration: updatedRun.executionDuration,
      attempt: updatedRun.attempt,
      error: updatedRun.error ?? null,
      error_stack: updatedRun.error_stack ?? null,
    });
  }

  /**
   * Retrieve a job run by its unique identifier
   * @param id - Unique identifier of the job run to retrieve
   * @returns Job run data or undefined if not found
   */
  async getJobRun(id: string): Promise<JobRun | undefined> {
    const row = this.stmtSelectJobRunById.get(id);
    if (!row) {
      return undefined;
    }
    return this.mapRowToJobRun(row);
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
    entityId?: string;
    runAtBefore?: Date;
    limit?: number;
    expiresAtBefore?: Date;
  }): Promise<Job[]> {
    // eslint-disable-next-line @typescript-eslint/no-explicit-any
    const params: any = {
      type: filter?.type || null,
      entityId: filter?.entityId || null,
      runAtBefore: filter?.runAtBefore ? fromDate(filter.runAtBefore) : null,
      limit: filter?.limit || null,
      status: filter?.status ? JSON.stringify(filter.status) : null,
      expiresAtBefore: filter?.expiresAtBefore ? fromDate(filter.expiresAtBefore) : null,
    };

    const rows = this.stmtSelectFilteredJobs.all(params);
    return rows.map((row) => this.mapRowToJob(row));
  }

  /**
   * Create a new log entry for a job run
   * @param log - Log entry data to create
   */
  async createJobLog(log: CreateJobLog): Promise<void> {
    const validLog = CreateJobLogSchema.strict().parse(log);

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
    const validJob = CreateScheduledJobSchema.strict().parse(job);

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
      time_zone: newJob.timeZone ?? null,
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

    const validUpdates = UpdateScheduledJobSchema.strict().parse(updates);
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
      time_zone: updated.timeZone ?? null,
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
    const validJob = CreateDeadLetterJobSchema.strict().parse(job);

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
      failed_reason: newJob.failReason,
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
        failReason: r.failed_reason,
      })
    );
  }

  /**
   * Acquire a lock for a specific job
   * @param lockId - Unique identifier of the lock
   * @param worker - Identifier of the lock worker
   * @param ttlMs - Time to live for the lock (in milliseconds)
   * @returns True if the lock was acquired, false otherwise
   */
  async acquireLock(lockId: string, worker: string, ttlMs: number): Promise<boolean> {
    const now = new Date();
    const expiresAt = new Date(now.getTime() + ttlMs);

    return this.transaction(async () => {
      // Create new lock
      const res = this.stmtInsertLock.run({
        id: lockId,
        worker,
        now: now.toISOString(),
        acquired: now.toISOString(),
        expires: expiresAt.toISOString(),
        created: now.toISOString(),
      });
      return res.changes > 0;
    });
  }

  /**
   * Renew an existing lock for a specific job
   * @param lockId - Unique identifier of the lock
   * @param worker - Identifier of the lock worker
   * @param ttlMs - Time to live for the lock (in milliseconds)
   * @returns True if the lock was renewed, false otherwise
   */
  async renewLock(lockId: string, worker: string, ttlMs: number): Promise<boolean> {
    const now = new Date();
    const expiresAt = new Date(now.getTime() + ttlMs);

    return this.transaction(async () => {
      // Update lock expiry
      const res = this.stmtUpdateLock.run({
        lockId,
        worker,
        now: now.toISOString(),
        newExpiry: expiresAt.toISOString(),
      });

      return res.changes > 0;
    });
  }

  /**
   * Release a lock.
   * @param lockId - Unique identifier of the lock
   * @param worker - Identifier of the lock worker
   * @returns True if the lock was released, false otherwise
   */
  async releaseLock(lockId: string, worker: string): Promise<boolean> {
    return this.transaction(async () => {
      const result = this.stmtDeleteLock.run({
        id: lockId,
        worker,
      });
      return result.changes > 0;
    });
  }

  /**
   * List all locks owned by a specific worker.
   * @returns An array of objects with lock details, each containing lockId, worker, and expiresAt
   */
  async listLocks(filters?: {
    worker?: string;
    expiredBefore?: Date;
  }): Promise<{ lockId: string; worker: string; expiresAt: Date }[]> {
    const rows = this.stmtListLocks.all({
      worker: filters?.worker,
      expiredBefore: filters?.expiredBefore ? fromDate(filters.expiredBefore) : null,
    });
    return rows.map((r) => this.mapRowToLock(r));
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
  }

  /**
   * Delete a job and its related data (runs, logs)
   * @param jobId - Unique identifier of the job to delete
   */
  async deleteJobAndRelatedData(jobId: string): Promise<void> {
    // Delete all related data in a single transaction
    this.stmtDeleteJobLogsByJobId.run(jobId);
    this.stmtDeleteJobRunsByJobId.run(jobId);
    this.stmtDeleteJob.run(jobId);
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

  /**
   * Acquire a slot for a specific job type and worker
   * @param jobType - The type of the job
   * @param worker - The ID of the worker requesting the slot
   * @param maxConcurrent - Maximum allowed concurrent jobs for this type
   * @returns True if the slot was acquired, false otherwise
   */
  async acquireTypeSlot(jobType: string, worker: string, maxConcurrent: number): Promise<boolean> {
    try {
      const result = await this.transaction(async () => {
        // Get total slots used for this job type
        const totalSlots = this.stmtGetTotalRunningJobsByType.get(jobType) as { total: number };
        if (totalSlots.total >= maxConcurrent) {
          return false;
        }

        const res = this.stmtInsertTypeSlot.run({
          job_type: jobType,
          worker: worker,
          max_concurrent: maxConcurrent,
        });
        // If changes > 0, we successfully inserted/updated a slot
        return res.changes > 0;
      });
      return result;
    } catch (error) {
      if (
        error instanceof Error &&
        (error.message.includes('SQLITE_BUSY') ||
          error.message.includes('cannot start a transaction within a transaction'))
      ) {
        return false;
      }
      throw error;
    }
  }

  /**
   * Release a slot for a specific job type and worker
   * @param jobType - The type of the job
   * @param worker - The worker releasing the slot
   */
  async releaseTypeSlot(jobType: string, worker: string): Promise<void> {
    return this.transaction(async () => {
      // Decrement the slot count
      this.stmtDecrementTypeSlot.run({
        job_type: jobType,
        worker: worker,
      });

      // Clean up any slots that are now empty
      this.stmtDeleteEmptyTypeSlots.run([]);
    });
  }

  /**
   * Release all slots held by a specific worker
   * @param worker - The worker to release all slots for
   */
  async releaseAllTypeSlots(worker: string): Promise<void> {
    return this.transaction(async () => {
      this.stmtDeleteWorkerTypeSlots.run(worker);
    });
  }

  /**
   * Get the current running count for a specific job type or total across all types
   * @param jobType - Optional, the type of the job to count. If not provided, returns total across all types
   * @returns Number of currently running jobs
   */
  async getRunningCount(jobType?: string): Promise<number> {
    if (jobType) {
      const result = this.stmtGetTotalRunningJobsByType.get(jobType) as { total: number };
      return result.total;
    } else {
      const result = this.stmtGetTotalRunningJobs.get([]) as { total: number };
      return result.total;
    }
  }

  /**
   * Register a new worker instance in the system
   * @param worker - Worker registration data containing the worker name
   * @returns Promise that resolves with the ID of the registered worker
   * @throws ZodError if the worker registration data is invalid
   */
  async registerWorker(worker: RegisterWorker): Promise<string> {
    const now = new Date();
    const workerInstance = WorkerSchema.strict().parse({
      ...worker,
      first_seen: now,
      last_heartbeat: now,
    });

    this.stmtInsertWorker.run({
      name: workerInstance.name,
      first_seen: workerInstance.first_seen.toISOString(),
      last_heartbeat: workerInstance.last_heartbeat.toISOString(),
    });

    return workerInstance.name;
  }

  /**
   * Update a worker's heartbeat timestamp
   * @param workerName - Name of the worker to update
   * @param heartbeat - Heartbeat data containing the new timestamp
   * @throws WorkerNotFoundError if the worker doesn't exist
   * @throws ZodError if the heartbeat data is invalid
   */
  async updateWorkerHeartbeat(workerName: string, heartbeat: UpdateWorkerHeartbeat): Promise<void> {
    const worker = await this.getWorker(workerName);
    if (!worker) {
      throw new WorkerNotFoundError(workerName);
    }

    this.stmtUpdateWorkerHeartbeat.run({
      name: workerName,
      last_heartbeat: heartbeat.last_heartbeat.toISOString(),
    });
  }

  /**
   * Get a worker by its name
   * @param workerName - Name of the worker to retrieve
   * @returns Promise that resolves with the worker data or null if not found
   */
  async getWorker(workerName: string): Promise<Worker | null> {
    // eslint-disable-next-line @typescript-eslint/no-explicit-any
    const worker = this.stmtSelectWorkerByName.get(workerName) as any;
    if (!worker) {
      return null;
    }

    return WorkerSchema.parse({
      ...worker,
      first_seen: new Date(worker.first_seen),
      last_heartbeat: new Date(worker.last_heartbeat),
    });
  }

  /**
   * Get all workers that haven't sent a heartbeat since the specified time
   * @param lastHeartbeatBefore - Time threshold for considering workers inactive
   * @returns Promise that resolves with an array of inactive workers
   */
  async getInactiveWorkers(lastHeartbeatBefore: Date): Promise<Worker[]> {
    // eslint-disable-next-line @typescript-eslint/no-explicit-any
    const workers = this.stmtSelectInactiveWorkers.all(lastHeartbeatBefore.toISOString()) as any[];

    return workers.map((worker) =>
      WorkerSchema.parse({
        ...worker,
        first_seen: new Date(worker.first_seen),
        last_heartbeat: new Date(worker.last_heartbeat),
      })
    );
  }

  /**
   * Get all registered workers in the system
   * @returns Promise that resolves with an array of all workers
   */
  async getWorkers(): Promise<Worker[]> {
    // eslint-disable-next-line @typescript-eslint/no-explicit-any
    const workers = this.stmtSelectAllWorkers.all([]) as any[];
    return workers.map((worker) =>
      WorkerSchema.parse({
        ...worker,
        first_seen: new Date(worker.first_seen),
        last_heartbeat: new Date(worker.last_heartbeat),
      })
    );
  }

  /**
   * Delete a worker by its name
   * @param workerName - Name of the worker to delete
   * @throws WorkerNotFoundError if the worker doesn't exist
   */
  async deleteWorker(workerName: string): Promise<void> {
    const worker = await this.getWorker(workerName);
    if (!worker) {
      throw new WorkerNotFoundError(workerName);
    }

    this.stmtDeleteWorker.run(workerName);
  }

  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  private mapRowToLock(row: any): { lockId: string; worker: string; expiresAt: Date } {
    return {
      lockId: row.id,
      worker: row.worker,
      expiresAt: new Date(row.expires_at),
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
      maxRetries: row.max_retries,
      backoffStrategy: row.backoff_strategy,
      failReason: row.fail_reason ?? undefined,
      failCount: row.fail_count,
      entityId: row.entity_id ?? undefined,
      expiresAt: toDate(row.expires_at) ?? undefined,
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
      progress: row.progress,
      finishedAt: toDate(row.finished_at),
      executionDuration: row.execution_duration,
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
