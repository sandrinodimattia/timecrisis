import { vi } from 'vitest';
import { randomUUID } from 'crypto';

import {
  CreateDeadLetterJob,
  CreateJob,
  CreateJobLog,
  CreateJobLogSchema,
  CreateJobRun,
  CreateJobRunSchema,
  CreateJobSchema,
  CreateScheduledJob,
  CreateScheduledJobSchema,
  DeadLetterJob,
  DeadLetterJobSchema,
  Job,
  JobLogEntry,
  JobLogEntrySchema,
  JobRun,
  JobRunSchema,
  JobSchema,
  JobStorageMetrics,
  ScheduledJob,
  ScheduledJobSchema,
  UpdateJob,
  UpdateJobRun,
  UpdateJobSchema,
  UpdateScheduledJob,
  UpdateScheduledJobSchema,
} from '../schemas/index.js';

import {
  JobStorage,
  JobNotFoundError,
  JobRunNotFoundError,
  ScheduledJobNotFoundError,
} from '../types.js';

export interface MockStorageOptions {
  shouldFailAcquire?: boolean;
  shouldFailRelease?: boolean;
  shouldFailExtend?: boolean;
  shouldFailTransaction?: boolean;
}

export class MockJobStorage implements JobStorage {
  private locks: Map<string, { owner: string; expiresAt: number }> = new Map();
  private jobs: Map<string, Job> = new Map();
  private jobRuns: Map<string, JobRun> = new Map();
  private jobLogs: Map<string, JobLogEntry[]> = new Map();
  private scheduledJobs: Map<string, ScheduledJob> = new Map();
  private deadLetterJobs: Map<string, DeadLetterJob> = new Map();

  constructor(private options: MockStorageOptions = {}) {
    this.init = vi.fn(this.init.bind(this));
    this.transaction = vi
      .fn()
      .mockImplementation(<T>(fn: (trx: unknown) => Promise<T>) => fn(null));
    this.createJob = vi.fn(this.createJob.bind(this));
    this.getJob = vi.fn(this.getJob.bind(this));
    this.updateJob = vi.fn(this.updateJob.bind(this));
    this.listJobs = vi.fn(this.listJobs.bind(this));
    this.createJobRun = vi.fn(this.createJobRun.bind(this));
    this.updateJobRun = vi.fn(this.updateJobRun.bind(this));
    this.listJobRuns = vi.fn(this.listJobRuns.bind(this));
    this.createJobLog = vi.fn(this.createJobLog.bind(this));
    this.listJobLogs = vi.fn(this.listJobLogs.bind(this));
    this.createScheduledJob = vi.fn(this.createScheduledJob.bind(this));
    this.getScheduledJob = vi.fn(this.getScheduledJob.bind(this));
    this.updateScheduledJob = vi.fn(this.updateScheduledJob.bind(this));
    this.listScheduledJobs = vi.fn(this.listScheduledJobs.bind(this));
    this.createDeadLetterJob = vi.fn(this.createDeadLetterJob.bind(this));
    this.listDeadLetterJobs = vi.fn(this.listDeadLetterJobs.bind(this));
    this.cleanup = vi.fn(this.cleanup.bind(this));
    this.getMetrics = vi.fn(this.getMetrics.bind(this));
    this.acquireLock = vi.fn(this.acquireLock.bind(this));
    this.renewLock = vi.fn(this.renewLock.bind(this));
    this.releaseLock = vi.fn(this.releaseLock.bind(this));
    this.simulateOtherLeader = vi.fn(this.simulateOtherLeader.bind(this));
  }

  /**
   * Set a lock directly for testing purposes
   */
  public setLock(lockId: string, owner: string, expiresAt: number): void {
    this.locks.set(lockId, { owner, expiresAt });
  }

  /**
   * Reset all internal storage maps to their initial empty state
   */
  public reset(): void {
    this.locks.clear();
    this.jobs.clear();
    this.jobRuns.clear();
    this.jobLogs.clear();
    this.scheduledJobs.clear();
    this.deadLetterJobs.clear();
  }

  /**
   * Update the mock storage options
   * @param options - New options to merge with existing ones
   */
  public setOptions(options: MockStorageOptions): void {
    this.options = { ...this.options, ...options };
  }

  /**
   * Initialize the storage provider
   * No-op for mock storage
   */
  async init(): Promise<void> {
    // Noop
  }

  /**
   * Execute a function within a transaction
   * @param fn - Function to execute within the transaction
   * @returns Promise of the function result
   * @throws Error if shouldFailTransaction is true
   */
  async transaction<T>(fn: (trx: unknown) => Promise<T>): Promise<T> {
    if (this.options.shouldFailTransaction) {
      throw new Error('Transaction failed');
    }

    return fn(null);
  }

  /**
   * Create a new job with the given partial job data
   * @param job - Partial job data to create the job with
   * @returns Promise of the created job's ID
   */
  async createJob(job: CreateJob): Promise<string> {
    const id = randomUUID();
    const now = new Date();

    // Parse and validate the input, applying defaults
    const validJob = CreateJobSchema.parse(job);

    // Create the full job object
    const newJob = JobSchema.parse({
      ...validJob,
      id,
      createdAt: now,
      updatedAt: now,
    });

    this.jobs.set(id, newJob);
    return id;
  }

  /**
   * Get a job by its ID
   * @param id - ID of the job to retrieve
   * @returns Promise of the job or null if not found
   */
  async getJob(id: string): Promise<Job | null> {
    return this.jobs.get(id) || null;
  }

  /**
   * Update a job with the provided updates
   * @param id - ID of the job to update
   * @param updates - Partial updates to apply to the job
   */
  async updateJob(id: string, updates: UpdateJob): Promise<void> {
    const job = this.jobs.get(id);
    if (!job) {
      throw new JobNotFoundError(id);
    }

    // Parse and validate the updates
    const validUpdates = UpdateJobSchema.parse(updates);

    // Create the updated job object
    const updatedJob = JobSchema.parse({
      ...job,
      ...validUpdates,
      updatedAt: new Date(),
    });

    this.jobs.set(id, updatedJob);
  }

  /**
   * List jobs matching the optional query
   * @param query - Optional partial job data to filter by
   * @returns Promise of matching jobs
   */
  async listJobs(query?: {
    status?: string[];
    type?: string;
    referenceId?: string;
    lockedBefore?: Date;
    runAtBefore?: Date;
    limit?: number;
  }): Promise<Job[]> {
    let jobs = Array.from(this.jobs.values());

    if (query?.type) {
      jobs = jobs.filter((job) => job.type === query.type);
    }

    if (query?.status && query.status.length > 0) {
      jobs = jobs.filter((job) => query.status!.includes(job.status));
    }

    if (query?.referenceId) {
      jobs = jobs.filter((job) => job.referenceId === query.referenceId);
    }

    if (query?.lockedBefore) {
      jobs = jobs.filter((job) => {
        const lock = this.locks.get(job.id);
        return !lock || lock.expiresAt <= query.lockedBefore!.getTime();
      });
    }

    if (query?.runAtBefore) {
      jobs = jobs.filter((job) => !job.runAt || job.runAt <= query.runAtBefore!);
    }

    if (query?.limit && query.limit > 0) {
      jobs = jobs.slice(0, query.limit);
    }

    return jobs;
  }

  /**
   * Create a new job run with the given partial job run data
   * @param jobRun - Partial job run data to create the run with
   * @returns Promise of the created job run's ID
   */
  async createJobRun(jobRun: CreateJobRun): Promise<string> {
    const id = randomUUID();
    const now = new Date();

    // Parse and validate the input, applying defaults
    const validJobRun = CreateJobRunSchema.parse(jobRun);

    // Create the full job run object
    const newJobRun = JobRunSchema.parse({
      ...validJobRun,
      id,
      startedAt: validJobRun.startedAt || now,
    });
    this.jobRuns.set(id, newJobRun);
    return id;
  }

  /**
   * Update a job run with the provided updates
   * This method follows the undefined vs null update pattern:
   * - If a field is undefined, it will not be updated
   * - If a field is null, it will be set to null
   * - If a field has a value, it will be updated to that value
   *
   * @param id - The ID of the job run to update
   * @param updates - Partial updates to apply to the job run
   * @returns Promise<void>
   */
  async updateJobRun(id: string, updates: UpdateJobRun): Promise<void> {
    const run = this.jobRuns.get(id);
    if (!run) {
      throw new JobRunNotFoundError(id);
    }

    const updatedRun = JobRunSchema.parse({
      ...run,
      ...updates,
    });
    this.jobRuns.set(id, updatedRun);
  }

  /**
   * List job runs for a specific job
   * @param jobId - ID of the job to list runs for
   * @returns Promise of job runs for the specified job
   */
  async listJobRuns(jobId: string): Promise<JobRun[]> {
    const runs = Array.from(this.jobRuns.values());
    return runs.filter((run) => run.jobId === jobId);
  }

  /**
   * Create a new job log entry
   * @param log - Log entry data without ID
   */
  async createJobLog(log: CreateJobLog): Promise<void> {
    const job = this.jobs.get(log.jobId);
    if (!job) {
      throw new JobNotFoundError(log.jobId);
    }

    // Parse and validate the input, applying defaults
    const validLog = CreateJobLogSchema.parse(log);

    // Create the full job run object
    const newLog = JobLogEntrySchema.parse({
      ...validLog,
      id: randomUUID(),
      timestamp: validLog.timestamp || new Date(),
    });

    const logs = this.jobLogs.get(log.jobId) || [];
    logs.push(newLog);
    this.jobLogs.set(log.jobId, logs);
  }

  /**
   * List log entries for a specific job
   * @param jobId - ID of the job to list logs for
   * @returns Promise of log entries for the specified job
   */
  async listJobLogs(jobId: string): Promise<JobLogEntry[]> {
    return this.jobLogs.get(jobId) || [];
  }

  /**
   * Get a scheduled job by ID
   * @param id - ID of the scheduled job to retrieve
   * @returns Promise of the scheduled job or null if not found
   */
  async getScheduledJob(id: string): Promise<ScheduledJob | null> {
    return this.scheduledJobs.get(id) || null;
  }

  /**
   * Create a new scheduled job with the given partial scheduled job data
   * @param job - Partial scheduled job data to create the job with
   * @returns Promise of the created scheduled job's ID
   */
  async createScheduledJob(job: CreateScheduledJob): Promise<string> {
    const id = randomUUID();
    const now = new Date();

    // Parse and validate the input, applying defaults
    const validJob = CreateScheduledJobSchema.parse(job);

    // Create the full job object
    const newJob = ScheduledJobSchema.parse({
      ...validJob,
      id,
      createdAt: now,
      updatedAt: now,
      enabled: true,
      data: validJob.data ?? {},
      lastScheduledAt: null,
      nextRunAt: null,
    });

    this.scheduledJobs.set(id, newJob);
    return id;
  }

  /**
   * Update a scheduled job with the provided updates
   * This method follows the undefined vs null update pattern:
   * - If a field is undefined, it will not be updated
   * - If a field is null, it will be set to null
   * - If a field has a value, it will be updated to that value
   *
   * The updatedAt timestamp is always updated to the current time
   * when any field is modified.
   *
   * @param id - The ID of the scheduled job to update
   * @param updates - Partial updates to apply to the scheduled job
   * @returns Promise<void>
   */
  async updateScheduledJob(id: string, updates: UpdateScheduledJob): Promise<void> {
    const job = this.scheduledJobs.get(id);
    if (!job) {
      throw new ScheduledJobNotFoundError(id);
    }

    // Parse and validate the input, applying defaults
    const validUpdates = UpdateScheduledJobSchema.parse(updates);

    // Create the full job object.
    const updatedJob = ScheduledJobSchema.parse({
      ...job,
      ...validUpdates,
      updatedAt: new Date(),
    });
    this.scheduledJobs.set(id, updatedJob);
  }

  /**
   * List scheduled jobs matching the optional query
   * @param query - Optional partial scheduled job data to filter by
   * @returns Promise of matching scheduled jobs
   */
  async listScheduledJobs(query?: Partial<ScheduledJob>): Promise<ScheduledJob[]> {
    const jobs = Array.from(this.scheduledJobs.values());
    if (!query) return jobs;
    return jobs.filter((job) =>
      Object.entries(query).every(([key, value]) => job[key as keyof ScheduledJob] === value)
    );
  }

  /**
   * Create a new dead letter job
   * @param job - Dead letter job data
   */
  async createDeadLetterJob(job: CreateDeadLetterJob): Promise<void> {
    const id = randomUUID();
    const newJob = DeadLetterJobSchema.parse({
      ...job,
      id,
    });
    this.deadLetterJobs.set(newJob.id, newJob);
  }

  /**
   * List all dead letter jobs
   * @returns Promise of all dead letter jobs
   */
  async listDeadLetterJobs(): Promise<DeadLetterJob[]> {
    return Array.from(this.deadLetterJobs.values());
  }

  /**
   * Clean up all storage data by resetting all maps
   */
  async cleanup(): Promise<void> {
    this.reset();
  }

  /**
   * Get storage metrics including job counts and performance metrics by type
   * @returns Promise of job storage metrics
   */
  async getMetrics(): Promise<JobStorageMetrics> {
    const jobs = Array.from(this.jobs.values());
    const averageDurationByType: Record<string, number> = {};
    const failureRateByType: Record<string, number> = {};

    // Calculate metrics by job type
    const jobsByType = new Map<string, Job[]>();
    jobs.forEach((job) => {
      const typeJobs = jobsByType.get(job.type) || [];
      typeJobs.push(job);
      jobsByType.set(job.type, typeJobs);
    });

    jobsByType.forEach((typeJobs, type) => {
      // Calculate average duration
      const completedJobs = typeJobs.filter(
        (j) => j.status === 'completed' && j.executionDuration !== undefined
      );

      if (completedJobs.length > 0) {
        const totalDuration = completedJobs.reduce((sum, j) => sum + (j.executionDuration || 0), 0);
        averageDurationByType[type] = totalDuration / completedJobs.length;
      }

      // Calculate failure rate
      const failedJobs = typeJobs.filter((j) => j.status === 'failed').length;
      failureRateByType[type] = failedJobs / typeJobs.length;
    });

    return {
      averageDurationByType,
      failureRateByType,
      jobs: {
        total: jobs.length,
        pending: jobs.filter((j) => j.status === 'pending').length,
        completed: jobs.filter((j) => j.status === 'completed').length,
        failed: jobs.filter((j) => j.status === 'failed').length,
        deadLetter: this.deadLetterJobs.size,
        scheduled: this.scheduledJobs.size,
      },
    };
  }

  /**
   * Attempt to acquire a lock with the given parameters
   * @param lockId - ID of the lock to acquire
   * @param owner - Owner attempting to acquire the lock
   * @param ttl - Time-to-live for the lock in milliseconds
   * @returns Promise of boolean indicating if lock was acquired
   * @throws Error if shouldFailAcquire is true
   */
  async acquireLock(lockId: string, owner: string, ttl: number): Promise<boolean> {
    if (this.options.shouldFailAcquire) {
      throw new Error('Failed to acquire lock');
    }

    const now = Date.now();
    const existingLock = this.locks.get(lockId);
    if (existingLock && existingLock.expiresAt > now) {
      return false;
    }

    this.locks.set(lockId, {
      owner,
      expiresAt: now + ttl,
    });
    return true;
  }

  /**
   * Attempt to renew a lock with the given parameters
   * @param lockId - ID of the lock to renew
   * @param owner - Owner attempting to renew the lock
   * @param ttl - New time-to-live for the lock in milliseconds
   * @returns Promise of boolean indicating if lock was renewed
   * @throws Error if shouldFailExtend is true
   */
  async renewLock(lockId: string, owner: string, ttl: number): Promise<boolean> {
    if (this.options.shouldFailExtend) {
      throw new Error('Failed to extend lock');
    }

    const now = Date.now();
    const existingLock = this.locks.get(lockId);

    // Can only extend if lock exists, hasn't expired, and is owned by the same owner
    if (existingLock && existingLock.owner === owner && existingLock.expiresAt > now) {
      existingLock.expiresAt = now + ttl;
      return true;
    }

    return false;
  }

  /**
   * Release a lock if owned by the specified owner
   * @param lockId - ID of the lock to release
   * @param owner - Owner attempting to release the lock
   * @returns Promise of boolean indicating if lock was released
   * @throws Error if shouldFailRelease is true
   */
  async releaseLock(lockId: string, owner: string): Promise<boolean> {
    if (this.options.shouldFailRelease) {
      throw new Error('Failed to release lock');
    }

    const lock = this.locks.get(lockId);
    if (!lock || lock.owner !== owner) {
      return false;
    }

    this.locks.delete(lockId);
    return true;
  }

  /**
   * Simulate another leader taking over by setting a lock with a different owner
   * @param lockId The lock ID to take over
   * @param ttl Time-to-live for the lock in milliseconds
   */
  async simulateOtherLeader(lockId: string, ttl: number): Promise<void> {
    const now = Date.now();
    this.locks.set(lockId, {
      owner: 'other-leader',
      expiresAt: now + ttl,
    });
  }

  /**
   * Close the storage provider and clear all data
   */
  public async close(): Promise<void> {
    this.locks.clear();
    this.jobs.clear();
    this.jobRuns.clear();
    this.jobLogs.clear();
    this.scheduledJobs.clear();
    this.deadLetterJobs.clear();
  }
}
