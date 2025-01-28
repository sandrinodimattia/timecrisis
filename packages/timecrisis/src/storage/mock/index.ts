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
  RegisterWorker,
  RegisterWorkerSchema,
  ScheduledJob,
  ScheduledJobSchema,
  UpdateJob,
  UpdateJobRun,
  UpdateJobSchema,
  UpdateScheduledJob,
  UpdateScheduledJobSchema,
  UpdateWorkerHeartbeat,
  UpdateWorkerHeartbeatSchema,
  Worker,
  WorkerSchema,
} from '../schemas/index.js';

import {
  JobStorage,
  JobNotFoundError,
  JobRunNotFoundError,
  ScheduledJobNotFoundError,
} from '../types.js';

import { WorkerNotFoundError } from '../types.js';

export interface MockStorageOptions {
  shouldFailAcquire?: boolean;
  shouldFailRelease?: boolean;
  shouldFailExtend?: boolean;
  shouldFailTransaction?: boolean;
}

export class MockJobStorage implements JobStorage {
  private locks: Map<string, { worker: string; expiresAt: number }> = new Map();
  private jobs: Map<string, Job> = new Map();
  private jobRuns: Map<string, JobRun> = new Map();
  private jobLogs: Map<string, JobLogEntry[]> = new Map();
  private scheduledJobs: Map<string, ScheduledJob> = new Map();
  private deadLetterJobs: Map<string, DeadLetterJob> = new Map();
  private workers: Map<string, Worker> = new Map();
  private runningJobCounts: Map<string, Map<string, number>> = new Map();

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
    this.getJobRun = vi.fn(this.getJobRun.bind(this));
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
    this.registerWorker = vi.fn(this.registerWorker.bind(this));
    this.updateWorkerHeartbeat = vi.fn(this.updateWorkerHeartbeat.bind(this));
    this.getWorker = vi.fn(this.getWorker.bind(this));
    this.getInactiveWorkers = vi.fn(this.getInactiveWorkers.bind(this));
    this.getWorkers = vi.fn(this.getWorkers.bind(this));
    this.acquireJobTypeSlot = vi.fn(this.acquireJobTypeSlot.bind(this));
    this.releaseJobTypeSlot = vi.fn(this.releaseJobTypeSlot.bind(this));
    this.releaseAllJobTypeSlots = vi.fn(this.releaseAllJobTypeSlots.bind(this));
    this.getRunningCount = vi.fn(this.getRunningCount.bind(this));
    this.deleteWorker = vi.fn(this.deleteWorker.bind(this));
  }

  /**
   * Set a lock directly for testing purposes
   */
  public setLock(lockId: string, worker: string, expiresAt: number): void {
    this.locks.set(lockId, { worker, expiresAt });
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
    this.workers.clear();
    this.runningJobCounts.clear();
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
  async getJob(id: string): Promise<Job | undefined> {
    return this.jobs.get(id);
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

    // Create and validate the updated job
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
      jobs = jobs.filter((job) => job.entityId === query.referenceId);
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
   * Retrieve a job run by its unique identifier
   * @param jobId - Unique identifier of the job to retrieve
   * @param jobRunId - Unique identifier of the job run to retrieve
   * @returns Job run data or null if not found
   */
  async getJobRun(jobId: string, jobRunId: string): Promise<JobRun | undefined> {
    return this.jobRuns.get(jobRunId);
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
   * @param jobRunId - ID of the job run to list logs for
   * @returns Promise of log entries for the specified job
   */
  async listJobLogs(jobId: string, jobRunId?: string): Promise<JobLogEntry[]> {
    const logs = this.jobLogs.get(jobId) || [];
    if (jobRunId) {
      return logs.filter((log) => log.jobRunId === jobRunId);
    }
    return logs;
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
  async listScheduledJobs(filter?: {
    enabled?: boolean;
    nextRunBefore?: Date;
  }): Promise<ScheduledJob[]> {
    let jobs = Array.from(this.scheduledJobs.values());

    // Filter by enabled status if specified
    if (filter?.enabled !== undefined) {
      jobs = jobs.filter((job) => job.enabled === filter.enabled);
    }

    // Filter by nextRunAt if specified
    if (filter?.nextRunBefore) {
      jobs = jobs.filter((job) => job.nextRunAt && job.nextRunAt <= filter.nextRunBefore!);
    }

    return jobs;
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
    const jobsByType = new Map<string, JobRun[]>();
    jobs.forEach((job) => {
      const typeJobs = jobsByType.get(job.type) || [];
      for (const kv of this.jobRuns.entries()) {
        if (kv[1].jobId !== job.id) {
          continue;
        }
        typeJobs.push(kv[1]);
      }
      jobsByType.set(job.type, typeJobs);
    });

    jobsByType.forEach((typeJobs, type) => {
      // Calculate average duration
      const completedJobs = typeJobs.filter((j) => j.status === 'completed');
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
   * @param worker - Owner attempting to acquire the lock
   * @param ttl - Time-to-live for the lock in milliseconds
   * @returns Promise of boolean indicating if lock was acquired
   * @throws Error if shouldFailAcquire is true
   */
  async acquireLock(lockId: string, worker: string, ttl: number): Promise<boolean> {
    if (this.options.shouldFailAcquire) {
      throw new Error('Failed to acquire lock');
    }

    const now = Date.now();
    const existingLock = this.locks.get(lockId);
    if (existingLock && existingLock.expiresAt > now) {
      return false;
    }

    this.locks.set(lockId, {
      worker,
      expiresAt: now + ttl,
    });
    return true;
  }

  /**
   * Attempt to renew a lock with the given parameters
   * @param lockId - ID of the lock to renew
   * @param worker - Worker attempting to renew the lock
   * @param ttl - New time-to-live for the lock in milliseconds
   * @returns Promise of boolean indicating if lock was renewed
   * @throws Error if shouldFailExtend is true
   */
  async renewLock(lockId: string, worker: string, ttl: number): Promise<boolean> {
    if (this.options.shouldFailExtend) {
      throw new Error('Failed to extend lock');
    }

    const now = Date.now();
    const existingLock = this.locks.get(lockId);

    // Can only extend if lock exists, hasn't expired, and is owned by the same worker
    if (existingLock && existingLock.worker === worker && existingLock.expiresAt > now) {
      existingLock.expiresAt = now + ttl;
      return true;
    }

    return false;
  }

  /**
   * Release a lock if owned by the specified worker
   * @param lockId - ID of the lock to release
   * @param worker - Wwner attempting to release the lock
   * @returns Promise of boolean indicating if lock was released
   * @throws Error if shouldFailRelease is true
   */
  async releaseLock(lockId: string, worker: string): Promise<boolean> {
    if (this.options.shouldFailRelease) {
      throw new Error('Failed to release lock');
    }

    const lock = this.locks.get(lockId);
    if (!lock || lock.worker !== worker) {
      return false;
    }

    this.locks.delete(lockId);
    return true;
  }

  /**
   * List all locks owned by a specific worker.
   * @param worker - Worker to list locks for
   * @returns An array of objects with lock details, each containing lockId, worker, and expiresAt
   */
  async listLocks(filters?: {
    worker?: string | undefined;
  }): Promise<{ lockId: string; worker: string; expiresAt: Date }[]> {
    const locks = [];
    for (const key of this.locks.keys()) {
      const lock = this.locks.get(key);
      if (typeof filters === 'undefined' || typeof filters.worker === 'undefined') {
        locks.push({
          lockId: key,
          worker: lock!.worker,
          expiresAt: new Date(lock!.expiresAt),
        });
        continue;
      } else if (filters.worker == lock!.worker) {
        locks.push({
          lockId: key,
          worker: lock!.worker,
          expiresAt: new Date(lock!.expiresAt),
        });
      }
    }

    return locks;
  }

  /**
   * Simulate another leader taking over by setting a lock with a different owner
   * @param lockId The lock ID to take over
   * @param ttl Time-to-live for the lock in milliseconds
   */
  async simulateOtherLeader(lockId: string, ttl: number): Promise<void> {
    const now = Date.now();
    this.locks.set(lockId, {
      worker: 'other-leader',
      expiresAt: now + ttl,
    });
  }

  /**
   * Register a new worker instance in the system
   * Creates a new worker with a unique ID and timestamps for first_seen and last_heartbeat.
   * @param worker - Worker registration data containing the worker name
   * @returns Promise resolving to the ID of the registered worker
   * @throws ZodError if the worker registration data is invalid
   */
  async registerWorker(worker: RegisterWorker): Promise<string> {
    const now = new Date();

    // Parse and validate the registration data
    const validWorker = RegisterWorkerSchema.parse(worker);

    // Create the worker instance with validated data
    const workerInstance = WorkerSchema.parse({
      ...validWorker,
      first_seen: now,
      last_heartbeat: now,
    });

    this.workers.set(validWorker.name, workerInstance);
    return validWorker.name;
  }

  /**
   * Update a worker's heartbeat timestamp
   * Updates the last_heartbeat field of an existing worker.
   * @param workerName - Name of the worker to update
   * @param heartbeat - Heartbeat data containing the new timestamp
   * @throws WorkerNotFoundError if the worker doesn't exist
   * @throws ZodError if the heartbeat data is invalid
   */
  async updateWorkerHeartbeat(workerName: string, heartbeat: UpdateWorkerHeartbeat): Promise<void> {
    const worker = this.workers.get(workerName);
    if (!worker) {
      throw new WorkerNotFoundError(workerName);
    }

    // Parse and validate the heartbeat data
    const validHeartbeat = UpdateWorkerHeartbeatSchema.parse(heartbeat);

    // Create the updated worker object
    const updatedWorker = WorkerSchema.parse({
      ...worker,
      last_heartbeat: validHeartbeat.last_heartbeat,
    });

    this.workers.set(workerName, updatedWorker);
  }

  /**
   * Get a worker by its name
   * @param name - Name of the worker to retrieve
   * @returns Promise resolving to the worker data or null if not found
   */
  async getWorker(name: string): Promise<Worker | null> {
    return this.workers.get(name) || null;
  }

  /**
   * Get all workers that haven't sent a heartbeat since the specified time
   * A worker is considered inactive if its last_heartbeat is before the specified time.
   * @param lastHeartbeatBefore - Time threshold for considering workers inactive
   * @returns Promise resolving to an array of inactive workers
   */
  async getInactiveWorkers(lastHeartbeatBefore: Date): Promise<Worker[]> {
    return Array.from(this.workers.values()).filter(
      (worker) => worker.last_heartbeat < lastHeartbeatBefore
    );
  }

  /**
   * Get all registered workers in the system
   * @returns Promise resolving to an array of all workers
   */
  async getWorkers(): Promise<Worker[]> {
    return Array.from(this.workers.values());
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

    this.workers.delete(workerName);
  }

  /**
   * Acquire a slot for a specific job type and worker
   * @param jobType - The type of job to acquire a slot for
   * @param worker - The worker requesting the slot
   * @param maxConcurrent - Maximum number of concurrent jobs allowed for this type
   * @returns Promise resolving to true if slot was acquired, false otherwise
   */
  async acquireJobTypeSlot(
    jobType: string,
    worker: string,
    maxConcurrent: number
  ): Promise<boolean> {
    if (this.options.shouldFailAcquire) {
      throw new Error('Failed to acquire slot');
    }

    // Get or create the worker map for this job type
    let workerMap = this.runningJobCounts.get(jobType);
    if (!workerMap) {
      workerMap = new Map();
      this.runningJobCounts.set(jobType, workerMap);
    }

    // Calculate total slots used across all workers
    let totalCount = 0;
    for (const count of workerMap.values()) {
      totalCount += count;
    }

    if (totalCount >= maxConcurrent) {
      return false;
    }

    // Increment the slot count for this worker
    const currentCount = workerMap.get(worker) || 0;
    workerMap.set(worker, currentCount + 1);
    return true;
  }

  /**
   * Release a slot for a specific job type and worker
   * @param jobType - The type of job to release a slot for
   * @param worker - The worker releasing the slot
   */
  async releaseJobTypeSlot(jobType: string, worker: string): Promise<void> {
    if (this.options.shouldFailRelease) {
      throw new Error('Failed to release slot');
    }

    const workerMap = this.runningJobCounts.get(jobType);
    if (!workerMap) {
      return;
    }

    const currentCount = workerMap.get(worker) || 0;
    if (currentCount > 0) {
      workerMap.set(worker, currentCount - 1);
    }

    // Clean up empty maps
    if (workerMap.get(worker) === 0) {
      workerMap.delete(worker);
    }
    if (workerMap.size === 0) {
      this.runningJobCounts.delete(jobType);
    }
  }

  /**
   * Release all slots held by a specific worker
   * @param worker - The worker to release all slots for
   */
  async releaseAllJobTypeSlots(worker: string): Promise<void> {
    if (this.options.shouldFailRelease) {
      throw new Error('Failed to release slots');
    }

    for (const [jobType, workerMap] of this.runningJobCounts.entries()) {
      if (workerMap.has(worker)) {
        workerMap.delete(worker);

        // Clean up empty maps
        if (workerMap.size === 0) {
          this.runningJobCounts.delete(jobType);
        }
      }
    }
  }

  /**
   * Get the current count of running jobs
   * If jobType is provided, returns the count for that specific type.
   * If jobType is not provided, returns the total count across all types.
   * @param jobType - Optional, the specific job type to get the count for
   * @returns Promise resolving to the number of running jobs
   */
  async getRunningCount(jobType?: string): Promise<number> {
    if (jobType) {
      const workerMap = this.runningJobCounts.get(jobType);
      if (!workerMap) {
        return 0;
      }

      let total = 0;
      for (const count of workerMap.values()) {
        total += count;
      }
      return total;
    } else {
      // Return the total running jobs across all types
      let total = 0;
      for (const workerMap of this.runningJobCounts.values()) {
        for (const count of workerMap.values()) {
          total += count;
        }
      }
      return total;
    }
  }

  /**
   * Close the storage provider and clean up all data
   */
  async close(): Promise<void> {
    this.reset();
  }
}
