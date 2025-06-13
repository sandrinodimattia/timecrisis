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
  Worker,
  RegisterWorker,
  RegisterWorkerSchema,
  UpdateWorkerHeartbeat,
  WorkerSchema,
} from '../schemas/index.js';

import {
  JobStorage,
  JobNotFoundError,
  JobRunNotFoundError,
  ScheduledJobNotFoundError,
  WorkerNotFoundError,
} from '../types.js';

/**
 * In-memory storage adapter that implements the JobStorage interface.
 * This storage provider keeps all data in memory using Maps and Arrays,
 * making it suitable for testing and development environments.
 * Note: Data is not persisted and will be lost when the process restarts.
 */
export class InMemoryJobStorage implements JobStorage {
  /**
   * Collection of jobs that have been moved to the dead letter queue
   * after exceeding their retry limits or encountering fatal errors
   */
  private deadLetterJobs: DeadLetterJob[] = [];

  /**
   * Map of all jobs indexed by their unique identifier
   */
  private jobs: Map<string, Job> = new Map();

  /**
   * Map of job runs for each job, indexed by the job's unique identifier
   */
  private jobRuns: Map<string, JobRun[]> = new Map();

  /**
   * Map of log entries for each job run, indexed by the job run's unique identifier
   */
  private jobLogs: Map<string, JobLogEntry[]> = new Map();

  /**
   * Map of scheduled jobs indexed by their unique identifier
   */
  private scheduledJobs: Map<string, ScheduledJob> = new Map();

  /**
   * Map of active locks used for job processing coordination
   * Each lock contains the worker's identifier and expiration time
   */
  private locks: Map<string, { worker: string; expiresAt: Date }> = new Map();

  /**
   * Map of workers
   */
  private workers: Map<string, Worker> = new Map();

  /**
   * Map to track running job counts per job type and worker
   * Keyed by job type, the value is a map of worker IDs to their running job counts
   */
  private runningJobCounts: Map<string, Map<string, number>> = new Map();

  /**
   * Flag to track if an operation is currently in progress
   * Used for basic concurrency control
   */
  private operationInProgress = false;

  /**
   * Queue of pending operations waiting to be executed
   * Operations are processed in FIFO order once the current operation completes
   */
  private operationQueue: Array<() => Promise<void>> = [];

  /**
   * Initialize the storage provider
   * No initialization is needed for in-memory storage
   */
  async init(): Promise<void> {
    // No initialization needed for in-memory storage
  }

  /**
   * Execute a function within a transaction-like context
   * While this doesn't provide true ACID transactions, it ensures operations
   * are executed sequentially using a simple queuing mechanism
   * @param fn - Function to execute within the transaction
   * @returns Promise resolving to the function's result
   */
  async transaction<T>(fn: (trx: unknown) => T): Promise<T> {
    if (this.operationInProgress) {
      return new Promise((resolve, reject) => {
        this.operationQueue.push(async () => {
          try {
            const result = await this.executeTransaction(fn);
            resolve(result);
          } catch (error) {
            reject(error);
          }
        });
      });
    }

    return this.executeTransaction(fn);
  }

  /**
   * Internal helper to execute a transaction function
   * Manages the operationInProgress flag and processes the next queued operation
   * @param fn - Function to execute
   * @returns Promise resolving to the function's result
   */
  private executeTransaction<T>(fn: (trx: unknown) => T): T {
    this.operationInProgress = true;
    try {
      const result = fn(null);
      return result;
    } finally {
      this.operationInProgress = false;
      if (this.operationQueue.length > 0) {
        const nextOperation = this.operationQueue.shift();
        if (nextOperation) {
          nextOperation();
        }
      }
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

    // Validate the registration data
    const validWorker = RegisterWorkerSchema.parse(worker);

    // Create the worker instance with validated data
    const workerInstance = WorkerSchema.parse({
      ...validWorker,
      first_seen: now,
      last_heartbeat: now,
    });

    this.workers.set(workerInstance.name, workerInstance);
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
    const worker = this.workers.get(workerName);
    if (!worker) {
      throw new WorkerNotFoundError(workerName);
    }

    // Update the worker with the new heartbeat
    const updatedWorker = WorkerSchema.parse({
      ...worker,
      last_heartbeat: heartbeat.last_heartbeat,
    });

    this.workers.set(workerName, updatedWorker);
  }

  /**
   * Get a worker by its name
   * @param workerName - Name of the worker to retrieve
   * @returns Promise that resolves with the worker data
   * @throws WorkerNotFoundError if the worker doesn't exist
   */
  async getWorker(workerName: string): Promise<Worker | null> {
    return this.workers.get(workerName) || null;
  }

  /**
   * Get all workers that haven't sent a heartbeat since the specified time
   * @param lastHeartbeatBefore - Time threshold for considering workers inactive
   * @returns Promise that resolves with an array of inactive workers
   */
  async getInactiveWorkers(lastHeartbeatBefore: Date): Promise<Worker[]> {
    const inactiveWorkers = Array.from(this.workers.values()).filter(
      (worker) => worker.last_heartbeat < lastHeartbeatBefore
    );

    // Validate all inactive workers before returning
    return inactiveWorkers;
  }

  /**
   * Get all registered workers in the system
   * @returns Promise that resolves with an array of all workers
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
   * Create a new job and store it in memory
   * @param job - Job data to create
   * @returns Unique identifier of the created job
   */
  async createJob(job: CreateJob): Promise<string> {
    const id = randomUUID();
    const now = new Date();

    // Parse and validate the input using Zod schema
    const validJob = CreateJobSchema.parse(job);

    // Create and validate the full job object
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
   * Retrieve a job by its unique identifier
   * @param id - Unique identifier of the job to retrieve
   * @returns Job data or null if not found
   */
  async getJob(id: string): Promise<Job | undefined> {
    return this.jobs.get(id);
  }

  /**
   * Update an existing job with new data
   * @param id - Unique identifier of the job to update
   * @param updates - Updated job data
   */
  async updateJob(id: string, updates: UpdateJob): Promise<void> {
    const job = await this.getJob(id);
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
   * Create a new job run for an existing job
   * @param jobRun - Job run data to create
   * @returns Unique identifier of the created job run
   */
  async createJobRun(jobRun: CreateJobRun): Promise<string> {
    const id = randomUUID();
    const now = new Date();

    // Parse and validate the input
    const validJobRun = CreateJobRunSchema.parse(jobRun);

    // Create and validate the full job run
    const newJobRun = JobRunSchema.parse({
      ...validJobRun,
      id,
      createdAt: now,
      updatedAt: now,
    });

    const runs = this.jobRuns.get(jobRun.jobId) || [];
    runs.push(newJobRun);
    this.jobRuns.set(jobRun.jobId, runs);
    return id;
  }

  /**
   * Retrieve a job run by its unique identifier
   * @param jobId - Unique identifier of the job to retrieve
   * @param jobRunId - Unique identifier of the job run to retrieve
   * @returns Job run data or null if not found
   */
  async getJobRun(jobId: string, jobRunId: string): Promise<JobRun | undefined> {
    const runs = this.jobRuns.get(jobId);
    if (!runs) {
      return undefined;
    }

    return runs.find((run) => run.id === jobRunId);
  }

  /**
   * Update an existing job run with new data
   * @param id - Unique identifier of the job run to update
   * @param updates - Updated job run data
   */
  async updateJobRun(id: string, updates: UpdateJobRun): Promise<void> {
    let found = false;
    for (const [jobId, runs] of this.jobRuns.entries()) {
      const runIndex = runs.findIndex((run) => run.id === id);
      if (runIndex !== -1) {
        const run = runs[runIndex];

        // Create and validate the updated run
        const updatedRun = JobRunSchema.parse({
          ...run,
          ...updates,
          updatedAt: new Date(),
        });

        runs[runIndex] = updatedRun;
        this.jobRuns.set(jobId, runs);
        found = true;
        break;
      }
    }

    if (!found) {
      throw new JobRunNotFoundError(id);
    }
  }

  /**
   * Create a new log entry for a job run
   * @param log - Log entry data to create
   */
  async createJobLog(log: CreateJobLog): Promise<void> {
    // Parse and validate the input
    const validLog = CreateJobLogSchema.parse(log);

    // Create and validate the full log entry
    const newLog = JobLogEntrySchema.parse({
      ...validLog,
      id: randomUUID(),
      createdAt: new Date(),
    });

    const logs = this.jobLogs.get(log.jobId) || [];
    logs.push(newLog);
    this.jobLogs.set(log.jobId, logs);
  }

  /**
   * Create a new scheduled job
   * @param job - Scheduled job data to create
   * @returns Unique identifier of the created scheduled job
   */
  async createScheduledJob(job: CreateScheduledJob): Promise<string> {
    const id = randomUUID();
    const now = new Date();

    // Parse and validate the input
    const validJob = CreateScheduledJobSchema.parse(job);

    // Create and validate the full scheduled job
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
   * Update an existing scheduled job with new data
   * @param id - Unique identifier of the scheduled job to update
   * @param updates - Updated scheduled job data
   */
  async updateScheduledJob(id: string, updates: UpdateScheduledJob): Promise<void> {
    const job = this.scheduledJobs.get(id);
    if (!job) {
      throw new ScheduledJobNotFoundError(id);
    }

    // Parse and validate the updates
    const validUpdates = UpdateScheduledJobSchema.parse(updates);

    // Create and validate the updated job
    const updatedJob = ScheduledJobSchema.parse({
      ...job,
      ...validUpdates,
      updatedAt: new Date(),
    });

    this.scheduledJobs.set(id, updatedJob);
  }

  /**
   * Create a new dead letter job
   * @param job - Dead letter job data to create
   */
  async createDeadLetterJob(job: CreateDeadLetterJob): Promise<void> {
    // Parse and validate the input
    const validJob = DeadLetterJobSchema.parse({
      ...job,
      id: randomUUID(),
      createdAt: new Date(),
    });

    this.deadLetterJobs.push(validJob);
  }

  /**
   * Retrieve a scheduled job by its unique identifier
   * @param id - Unique identifier of the scheduled job to retrieve
   * @returns Scheduled job data or null if not found
   */
  async getScheduledJob(id: string): Promise<ScheduledJob | null> {
    const job = this.scheduledJobs.get(id);
    return job || null;
  }

  /**
   * List jobs based on the provided filter criteria
   * @param query - Filter criteria (optional)
   * @returns Array of jobs matching the filter criteria
   */
  async listJobs(query?: {
    status?: string[];
    type?: string;
    referenceId?: string;
    runAtBefore?: Date;
    expiresAtBefore?: Date;
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

    if (query?.expiresAtBefore) {
      jobs = jobs.filter((job) => job.expiresAt && job.expiresAt <= query.expiresAtBefore!);
    }

    if (query?.limit && query.limit > 0) {
      jobs = jobs.slice(0, query.limit);
    }

    return jobs;
  }

  /**
   * List job runs for a specific job
   * @param jobId - Unique identifier of the job
   * @returns Array of job runs for the specified job
   */
  async listJobRuns(jobId: string): Promise<JobRun[]> {
    return this.jobRuns.get(jobId) || [];
  }

  /**
   * List log entries for a specific job run
   * @param jobId - Unique identifier of the job
   * @param runId - Unique identifier of the job run (optional)
   * @returns Array of log entries for the specified job run
   */
  async listJobLogs(jobId: string, runId?: string): Promise<JobLogEntry[]> {
    let logs = this.jobLogs.get(jobId) || [];
    if (runId) {
      logs = logs.filter((log) => log.jobRunId === runId);
    }
    return logs;
  }

  /**
   * List scheduled jobs based on the provided filter criteria
   * @param filter - Filter criteria (optional)
   * @returns Array of scheduled jobs matching the filter criteria
   */
  async listScheduledJobs(
    filter: {
      enabled?: boolean;
      nextRunBefore?: Date;
    } = {}
  ): Promise<ScheduledJob[]> {
    let jobs = Array.from(this.scheduledJobs.values());

    // Filter by enabled status if specified
    if (filter.enabled !== undefined) {
      jobs = jobs.filter((job) => job.enabled === filter.enabled);
    }

    // Filter by nextRunAt if specified
    if (filter.nextRunBefore) {
      jobs = jobs.filter((job) => job.nextRunAt && job.nextRunAt <= filter.nextRunBefore!);
    }

    return jobs;
  }

  /**
   * List dead letter jobs
   * @returns Array of dead letter jobs
   */
  async listDeadLetterJobs(): Promise<DeadLetterJob[]> {
    return this.deadLetterJobs;
  }

  /**
   * Acquire a lock.
   * @param lockId - Unique identifier of the lock
   * @param worker - Identifier of the lock worker
   * @param ttl - Time to live for the lock (in milliseconds)
   * @returns True if the lock was acquired, false otherwise
   */
  async acquireLock(lockId: string, worker: string, ttl: number): Promise<boolean> {
    const now = new Date();

    const existingLock = this.locks.get(lockId);
    if (existingLock) {
      // If lock exists and hasn't expired, return false regardless of worker.
      if (existingLock.expiresAt > now) {
        return false;
      }
    }

    // Lock is either expired or doesn't exist, we can acquire it.
    this.locks.set(lockId, {
      worker,
      expiresAt: new Date(now.getTime() + ttl),
    });
    return true;
  }

  /**
   * Renew an existing lock .
   * @param lockId - Unique identifier of the lock
   * @param worker - Identifier of the lock worker
   * @param ttl - Time to live for the lock (in milliseconds)
   * @returns True if the lock was renewed, false otherwise
   */
  async renewLock(lockId: string, worker: string, ttl: number): Promise<boolean> {
    const now = new Date();
    const existingLock = this.locks.get(lockId);

    // Can only extend if lock exists, hasn't expired, and is owned by the same worker
    if (existingLock && existingLock.worker === worker && existingLock.expiresAt > now) {
      existingLock.expiresAt = new Date(now.getTime() + ttl);
      return true;
    }

    return false;
  }

  /**
   * Release a lock.
   * @param lockId - Unique identifier of the lock
   * @param worker - Identifier of the lock worker
   * @returns True if the lock was released, false otherwise
   */
  async releaseLock(lockId: string, worker: string): Promise<boolean> {
    const lock = this.locks.get(lockId);
    if (!lock || lock.worker !== worker) {
      return false;
    }

    this.locks.delete(lockId);
    return true;
  }

  /**
   * List all locks owned by a specific worker.
   * @returns An array of objects with lock details, each containing lockId, worker, and expiresAt
   */
  async listLocks(filters?: {
    worker?: string | undefined;
  }): Promise<{ lockId: string; worker: string; expiresAt: Date }[]> {
    const locks = [];
    for (const key of this.locks.keys()) {
      const lock = this.locks.get(key);
      if (!filters || !filters.worker) {
        locks.push({
          lockId: key,
          worker: lock!.worker,
          expiresAt: lock!.expiresAt,
        });
        continue;
      } else if (filters.worker !== lock!.worker) {
        locks.push({
          lockId: key,
          worker: lock!.worker,
          expiresAt: lock!.expiresAt,
        });
      }
    }
    return locks;
  }

  /**
   * Acquire a slot for a specific job type and worker
   * @param jobType - The type of the job
   * @param worker - The ID of the worker requesting the slot
   * @param maxConcurrent - Maximum allowed concurrent jobs for this type
   * @returns True if the slot was acquired, false otherwise
   */
  async acquireTypeSlot(jobType: string, worker: string, maxConcurrent: number): Promise<boolean> {
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
   * @param jobType - The type of the job
   * @param workerName - The ID of the worker releasing the slot
   */
  async releaseTypeSlot(jobType: string, workerName: string): Promise<void> {
    const workerMap = this.runningJobCounts.get(jobType);
    if (!workerMap) {
      return;
    }

    const currentCount = workerMap.get(workerName) || 0;
    if (currentCount > 0) {
      workerMap.set(workerName, currentCount - 1);
    }

    // Clean up empty maps
    if (workerMap.get(workerName) === 0) {
      workerMap.delete(workerName);
    }
    if (workerMap.size === 0) {
      this.runningJobCounts.delete(jobType);
    }
  }

  /**
   * Release all slots held by a specific worker
   * @param workerName - The ID of the worker to release all slots for
   */
  async releaseAllTypeSlots(workerName: string): Promise<void> {
    for (const [jobType, workerMap] of this.runningJobCounts.entries()) {
      if (workerMap.has(workerName)) {
        workerMap.delete(workerName);

        // Clean up empty maps
        if (workerMap.size === 0) {
          this.runningJobCounts.delete(jobType);
        }
      }
    }
  }

  /**
   * Get the current running count for a specific job type
   * @param jobType - The type of the job
   * @returns Number of currently running jobs of the specified type
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
   * Clean up jobs and related data based on the provided retention periods
   * @param options - Retention periods for jobs, failed jobs, and dead letter jobs
   */
  async cleanup(options: {
    jobRetention: number;
    failedJobRetention: number;
    deadLetterRetention: number;
  }): Promise<void> {
    const now = new Date();

    // Validate and normalize retention periods
    const retention = (days: number): number => days * 24 * 60 * 60 * 1000; // Convert days to milliseconds
    const completedThreshold = new Date(now.getTime() - retention(options.jobRetention));
    const failedThreshold = new Date(now.getTime() - retention(options.failedJobRetention));
    const deadLetterThreshold = new Date(now.getTime() - retention(options.deadLetterRetention));

    // Process jobs in batches
    const batchSize = 1000;
    let processedCount = 0;

    // Clean up jobs and related data
    for (const [id, job] of this.jobs.entries()) {
      processedCount++;

      if (
        (job.status === 'completed' && job.updatedAt && job.updatedAt < completedThreshold) ||
        (job.status === 'failed' && job.updatedAt && job.updatedAt < failedThreshold)
      ) {
        await this.deleteJobAndRelatedData(id);
      }

      // Yield to event loop periodically to prevent blocking
      if (processedCount % batchSize === 0) {
        await new Promise((resolve) => setTimeout(resolve, 0));
      }
    }

    // Clean up dead letter jobs
    const deadLetterJobsToKeep = [];
    for (const job of this.deadLetterJobs) {
      if (job.failedAt >= deadLetterThreshold) {
        deadLetterJobsToKeep.push(job);
      }
    }
    this.deadLetterJobs = deadLetterJobsToKeep;

    // Clean up expired locks
    for (const [lockId, lock] of this.locks.entries()) {
      if (lock.expiresAt <= now) {
        this.locks.delete(lockId);
      }
    }
  }

  /**
   * Delete a job and its related data
   * @param jobId - Unique identifier of the job to delete
   */
  async deleteJobAndRelatedData(jobId: string): Promise<void> {
    this.jobs.delete(jobId);
    this.jobRuns.delete(jobId);
    this.jobLogs.delete(jobId);
  }

  /**
   * Get storage metrics
   * @returns Storage metrics
   */
  async getMetrics(): Promise<JobStorageMetrics> {
    const jobs = Array.from(this.jobs.values());
    const failureRateByType: Record<string, number> = {};
    const averageDurationByType: Record<string, number> = {};

    // Calculate metrics by job type
    const jobsByType = new Map<string, JobRun[]>();
    jobs.forEach((job) => {
      const typeJobs = jobsByType.get(job.type) || [];
      const jobRuns = this.jobRuns.get(job.id) || [];
      for (const jobRun of jobRuns) {
        typeJobs.push(jobRun);
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
        deadLetter: this.deadLetterJobs.length,
        scheduled: this.scheduledJobs.size,
      },
    };
  }

  /**
   * Close the storage provider and clean up all data
   * This will remove all jobs, runs, logs, locks, and workers from memory
   */
  async close(): Promise<void> {
    this.deadLetterJobs = [];
    this.jobs.clear();
    this.jobRuns.clear();
    this.jobLogs.clear();
    this.scheduledJobs.clear();
    this.locks.clear();
    this.workers.clear();
    this.runningJobCounts.clear();
    this.operationQueue = [];
    this.operationInProgress = false;
  }
}
