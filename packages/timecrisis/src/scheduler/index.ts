import { z } from 'zod';
import { hostname } from 'os';
import { randomUUID } from 'crypto';
import cronParser from 'cron-parser';
import { ChildProcess, fork } from 'node:child_process';

import {
  EnqueueOptions,
  ForkHelperPathMissingError,
  InvalidScheduleError,
  JobAlreadyRegisteredError,
  JobContext,
  JobDefinition,
  JobDefinitionNotFoundError,
  ProcessExitError,
  ScheduleOptions,
  SchedulerError,
  SchedulerMetrics,
  SchedulerConfig,
} from './types.js';

import {
  DeadWorkersTask,
  ExpiredJobsTask,
  PendingJobsTask,
  ScheduledJobsTask,
  StorageCleanupTask,
  WorkerAliveTask,
} from '../tasks/index.js';

import { LeaderElection } from '../leader/index.js';
import { parseDuration } from '../lib/duration.js';
import { EmptyLogger, Logger } from '../logger/index.js';
import { Job, JobRun } from '../storage/schemas/index.js';
import { JobNotFoundError, JobStorage } from '../storage/types.js';

export class JobScheduler {
  private worker: string;
  private storage: JobStorage;
  private logger: Logger;
  private leaderElection: LeaderElection;
  private jobs: Map<string, JobDefinition> = new Map();
  private isRunning: boolean = false;
  private opts: SchedulerConfig;

  private tasks: {
    workerInactiveCleanup: DeadWorkersTask;
    expiredJobs: ExpiredJobsTask;
    pendingJobs: PendingJobsTask;
    storageCleanup: StorageCleanupTask;
    scheduledJobs: ScheduledJobsTask;
    workerAlive: WorkerAliveTask;
  };

  constructor(opts: SchedulerConfig) {
    this.worker = opts.worker ?? `${hostname()}-${randomUUID()}`;
    this.logger = opts.logger ?? new EmptyLogger();
    this.storage = opts.storage;
    this.opts = {
      ...opts,
      maxConcurrentJobs: opts.maxConcurrentJobs ?? 20,
      leaderLockTTL: opts.leaderLockTTL ?? 30000,
      scheduledJobMaxStaleAge: opts.scheduledJobMaxStaleAge ?? 1000 * 60 * 60,
      expiredJobCheckInterval: opts.expiredJobCheckInterval ?? 60000,
      jobLockTTL: opts.jobLockTTL ?? 60000,
      jobProcessingInterval: opts.jobProcessingInterval ?? 5000,
      jobSchedulingInterval: opts.jobSchedulingInterval ?? 60000,
      shutdownTimeout: opts.shutdownTimeout ?? 15000,
      workerHeartbeatInterval: opts.workerHeartbeatInterval ?? 15000,
      workerInactiveCheckInterval: opts.workerInactiveCheckInterval ?? 60000,
    };

    // Initialize leader election process, without starting it.
    this.leaderElection = new LeaderElection({
      storage: this.storage,
      node: this.worker,
      lockTTL: opts.leaderLockTTL ?? 30000,
      onAcquired: async (): Promise<void> => {
        this.logger.info('Acquired leadership');
        await this.redistributeStuckJobs();
      },
      onLost: async (): Promise<void> => {
        this.logger.warn('Lost leadership');
      },
    });

    // Create the different tasks which run in the background.
    this.tasks = {
      pendingJobs: new PendingJobsTask({
        logger: this.logger,
        jobs: this.jobs,
        storage: this.storage,
        worker: this.worker,
        executeForkMode: this.executeForkMode.bind(this),
        touchJob: this.touchJob.bind(this),
        maxConcurrentJobs: this.opts.maxConcurrentJobs!,
        jobLockTTL: this.opts.jobLockTTL!,
        pollInterval: this.opts.jobProcessingInterval!,
      }),
      scheduledJobs: new ScheduledJobsTask({
        storage: this.storage,
        logger: this.logger,
        leaderElection: this.leaderElection,
        scheduledJobMaxStaleAge: this.opts.scheduledJobMaxStaleAge!,
        pollInterval: this.opts.jobSchedulingInterval!,
        enqueueJob: async (job, data): Promise<void> => {
          await this.enqueue(job, data);
        },
      }),
      expiredJobs: new ExpiredJobsTask({
        storage: this.storage,
        logger: this.logger,
        leaderElection: this.leaderElection,
        jobLockTTL: this.opts.jobLockTTL!,
        pollInterval: this.opts.expiredJobCheckInterval!,
      }),
      storageCleanup: new StorageCleanupTask({
        storage: this.storage,
        logger: this.logger,
        pollInterval: 3600000,
        jobRetention: 365,
        failedJobRetention: 365,
        deadLetterRetention: 365,
      }),
      workerAlive: new WorkerAliveTask({
        logger: this.logger,
        storage: this.storage,
        name: this.worker,
        heartbeatInterval: this.opts.workerHeartbeatInterval!,
      }),
      workerInactiveCleanup: new DeadWorkersTask({
        logger: this.logger,
        storage: this.storage,
        leaderElection: this.leaderElection,
        pollInterval: this.opts.workerInactiveCheckInterval!,
        workerDeadTimeout: this.opts.workerHeartbeatInterval! * 2,
      }),
    };
  }

  /**
   * Register a job type
   */
  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  registerJob<T extends z.ZodObject<any> = z.ZodObject<any>>(job: JobDefinition<T>): void {
    if (this.jobs.has(job.type)) {
      throw new JobAlreadyRegisteredError(job.type);
    }

    this.jobs.set(job.type, job as unknown as JobDefinition);
  }

  /**
   * Create and enqueue a new job
   */
  async enqueue<T extends z.infer<z.ZodAny>>(
    type: string,
    data: T,
    options: EnqueueOptions = {}
  ): Promise<string> {
    // Get the job.
    const job = this.jobs.get(type);
    if (!job) {
      throw new JobDefinitionNotFoundError(type);
    }

    // Validate the job data against the schema
    const validData = await job.schema.parseAsync(data);

    // Calculate expiration if provided.
    const expiresAt = options.expiresIn
      ? new Date(Date.now() + parseDuration(options.expiresIn))
      : options.expiresAt;

    // Create the job and return its ID
    const jobId = await this.storage.createJob({
      type,
      data: validData,
      maxRetries: options.maxRetries,
      priority: options.priority ?? job.priority,
      referenceId: options.referenceId,
      expiresAt,
      backoffStrategy: options.backoffStrategy,
    });

    return jobId;
  }

  /**
   * Schedule a job to run at regular intervals
   */
  async schedule<T extends z.infer<z.ZodAny>>(
    name: string,
    type: string,
    data: T,
    options: ScheduleOptions
  ): Promise<string> {
    // Validate job type exists
    const job = this.jobs.get(type);
    if (!job) {
      throw new JobDefinitionNotFoundError(type);
    }

    // Validate the job data against the schema
    const validData = await job.schema.parseAsync(data);

    // Validate schedule format
    let nextRunAt: Date | null = null;
    if (options.scheduleType === 'cron') {
      const cronExpression = options.scheduleValue;
      try {
        const interval = cronParser.parseExpression(cronExpression);
        nextRunAt = interval.next().toDate();
      } catch {
        throw new InvalidScheduleError(cronExpression, 'cron expression');
      }
    } else if (options.scheduleType === 'exact') {
      const timestamp = new Date(options.scheduleValue);
      if (isNaN(timestamp.getTime())) {
        throw new InvalidScheduleError(options.scheduleValue, 'exact time');
      }
      nextRunAt = timestamp;
    } else if (options.scheduleType === 'interval') {
      const scheduleValue = options.scheduleValue;
      try {
        const duration = parseDuration(scheduleValue);
        nextRunAt = new Date(Date.now() + duration);
      } catch {
        throw new InvalidScheduleError(scheduleValue, 'interval');
      }
    } else {
      throw new InvalidScheduleError(options.scheduleType, 'schedule type');
    }

    // Create the scheduled job
    return await this.storage.createScheduledJob({
      name,
      type,
      scheduleType: options.scheduleType,
      scheduleValue: options.scheduleValue,
      data: validData,
      enabled: options.enabled ?? true,
      nextRunAt,
    });
  }

  /**
   * Executes a job in fork mode
   * @param def Job definition
   * @param job Job to execute
   * @param ctx Job context
   * @returns
   */
  private async executeForkMode(def: JobDefinition, job: Job, ctx: JobContext): Promise<void> {
    if (!def.forkHelperPath) {
      throw new ForkHelperPathMissingError(job.type);
    }

    return new Promise<void>((resolve, reject) => {
      this.logger.info(`Job ${job.id} of type ${job.type} is running in fork mode`);

      const child: ChildProcess = fork(def.forkHelperPath!, [job.type, job.id], {
        env: {
          ...process.env,
          JOB_ID: job.id,
          JOB_TYPE: job.type,
          JOB_DATA: JSON.stringify(job.data),
        },
      });

      let childError: Error | null = null;

      // Set up message handling
      child.on(
        'message',
        async (msg: {
          type: string;
          message: string;
          level: 'error' | 'warn' | 'info';
          metadata?: Record<string, unknown>;
        }) => {
          if (msg.type === 'log') {
            await ctx.log(msg.level, msg.message, msg.metadata);
          } else if (msg.type === 'touch') {
            await ctx.touch();
          } else if (msg.type === 'error') {
            childError = new Error(msg.message);
          }
        }
      );

      // Handle process exit
      child.on('exit', (code) => {
        if (code === 0 && !childError) {
          resolve();
        } else {
          reject(childError || new ProcessExitError(code ?? -1));
        }
      });

      // Handle process errors
      child.on('error', (err) => {
        childError = err;
        reject(err);
      });
    });
  }

  /**
   * Touch a job to keep it alive
   */
  private async touchJob(jobId: string): Promise<void> {
    await this.storage.updateJob(jobId, {
      lockedAt: new Date(),
    });
  }

  /**
   * Start the scheduler.
   */
  async start(): Promise<void> {
    if (this.isRunning) {
      throw new SchedulerError('Scheduler is already running');
    }

    this.isRunning = true;

    // Start leader election process.
    await this.leaderElection.start();
    await this.tasks.workerAlive.start();
    await this.tasks.workerInactiveCleanup.start();
    await this.tasks.expiredJobs.start();
    await this.tasks.pendingJobs.start();
    await this.tasks.scheduledJobs.start();
    await this.tasks.storageCleanup.start();
  }

  /**
   * Stop the scheduler
   * @param force If true, don't wait for running jobs to finish
   */
  async stop(force: boolean = false): Promise<void> {
    if (!this.isRunning) {
      return;
    }

    this.isRunning = false;
    this.logger.info('Stopping scheduler...');

    // Stop leader election.
    await this.leaderElection.stop();

    // Stop tasks.
    this.tasks.workerAlive.stop();
    this.tasks.workerInactiveCleanup.stop();
    this.tasks.expiredJobs.stop();
    this.tasks.pendingJobs.stop();
    this.tasks.scheduledJobs.stop();
    this.tasks.storageCleanup.stop();

    // Shutdown.
    if (!force) {
      const startTime = Date.now();
      const shutdownTimeout = this.opts.shutdownTimeout ?? 15000;
      const pollInterval = 500;

      while (this.tasks.pendingJobs.getRunningCount() > 0) {
        const elapsedTime = Date.now() - startTime;
        if (elapsedTime >= shutdownTimeout) {
          this.logger.warn(
            `Shutdown timeout of ${shutdownTimeout} ms reached with ${this.tasks.pendingJobs.getRunningCount()} jobs still running`
          );
          break;
        }

        this.logger.info(
          `Waiting for ${this.tasks.pendingJobs.getRunningCount()} running jobs to finish (${elapsedTime}ms elapsed)...`
        );
        await new Promise((resolve) => setTimeout(resolve, pollInterval));
      }
    }

    // Close storage
    await this.storage.close();
  }

  /**
   * Get current scheduler metrics
   */
  async getMetrics(): Promise<SchedulerMetrics> {
    const jobs = await this.storage.listJobs();
    const deadLetterJobs = await this.storage.listDeadLetterJobs();
    const storageMetrics = await this.storage.getMetrics();

    // Calculate metrics
    const metrics: SchedulerMetrics = {
      running: 0,
      pending: 0,
      completed: 0,
      failed: 0,
      averageDuration: 0,
      deadLetterJobs: deadLetterJobs.length,
      types: {},
      storage: storageMetrics,
    };

    // Count jobs by status
    for (const job of jobs) {
      switch (job.status) {
        case 'running':
          metrics.running++;
          break;
        case 'pending':
          metrics.pending++;
          break;
        case 'completed':
          metrics.completed++;
          break;
        case 'failed':
          metrics.failed++;
          break;
      }

      // Initialize job type metrics if not exists
      if (!metrics.types[job.type]) {
        metrics.types[job.type] = {
          running: 0,
          pending: 0,
          completed: 0,
          failed: 0,
        };
      }

      // Update job type metrics
      switch (job.status) {
        case 'running':
          metrics.types[job.type].running++;
          break;
        case 'pending':
          metrics.types[job.type].pending++;
          break;
        case 'completed':
          metrics.types[job.type].completed++;
          break;
        case 'failed':
          metrics.types[job.type].failed++;
          break;
      }
    }

    // Calculate average duration from successful jobs
    const successful = jobs.filter((r) => r.status === 'completed' && r.executionDuration);
    if (successful.length > 0) {
      const totalDuration = successful.reduce((sum, r) => sum + (r.executionDuration || 0), 0);
      metrics.averageDuration = totalDuration / successful.length;
    }

    return metrics;
  }

  /**
   * Get detailed information about a job.
   */
  async getJobDetails(jobId: string): Promise<{
    job: Job;
    runs: JobRun[];
  }> {
    const job = await this.storage.getJob(jobId);
    if (!job) {
      throw new JobNotFoundError(jobId);
    }

    return {
      job,
      runs: await this.storage.listJobRuns(jobId),
    };
  }

  /**
   * Redistribute jobs that might be stuck due to failed nodes
   */
  private async redistributeStuckJobs(): Promise<void> {
    const now = new Date();
    const stuckJobs = await this.storage.listJobs({
      status: ['running'],
      lockedBefore: new Date(now.getTime() - this.opts.jobLockTTL!),
    });

    for (const job of stuckJobs) {
      this.logger.info(`Redistributing job ${job.id} due to leadership change`, {
        jobId: job.id,
        previousLock: job.lockedAt,
      });

      // Change the job back to pending.
      await this.storage.updateJob(job.id, {
        status: 'pending',
        lockedAt: null,
        failCount: job.failCount + 1,
        failReason: 'Job redistributed due to node failure',
      });

      await this.storage.createJobLog({
        jobId: job.id,
        timestamp: now,
        level: 'warn',
        message: 'Job redistributed due to node failure',
        metadata: {
          previousLock: job.lockedAt,
        },
      });
    }
  }
}
