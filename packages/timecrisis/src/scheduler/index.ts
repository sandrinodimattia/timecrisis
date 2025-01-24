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
  SchedulerOptions,
} from './types.js';
import { LeaderElection } from '../leader/index.js';
import { parseDuration } from '../lib/duration.js';
import { EmptyLogger, Logger } from '../logger/index.js';
import { Job, JobRun } from '../storage/schemas/index.js';
import { JobNotFoundError, JobStorage } from '../storage/types.js';
import { ExpiredJobsTask, PendingJobsTask, ScheduledJobsTask } from '../tasks/index.js';

export class JobScheduler {
  private node: string;
  private leaderElection: LeaderElection;
  private jobs: Map<string, JobDefinition> = new Map();
  private storage: JobStorage;
  private logger: Logger;
  private isRunning: boolean = false;
  private intervals = {
    process: null as NodeJS.Timeout | null,
    schedule: null as NodeJS.Timeout | null,
    cleanup: null as NodeJS.Timeout | null,
    expired: null as NodeJS.Timeout | null,
  };
  private tasks: {
    pendingJobs: PendingJobsTask;
    scheduledJobs: ScheduledJobsTask;
    expiredJobs: ExpiredJobsTask;
  };

  constructor(private opts: SchedulerOptions) {
    this.node = opts.node ?? `${hostname()}-${randomUUID()}`;
    this.logger = opts.logger ?? new EmptyLogger();
    this.storage = opts.storage;
    this.opts = {
      maxConcurrentJobs: 20,
      pollInterval: 2000,
      jobLockTTL: 300000,
      ...opts,
    };

    // Initialize leader election process, without starting it.
    this.leaderElection = new LeaderElection({
      storage: this.storage,
      node: this.node,
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
      pendingJobs: new PendingJobsTask(
        this.storage,
        this.jobs,
        this.executeForkMode.bind(this),
        this.touchJob.bind(this),
        this.logger,
        {
          maxConcurrentJobs: this.opts.maxConcurrentJobs!,
        }
      ),
      scheduledJobs: new ScheduledJobsTask(this.storage, async (type: string, data: unknown) => {
        await this.enqueue(type, data);
      }),
      expiredJobs: new ExpiredJobsTask(this.storage, this.leaderElection, this.logger, {
        lockLifetime: this.opts.jobLockTTL!,
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
      maxRetries: options.maxRetries ?? 3,
      priority: options.priority ?? job.priority ?? 0,
      referenceId: options.referenceId,
      expiresAt,
      backoffStrategy: options.backoffStrategy ?? 'exponential',
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

    this.logger.info(`Pending jobs will be processed every ${this.opts.pollInterval} ms`);

    // Start the interval to process pending jobs.
    this.intervals.process = setInterval(async () => {
      try {
        await this.tasks.pendingJobs.execute();
      } catch (err) {
        this.logger.error('Error processing jobs:', {
          error: err instanceof Error ? err.message : String(err),
          error_stack: err instanceof Error ? err.stack : undefined,
        });
      }
    }, this.opts.pollInterval!);

    this.logger.info(`Scheduled jobs will be planned every ${this.opts.pollInterval} ms`);

    // Start the interval to enqueue jobs based on the schedule.
    this.intervals.schedule = this.scheduleLeaderTask(this.opts.pollInterval!, async () => {
      try {
        await this.tasks.scheduledJobs.execute();
      } catch (err) {
        this.logger.error('Error scheduling jobs:', {
          error: err instanceof Error ? err.message : String(err),
          error_stack: err instanceof Error ? err.stack : undefined,
        });
      }
    });

    this.logger.info(`Expired jobs will be processed every ${this.opts.pollInterval} ms`);

    // Start the interval to handle expired jobs.
    this.intervals.expired = this.scheduleLeaderTask(this.opts.pollInterval!, async () => {
      try {
        await this.tasks.expiredJobs.execute();
      } catch (err) {
        this.logger.error('Error handling expired jobs:', {
          error: err instanceof Error ? err.message : String(err),
          error_stack: err instanceof Error ? err.stack : undefined,
        });
      }
    });

    // Enforce job retention.
    this.intervals.cleanup = this.scheduleLeaderTask(3600000, async () => {
      try {
        await this.storage.cleanup({
          jobRetention: 90,
          failedJobRetention: 90,
          deadLetterRetention: 180,
        });
      } catch (err) {
        this.logger.error('Error cleaning up jobs:', {
          error: err instanceof Error ? err.message : String(err),
          error_stack: err instanceof Error ? err.stack : undefined,
        });
      }
    });
  }

  /**
   * Schedule a task to only run when the current process is the leader
   * @param interval
   * @param fn
   * @returns
   */
  scheduleLeaderTask(interval: number, fn: () => Promise<void>): NodeJS.Timeout {
    return setInterval(async () => {
      if (this.leaderElection.isCurrentLeader()) {
        await fn();
      }
    }, interval);
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

    // Stop leader election
    await this.leaderElection.stop();

    // Clear all intervals
    if (this.intervals.process) {
      clearInterval(this.intervals.process);
      this.intervals.process = null;
    }
    if (this.intervals.schedule) {
      clearInterval(this.intervals.schedule);
      this.intervals.schedule = null;
    }
    if (this.intervals.cleanup) {
      clearInterval(this.intervals.cleanup);
      this.intervals.cleanup = null;
    }
    if (this.intervals.expired) {
      clearInterval(this.intervals.expired);
      this.intervals.expired = null;
    }

    if (!force) {
      // Wait for running jobs to finish with a 2-minute timeout.
      const startTime = Date.now();
      const timeoutMs = 2 * 60 * 1000; // 2 minutes
      while (this.tasks.pendingJobs.getRunningCount() > 0) {
        if (Date.now() - startTime > timeoutMs) {
          this.logger.warn(
            'Stop operation timed out after 2 minutes with running jobs still active'
          );
          break;
        }
        await new Promise((resolve) => setTimeout(resolve, 100));
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
