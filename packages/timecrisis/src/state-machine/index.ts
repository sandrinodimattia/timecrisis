import { z } from 'zod';

import { Logger } from '../logger/index.js';
import { Job } from '../storage/schemas/job.js';
import { parseDuration } from '../lib/duration.js';
import { InvalidStateTransitionError, StateMachineConfig } from './types.js';
import { EnqueueOptions, JobDefinitionNotFoundError } from '../scheduler/types.js';

/**
 * Possible states for a job.
 **/
export enum JobState {
  Pending = 'pending',
  Running = 'running',
  Completed = 'completed',
  Failed = 'failed',
  Canceled = 'canceled',
}

/**
 * Possible events (transitions) for a job.
 **/
export enum JobEvent {
  Enqueue = 'enqueue',
  Start = 'start',
  Complete = 'complete',
  Fail = 'fail',
  Cancel = 'cancel',
}

type TransitionTable = {
  [K in JobState]?: {
    [E in JobEvent]?: JobState;
  };
};

/**
 * This table indicates, for each state, what the valid
 * destination states are depending on the event.
 */
const transitions: TransitionTable = {
  [JobState.Pending]: {
    [JobEvent.Start]: JobState.Running,
    [JobEvent.Fail]: JobState.Failed,
    [JobEvent.Cancel]: JobState.Canceled,
  },
  [JobState.Running]: {
    [JobEvent.Enqueue]: JobState.Pending,
    [JobEvent.Fail]: JobState.Failed,
    [JobEvent.Complete]: JobState.Completed,
  },
  [JobState.Completed]: {},
  [JobState.Failed]: {
    [JobEvent.Enqueue]: JobState.Pending,
  },
  [JobState.Canceled]: {},
};

/**
 * Manages job state transitions and ensures they follow valid paths
 */
export class JobStateMachine {
  private cfg: StateMachineConfig;
  private logger: Logger;

  constructor(config: StateMachineConfig) {
    this.cfg = config;
    this.logger = config.logger.child('state-machine');
  }

  /**
   * Validates if a state transition is allowed
   */
  private validateTransition(currentState: JobState, event: JobEvent): JobState {
    const allowedTransitions = transitions[currentState];
    const nextState = allowedTransitions?.[event];
    if (!nextState) {
      throw new InvalidStateTransitionError(currentState, event);
    }

    return nextState;
  }

  /**
   * Move job to enqueued state
   */
  async enqueue<T extends z.infer<z.ZodAny>>(
    type: string,
    data: T,
    options: EnqueueOptions = {}
  ): Promise<string> {
    // Get the job.
    const job = this.cfg.jobs.get(type);
    if (!job) {
      throw new JobDefinitionNotFoundError(type);
    }

    // Validate the job data against the schema
    const validData = await job.schema.parseAsync(data);

    // Calculate expiration if provided.
    let expiresAt: Date | undefined;
    if (options.expiresIn) {
      expiresAt = new Date(Date.now() + parseDuration(options.expiresIn));
    } else if (options.expiresAt) {
      expiresAt = options.expiresAt;
    } else if (job.expiresAfter) {
      expiresAt = new Date(Date.now() + parseDuration(job.expiresAfter));
    } else {
      const oneHour = 60 * 60 * 1000;
      expiresAt = new Date(Date.now() + oneHour);
    }

    // Create the job and return its ID
    const jobId = await this.cfg.storage.createJob({
      type,
      data: validData,
      maxRetries: options.maxRetries ?? job.maxRetries,
      priority: options.priority ?? job.priority,
      referenceId: options.referenceId,
      scheduledJobId: options.scheduledJobId,
      expiresAt,
      backoffStrategy: options.backoffStrategy ?? job.backoffStrategy,
    });

    return jobId;
  }

  async start(job: Job): Promise<{ jobRunId: string; attempt: number }> {
    const nextState = this.validateTransition(job.status as JobState, JobEvent.Start);

    const now = new Date();
    job.status = nextState;
    job.startedAt = now;

    // Update job status to running.
    await this.cfg.storage.updateJob(job.id, {
      status: job.status,
      startedAt: job.startedAt,
    });

    // Get the previous job runs if any.
    const jobRuns = await this.cfg.storage.listJobRuns(job.id);

    // Create a new job run.
    const jobRunId = await this.cfg.storage.createJobRun({
      jobId: job.id,
      status: JobState.Running,
      startedAt: now,
      attempt: jobRuns.length + 1,
    });

    this.logger.debug(`Starting job`, {
      jobId: job.id,
      jobRunId: jobRunId,
      type: job.type,
      attempt: jobRuns.length + 1,
      maxAttempts: job.maxRetries,
    });

    try {
      this.cfg.onJobStarted?.(job.type, job.id, jobRunId, jobRuns.length + 1);
    } catch {
      // Swallow error.
    }

    return { jobRunId, attempt: jobRuns.length + 1 };
  }

  /**
   * Move job to completed state
   * @param job Job to complete
   * @param jobRunId Job run ID
   */
  async complete(job: Job, jobRunId: string): Promise<void> {
    const nextState = this.validateTransition(job.status as JobState, JobEvent.Complete);

    // Get the active job run.
    const jobRuns = await this.cfg.storage.listJobRuns(job.id);
    const jobRun = jobRuns.find((jr) => jr.id === jobRunId)!;
    const executionDuration = new Date().getTime() - jobRun.startedAt.getTime();

    this.logger.debug(`Setting job run to completed`, {
      job_id: job.id,
      job_run_id: jobRunId,
      status: nextState,
      execution_duration: executionDuration,
    });

    // Mark success.
    await this.cfg.storage.updateJob(job.id, {
      status: nextState,
      finishedAt: new Date(),
    });

    // Mark success.
    await this.cfg.storage.updateJobRun(jobRunId, {
      status: JobState.Completed,
      executionDuration,
      progress: 100,
      finishedAt: new Date(),
    });

    /**
     * Create a log entry.
     */
    await this.cfg.storage.createJobLog({
      jobId: job.id,
      jobRunId: jobRunId,
      level: 'info',
      message: `Job completed successfully`,
      timestamp: new Date(),
    });

    try {
      this.cfg.onJobCompleted?.(job.type, job.id, jobRunId);
    } catch {
      // Swallow error.
    }
  }

  /**
   * Move job to failed state
   */
  async fail(
    job: Job,
    jobRunId: string | undefined,
    canRetry: boolean,
    error: Error,
    errorMessage: string,
    errorStack?: string
  ): Promise<void> {
    this.validateTransition(job.status as JobState, JobEvent.Fail);

    const now = new Date();

    // Get the previous job runs if any.
    const jobRuns = await this.cfg.storage.listJobRuns(job.id);
    const jobRun =
      jobRuns.find((jr) => jr.id === jobRunId) ||
      jobRuns.find((jr) => jr.status === JobState.Running);
    const executionDuration = jobRun ? new Date().getTime() - jobRun.startedAt.getTime() : 0;

    this.logger.warn('Job failed', {
      job_id: job.id,
      job_run_id: jobRun?.id,
      type: job.type,
      error: errorMessage,
      error_stack: errorStack ?? undefined,
      execution_duration: executionDuration,
    });

    try {
      this.cfg.onJobFailed?.(job.type, job.id, jobRun?.id, error);
    } catch {
      // Swallow error.
    }

    // Check if we should retry
    const attempts = jobRuns.length;
    const retries = jobRuns.length - 1;
    if (canRetry && retries < job.maxRetries) {
      const nextState = this.validateTransition(job.status as JobState, JobEvent.Enqueue);

      // Calculate backoff delay.
      let delay = 10000;
      if (job.backoffStrategy === 'exponential') {
        const baseDelay = 10000;
        const exponentialDelay = Math.pow(2, retries) * baseDelay;
        const jitter = Math.random() * baseDelay * 0.1;
        delay = Math.min(exponentialDelay + jitter, 24 * 60 * 60 * 1000);
      }
      const nextRun = new Date(Date.now() + delay);

      this.logger.warn(`Enqueuing retry for failed job in ${delay} ms`, {
        job_id: job.id,
        type: job.type,
        error: errorMessage,
        execution_duration: executionDuration,
        delay,
        next_run: nextRun,
      });

      // Reset the job to pending for retry
      await this.cfg.storage.updateJob(job.id, {
        status: nextState,
        failReason: null,
        failCount: job.failCount + 1,
        runAt: nextRun,
      });

      /**
       * Create a log entry.
       */
      await this.cfg.storage.createJobLog({
        jobId: job.id,
        jobRunId: jobRunId,
        level: 'warn',
        message: `Job failed, retrying in ${delay} ms: ${errorMessage}`,
        timestamp: new Date(),
      });
    } else {
      this.logger.warn(`Job failed permanently due to error: ${errorMessage}`, {
        job_id: job.id,
        type: job.type,
        error: errorMessage,
        error_stack: errorStack ?? undefined,
        attempts: attempts,
        max_retries: job.maxRetries,
      });

      // Mark as failed if we've exceeded retries
      await this.cfg.storage.updateJob(job.id, {
        status: JobState.Failed,
        failReason: errorMessage,
        failCount: job.failCount + 1,
        finishedAt: now,
      });

      /**
       * Create a log entry.
       */
      await this.cfg.storage.createJobLog({
        jobId: job.id,
        jobRunId: jobRunId,
        level: 'error',
        message: `Job failed permanently after ${attempts} attempts: ${errorMessage}`,
        timestamp: new Date(),
      });

      // Move to dead letter queue.
      await this.cfg.storage.createDeadLetterJob({
        jobId: job.id,
        jobType: job.type,
        data: job.data,
        failReason: errorMessage,
        failedAt: new Date(),
      });
    }

    // Update the job run to failed.
    if (jobRun) {
      this.logger.debug(`Setting job run to failed: ${errorMessage}`, {
        job: job.id,
        jobRun: jobRun.id,
        status: JobState.Failed,
      });

      await this.cfg.storage.updateJobRun(jobRun.id, {
        status: JobState.Failed,
        executionDuration,
        finishedAt: now,
        error: errorMessage,
        errorStack: errorStack,
      });
    }
  }
}
