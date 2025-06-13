import { z } from 'zod';

import { Lock } from '../../concurrency/lock.js';
import { Job } from '../../storage/schemas/job.js';
import { JobContextImpl } from '../../scheduler/context.js';
import { formatLockName } from '../../concurrency/job-lock.js';
import { JobDefinition, JobExpiredError } from '../../scheduler/types.js';
import { JobContextData, Middleware, PendingJobsContext } from './types.js';

/**
 * Simple middleware composition utility.
 * @param middlewares
 * @returns
 */
export function composeMiddlewares(middlewares: Middleware[]): Middleware {
  return async function composed(ctx, jobCtx, next) {
    let index = -1;

    async function dispatch(i: number): Promise<void> {
      if (i <= index) {
        throw new Error('next() called multiple times');
      }
      index = i;
      const fn = middlewares[i] || next;
      if (!fn) return;
      return fn(ctx, jobCtx, () => dispatch(i + 1));
    }

    return dispatch(0);
  };
}

/**
 * Attempts to acquire a concurrency slot for the job.
 */
export function limitExecutions(): Middleware {
  return async (ctx, jobCtx, next) => {
    const { job } = jobCtx;
    const { concurrency, logger } = ctx;

    // Attempt to acquire a concurrency slot
    if (!concurrency.acquire(job.id)) {
      logger.debug('Failed to acquire concurrency slot for job', {
        job_id: job.id,
        type: job.type,
      });
      return;
    }

    try {
      await next();
    } finally {
      // Always release the global concurrency slot
      concurrency.release(job.id);
    }
  };
}

/**
 * Tries to acquire a concurrency slot specific to the job's `type`.
 * This must be released whether success or failure.
 */
export function limitExecutionsByType(): Middleware {
  return async (ctx, jobCtx, next) => {
    const { job, jobDef } = jobCtx;
    const { storage, logger } = ctx;

    const typeConcurrencyLimit = jobDef.concurrency;
    let typeSlotAcquired = false;

    try {
      // Attempt to acquire a slot to run this job type.
      typeSlotAcquired = await storage.acquireTypeSlot(job.type, ctx.worker, typeConcurrencyLimit);
      if (!typeSlotAcquired) {
        logger.debug(`Failed to acquire execution slot for job type "${job.type}"`, {
          job_id: job.id,
          type: job.type,
        });
        return;
      }

      logger.debug(`Execution slot for type "${job.type}" successfully acquired`, {
        job_id: job.id,
        type: job.type,
      });
      await next();
    } finally {
      // Release the job type slot (if it was acquired)
      if (typeSlotAcquired) {
        logger.debug('Releasing execution slot for job type', {
          job_id: job.id,
          type: job.type,
          worker: ctx.worker,
        });
        await storage.releaseTypeSlot(job.type, ctx.worker);
      }
    }
  };
}
/**
 * Acquires a distributed lock on the job (by job id).
 * If not acquired, we simply stop the pipeline.
 */
export function distributedLockMiddleware(): Middleware {
  return async (ctx, jobCtx, next) => {
    const { job } = jobCtx;
    const { lock: distributedLock, logger } = ctx;

    let jobLock: Lock | undefined;

    try {
      // Attempt to acquire a slot to run this job.
      jobLock = await distributedLock.acquire(formatLockName(job.id));
      if (!jobLock) {
        logger.debug(`Failed to acquire job lock "${formatLockName(job.id)}"`, {
          job_id: job.id,
          type: job.type,
        });
        return;
      }

      logger.debug('Acquired lock to execute job', {
        job_id: job.id,
        type: job.type,
      });

      try {
        await next();
      } finally {
        // Release the lock (if acquired)
        if (jobLock) {
          logger.debug('Releasing job lock', {
            job_id: job.id,
            type: job.type,
          });
          await jobLock.release();
        }
      }
    } catch (error) {
      let errorMessage = error instanceof Error ? error.message : String(error);
      logger.error('Error performing job lock operation', {
        job_id: job.id,
        type: job.type,
        error: errorMessage,
        error_stack: error instanceof Error ? error.stack : undefined,
      });
    }
  };
}
/**
 * Check if the job has expired. If so, fail the job immediately.
 */
export function expirationCheckMiddleware(): Middleware {
  return async (ctx, jobCtx, next) => {
    const { job, now } = jobCtx;
    const { stateMachine, logger } = ctx;

    if (job.expiresAt && job.expiresAt < now) {
      logger.debug('Job has expired, failing the job', {
        job_id: job.id,
        type: job.type,
        expires_at: job.expiresAt,
      });

      const err = new JobExpiredError(`Job "${job.id}" expired at ${job.expiresAt}`);
      await stateMachine.fail(job, undefined, false, err, err.message, err.stack);
      return;
    }

    await next();
  };
}

/**
 * Calls the state machine to 'start' the job.
 * Saves `jobRunId` and `attempt` in `jobCtx` for use downstream.
 */
export function startJobMiddleware(): Middleware {
  return async (ctx, jobCtx, next) => {
    const { job } = jobCtx;
    const { stateMachine, logger } = ctx;

    const { jobRunId, attempt } = await stateMachine.start(job);

    logger.debug('Started job execution', {
      job_id: job.id,
      job_run_id: jobRunId,
      type: job.type,
      attempt,
    });

    jobCtx.attempt = attempt;
    jobCtx.jobRunId = jobRunId;

    await next();
  };
}
/**
 * Actually runs the job's handler (in-process or fork mode).
 * If it throws, we do NOT catch here, so error handling can
 * happen in a higher-level error handler that calls `failJobMiddleware`.
 */
export function executeJobMiddleware(
  createJobContextImpl: (
    jobDef: JobDefinition,
    job: Job,
    jobRun: { id: string; attempt: number; startedAt: Date }
  ) => JobContextImpl
): Middleware {
  return async (ctx, jobCtx, next) => {
    const { executeForkMode, logger } = ctx;
    const { job, jobDef, jobRunId, attempt, now } = jobCtx;

    // Create a job context
    const jobRunContext = createJobContextImpl(jobDef, job, {
      id: jobRunId!,
      attempt: attempt!,
      startedAt: now,
    });

    // Decide how to execute
    if (jobDef.forkMode === true) {
      logger.debug('Executing job in fork mode', {
        job_id: job.id,
        type: job.type,
        fork_helper_path: jobDef.forkHelperPath,
      });

      await executeForkMode(jobDef, job, jobRunContext);
    } else {
      logger.debug('Executing job handler in process', {
        job_id: job.id,
        type: job.type,
      });

      await jobDef.handle(job.data as typeof jobDef.schema, jobRunContext);
    }

    await next();
  };
}
/**
 * Completes the job if no error is thrown. If an error is thrown, fail the job.
 * We wrap `next()` in try/catch to intercept errors from subsequent middlewares
 * (i.e. the job execution).
 */
export function completionMiddleware(): Middleware {
  return async (ctx, jobCtx, next) => {
    const { job, jobRunId } = jobCtx;
    const { stateMachine, logger } = ctx;

    try {
      // Run subsequent middlewares (i.e. job execution)
      await next();

      // If we reach here, job execution completed successfully
      if (jobRunId) {
        logger.debug('Job completed successfully', {
          job_id: job.id,
          type: job.type,
        });
        await stateMachine.complete(job, jobRunId);
      }
    } catch (error) {
      let errorMessage = error instanceof Error ? error.message : String(error);
      if (error instanceof z.ZodError) {
        const flat = error.errors.map((err) => `${err.message}`).join(',');
        errorMessage = `Zod validation error: ${flat}`;
      }

      logger.error('Error processing job', {
        job_id: job.id,
        type: job.type,
        error: errorMessage,
        error_stack: error instanceof Error ? error.stack : undefined,
      });

      await stateMachine.fail(
        job,
        jobRunId,
        true, // willRetry
        error as Error,
        errorMessage,
        error instanceof Error ? error.stack : undefined
      );
    }
  };
}

/**
 * Creates a job pipeline.
 * @param createJobContextImpl A function that creates a job context.
 * @returns A function that can be used to run a job.
 */
export function createJobPipeline(
  createJobContextImpl: (
    jobDef: JobDefinition,
    job: Job,
    jobRun: { id: string; attempt: number; startedAt: Date }
  ) => JobContextImpl
) {
  const pipeline = composeMiddlewares([
    distributedLockMiddleware(),
    limitExecutions(),
    limitExecutionsByType(),
    expirationCheckMiddleware(),
    startJobMiddleware(),
    completionMiddleware(),
    executeJobMiddleware(createJobContextImpl),
  ]);

  return async (taskCtx: PendingJobsContext, job: Job, jobDef: JobDefinition): Promise<void> => {
    const jobCtx: JobContextData = {
      job,
      jobDef,
      now: new Date(),
    };
    await pipeline(taskCtx, jobCtx, async () => {
      // If we exhaust all pipeline steps without returning early,
      // nothing else to do.
    });
  };
}
