import { TaskContext } from '../types.js';

import { Job } from '../../storage/schemas/job.js';
import { JobContext, JobDefinition } from '../../scheduler/types.js';

export interface PendingJobsContext extends TaskContext {
  /**
   * Lock lifetime in milliseconds.
   */
  jobLockTTL: number;

  /**
   * Poll interval in milliseconds.
   */
  pollInterval: number;

  /**
   * Execute the job in fork mode.
   * @param jobDef Job definition
   * @param job Job
   * @param ctx Job context
   * @returns
   */
  executeForkMode: (jobDef: JobDefinition, job: Job, ctx: JobContext) => Promise<void>;
}

export interface JobContextData {
  /**
   * The current date and time.
   */
  now: Date;

  /**
   * The job which is currently being processed.
   */
  job: Job;

  /**
   * The job run ID.
   */
  jobRunId?: string;

  /**
   * The job definition.
   */
  jobDef: JobDefinition;

  /**
   * The current attempt.
   */
  attempt?: number;
}

export type Middleware = (
  ctx: PendingJobsContext,
  jobCtx: JobContextData,
  next: () => Promise<void>
) => Promise<void>;
