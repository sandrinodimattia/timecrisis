import { z } from 'zod';

/**
 * Schema for job status
 */
export const JobStatusSchema = z.enum(['pending', 'running', 'completed', 'failed', 'scheduled']);

/**
 * Schema for backoff strategy
 */
export const BackoffStrategySchema = z.enum(['exponential', 'linear']);

/**
 * Schema for a job
 */
export const JobSchema = z.object({
  /**
   * Unique identifier for the job.
   */
  id: z.string(),

  /**
   * Reference ID for grouping jobs. This is the ID of the user, the file, ...
   */
  referenceId: z.string().nullable().optional(),

  /**
   * Type of job, used to match with a job handler.
   */
  type: z.string(),

  /**
   * Job-specific data which will be provided to the job when it runs.
   */
  data: z.unknown(),

  /**
   * Job priority (-20 to 20, higher is more important).
   */
  priority: z.number().int().min(-20).max(20).default(1),

  /**
   * Current status of the job.
   */
  status: JobStatusSchema.default('pending'),

  /**
   * Progress of the job (0-100)
   */
  progress: z.number().min(0).max(100).default(0),

  /**
   * Duration of execution in milliseconds.
   */
  executionDuration: z.number().int().min(0).optional(),

  /**
   * Number of retries attempted.
   */
  attempts: z.number().int().min(0).default(0),

  /**
   * Maximum number of retries allowed.
   */
  maxRetries: z.number().int().min(0).default(0),

  /**
   * Strategy for retry backoff.
   */
  backoffStrategy: BackoffStrategySchema.default('exponential'),

  /**
   * Reason for failure if job failed.
   */
  failReason: z.string().optional(),

  /**
   * Number of times this job has failed.
   */
  failCount: z.number().int().min(0).default(0),

  /**
   * When the job expires. This is useful for when a job is time sensitive.
   * After this date, the job will be automatically failed.
   */
  expiresAt: z.date().nullable().optional(),

  /**
   * When the job was last locked.
   */
  lockedAt: z.date().nullable().optional(),

  /**
   * When the job was started.
   */
  startedAt: z.date().nullable().optional(),

  /**
   * When the job should start executing.
   */
  runAt: z.date().nullable().optional(),

  /**
   * When the job was finished. Either successfully or with an error.
   */
  finishedAt: z.date().nullable().optional(),

  /**
   * When the job was created
   */
  createdAt: z.date(),

  /**
   * When the job was last updated
   */
  updatedAt: z.date(),
});

/**
 * Schema for creating a new job
 */
export const CreateJobSchema = JobSchema.omit({
  id: true,
  createdAt: true,
  updatedAt: true,
}).partial({
  progress: true,
  priority: true,
  status: true,
  attempts: true,
  maxRetries: true,
  backoffStrategy: true,
  failCount: true,
});

/**
 * Type for creating a new job
 */
export type CreateJob = z.infer<typeof CreateJobSchema>;

/**
 * Schema for updating an existing job
 */
export const UpdateJobSchema = JobSchema.omit({
  id: true,
  createdAt: true,
  updatedAt: true,
  type: true,
}).partial();

/**
 * Type for a job
 */
export type Job = z.infer<typeof JobSchema>;

/**
 * Type for updating an existing job
 */
export type UpdateJob = z.infer<typeof UpdateJobSchema>;
