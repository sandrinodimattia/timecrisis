import { z } from 'zod';

/**
 * Schema for job status
 */
export const JobStatusSchema = z.enum(['pending', 'running', 'completed', 'failed', 'canceled']);

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
   * Type of job, used to match with a job handler.
   */
  type: z.string(),

  /**
   * Entity ID for grouping jobs. This is the ID of the user, the file, ...
   */
  entityId: z.string().nullable().optional(),

  /**
   * ID of the scheduled job that created this job, if any.
   */
  scheduledJobId: z.string().nullable().optional(),

  /**
   * Job-specific data which will be provided to the job when it runs.
   */
  data: z.unknown(),

  /**
   * Job priority (1 to 100, lower is more important).
   */
  priority: z.number().int().min(1).max(100).default(10),

  /**
   * Current status of the job.
   */
  status: JobStatusSchema.default('pending'),

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
  failReason: z.string().nullable().optional(),

  /**
   * Number of times this job has failed.
   */
  failCount: z.number().int().min(0).default(0),

  /**
   * When the job was started.
   */
  startedAt: z.date().nullable().optional(),

  /**
   * When the job should start executing.
   */
  runAt: z.date().nullable().optional(),

  /**
   * When the job expires. This is useful for when a job is time sensitive.
   * After this date, the job will be automatically failed.
   */
  expiresAt: z.date().nullable().optional(),

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
  priority: true,
  status: true,
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
