import { z } from 'zod';

/**
 * Schema for job run status
 */
export const JobRunStatusSchema = z.enum(['running', 'completed', 'failed']);

/**
 * Schema for a job run
 */
export const JobRunSchema = z.object({
  /**
   * Unique identifier for the run.
   */
  id: z.string(),

  /**
   * ID of the associated job
   */
  jobId: z.string(),

  /**
   * Current status of the run
   */
  status: JobRunStatusSchema,

  /**
   * Progress of the job run (0-100)
   */
  progress: z.number().min(0).max(100).default(0),

  /**
   * When the run started
   */
  startedAt: z.date(),

  /**
   * When the run completed
   */
  finishedAt: z.date().optional(),

  /**
   * Which attempt this run represents
   */
  attempt: z.number().int().min(1).default(1),

  /**
   * Error message if run failed.
   */
  error: z.string().optional(),

  /**
   * Error stack trace if run failed.
   */
  error_stack: z.string().optional(),
});

/**
 * Schema for creating a new job run
 */
export const CreateJobRunSchema = JobRunSchema.omit({
  id: true,
}).partial({
  progress: true,
});

/**
 * Schema for updating an existing job run
 */
export const UpdateJobRunSchema = JobRunSchema.omit({
  id: true,
  jobId: true,
}).partial();

/**
 * Type for a job run
 */
export type JobRun = z.infer<typeof JobRunSchema>;

/**
 * Type for creating a new job run
 */
export type CreateJobRun = z.infer<typeof CreateJobRunSchema>;

/**
 * Type for updating an existing job run
 */
export type UpdateJobRun = z.infer<typeof UpdateJobRunSchema>;
