import { z } from 'zod';

/**
 * Schema for job log level
 */
export const JobLogLevelSchema = z.enum(['debug', 'info', 'warn', 'error']);

/**
 * Schema for a job log entry
 */
export const JobLogEntrySchema = z.object({
  /**
   * Unique identifier for the log entry
   */
  id: z.string(),

  /**
   * ID of the associated job.
   */
  jobId: z.string(),

  /**
   * ID of the associated run.
   */
  jobRunId: z.string().optional(),

  /**
   * When the log entry was created.
   */
  timestamp: z.date().default(() => new Date()),

  /**
   * Log level.
   */
  level: JobLogLevelSchema,

  /**
   * Log message
   */
  message: z.string(),

  /**
   * Additional metadata
   */
  metadata: z.record(z.unknown()).optional(),
});

/**
 * Schema for creating a new job log entry
 */
export const CreateJobLogSchema = JobLogEntrySchema.omit({
  id: true,
}).partial({
  timestamp: true,
  jobRunId: true,
  metadata: true,
});

/**
 * Type for a job log entry
 */
export type JobLogEntry = z.infer<typeof JobLogEntrySchema>;

/**
 * Type for creating a new job log entry
 */
export type CreateJobLog = z.infer<typeof CreateJobLogSchema>;
