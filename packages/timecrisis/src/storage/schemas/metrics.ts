import { z } from 'zod';

/**
 * Schema for job counts
 */
export const JobCountsSchema = z.object({
  total: z.number().int().min(0).default(0),
  pending: z.number().int().min(0).default(0),
  completed: z.number().int().min(0).default(0),
  failed: z.number().int().min(0).default(0),
  deadLetter: z.number().int().min(0).default(0),
  scheduled: z.number().int().min(0).default(0),
});

/**
 * Schema for job storage metrics
 */
export const JobStorageMetricsSchema = z.object({
  /**
   * Average duration by job type in milliseconds
   */
  averageDurationByType: z.record(z.number().min(0)),

  /**
   * Failure rate by job type (0-1)
   */
  failureRateByType: z.record(z.number().min(0).max(1)),

  /**
   * Job counts by status
   */
  jobs: JobCountsSchema,
});

/**
 * Type for job storage metrics
 */
export type JobStorageMetrics = z.infer<typeof JobStorageMetricsSchema>;
