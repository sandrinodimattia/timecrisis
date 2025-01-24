import { z } from 'zod';

/**
 * Schema for a dead letter job
 */
export const DeadLetterJobSchema = z.object({
  /**
   * Unique identifier for the dead letter entry
   */
  id: z.string(),

  /**
   * ID of the original job
   */
  jobId: z.string(),

  /**
   * Type of the original job
   */
  jobType: z.string(),

  /**
   * Original job data
   */
  data: z.unknown().optional(),

  /**
   * When the job failed
   */
  failedAt: z.date(),

  /**
   * Reason for failure
   */
  reason: z.string(),
});

/**
 * Schema for creating a new dead letter job
 */
export const CreateDeadLetterJobSchema = DeadLetterJobSchema.omit({
  id: true,
}).extend({
  failedAt: z.date().default(() => new Date()),
});

/**
 * Type for a dead letter job
 */
export type DeadLetterJob = z.infer<typeof DeadLetterJobSchema>;

/**
 * Type for creating a new dead letter job
 */
export type CreateDeadLetterJob = z.infer<typeof CreateDeadLetterJobSchema>;
