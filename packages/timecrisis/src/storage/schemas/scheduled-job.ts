import { z } from 'zod';

/**
 * Schema for schedule type
 */
export const ScheduleTypeSchema = z.enum(['exact', 'cron', 'interval']);

/**
 * Schema for a scheduled job
 */
export const ScheduledJobSchema = z.object({
  /**
   * Unique identifier for the schedule.
   */
  id: z.string(),

  /**
   * Human-readable name.
   */
  name: z.string().min(1),

  /**
   * Type of job to create.
   */
  type: z.string().min(1),

  /**
   * Schedule type (cron or interval)
   */
  scheduleType: ScheduleTypeSchema.default('cron'),

  /**
   * Schedule value (cron expression or interval in ms)
   */
  scheduleValue: z.string(),

  /**
   * IANA time zone for cron schedules (e.g., 'Europe/Paris').
   * If not provided, cron schedules default to UTC.
   */
  timeZone: z.string().optional(),

  /**
   * Job-specific data
   */
  data: z.unknown().default({}),

  /**
   * Whether the schedule is enabled
   */
  enabled: z.boolean().default(true),

  /**
   * When the job was last scheduled
   */
  lastScheduledAt: z.date().nullable().optional(),

  /**
   * When the job should run next
   */
  nextRunAt: z.date().nullable().optional(),

  /**
   * Reference ID for the scheduled job
   */
  referenceId: z.string().nullable().optional(),

  /**
   * When the scheduled job was created
   */
  createdAt: z.date(),

  /**
   * When the scheduled job was last updated
   */
  updatedAt: z.date(),
});

/**
 * Schema for creating a new scheduled job
 */
export const CreateScheduledJobSchema = ScheduledJobSchema.omit({
  id: true,
  createdAt: true,
  updatedAt: true,
}).partial({
  data: true,
  enabled: true,
  lastScheduledAt: true,
  nextRunAt: true,
  referenceId: true,
});

/**
 * Schema for updating an existing scheduled job
 */
export const UpdateScheduledJobSchema = ScheduledJobSchema.omit({
  id: true,
  createdAt: true,
  updatedAt: true,
  type: true,
}).partial();

/**
 * Type for a scheduled job
 */
export type ScheduledJob = z.infer<typeof ScheduledJobSchema>;

/**
 * Type for creating a new scheduled job
 */
export type CreateScheduledJob = z.infer<typeof CreateScheduledJobSchema>;

/**
 * Type for updating an existing scheduled job
 */
export type UpdateScheduledJob = z.infer<typeof UpdateScheduledJobSchema>;
