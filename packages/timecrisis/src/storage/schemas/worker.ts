import { z } from 'zod';

/**
 * Schema for a worker instance
 */
export const WorkerSchema = z.object({
  /**
   * Unique identifier of the worker
   */
  id: z.string(),

  /**
   * Name of the worker instance
   */
  name: z.string(),

  /**
   * When the worker was first seen
   */
  first_seen: z.date(),

  /**
   * Last time the worker sent a heartbeat
   */
  last_heartbeat: z.date(),
});

/**
 * Schema for registering a new worker
 */
export const RegisterWorkerSchema = WorkerSchema.omit({
  id: true,
  first_seen: true,
  last_heartbeat: true,
});

/**
 * Schema for updating a worker's heartbeat
 */
export const UpdateWorkerHeartbeatSchema = WorkerSchema.pick({
  last_heartbeat: true,
});

/**
 * Type for a worker instance
 */
export type Worker = z.infer<typeof WorkerSchema>;

/**
 * Type for registering a new worker
 */
export type RegisterWorker = z.infer<typeof RegisterWorkerSchema>;

/**
 * Type for updating a worker's heartbeat
 */
export type UpdateWorkerHeartbeat = z.infer<typeof UpdateWorkerHeartbeatSchema>;
