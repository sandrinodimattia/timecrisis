import { z } from 'zod';
import { describe, it, expect, beforeEach, vi, afterEach } from 'vitest';

import { JobScheduler } from './index';
import { EmptyLogger } from '../logger';
import { InMemoryJobStorage } from '../storage/memory';
import { JobDefinitionNotFoundError, JobAlreadyRegisteredError } from './types';

describe('JobScheduler', () => {
  let scheduler: JobScheduler;
  let storage: InMemoryJobStorage;
  const now = new Date('2025-01-23T00:00:00.000Z');

  beforeEach(async () => {
    vi.useFakeTimers();
    vi.setSystemTime(now);

    storage = new InMemoryJobStorage();
    scheduler = new JobScheduler({
      storage,
      logger: new EmptyLogger(),
      node: 'test-node',
      pollInterval: 100, // Faster polling for tests
      maxConcurrentJobs: 5,
      jobLockTTL: 1000,
      leaderLockTTL: 1000,
    });

    // Start the scheduler and wait for first interval execution
    await scheduler.start();
    // Wait for leadership acquisition and first interval
    await vi.advanceTimersByTimeAsync(100);
    // Wait for first job processing interval
    await vi.advanceTimersByTimeAsync(100);
  });

  afterEach(async () => {
    // Stop the scheduler first to clear intervals
    await scheduler.stop(true);
    // Reset storage between tests
    storage = new InMemoryJobStorage();
    // Clear all timers and mocks
    vi.clearAllTimers();
    vi.clearAllMocks();
    vi.useRealTimers();
  });

  describe('job registration', () => {
    it('should register a job successfully', () => {
      const jobDefinition = {
        type: 'test-job',
        schema: z.object({ data: z.string() }),
        handle: async (): Promise<void> => {},
      };

      scheduler.registerJob(jobDefinition);
      expect(() => scheduler.registerJob(jobDefinition)).toThrow(JobAlreadyRegisteredError);
    });

    it('should throw when registering duplicate job type', () => {
      const jobDefinition = {
        type: 'test-job',
        schema: z.object({ data: z.string() }),
        handle: async (): Promise<void> => {},
      };

      scheduler.registerJob(jobDefinition);
      expect(() => scheduler.registerJob(jobDefinition)).toThrow(JobAlreadyRegisteredError);
    });
  });

  describe('job enqueueing', () => {
    it('should enqueue a job successfully', async () => {
      const jobDefinition = {
        type: 'test-job',
        schema: z.object({ data: z.string() }),
        handle: async (): Promise<void> => {},
      };

      scheduler.registerJob(jobDefinition);
      const jobId = await scheduler.enqueue('test-job', { data: 'test' });
      expect(jobId).toBeDefined();

      const job = await storage.getJob(jobId);
      expect(job).toBeDefined();
      expect(job?.type).toBe('test-job');
      expect(job?.data).toEqual({ data: 'test' });
    });

    it('should throw when enqueueing unregistered job type', async () => {
      await expect(scheduler.enqueue('non-existent', { data: 'test' })).rejects.toThrow(
        JobDefinitionNotFoundError
      );
    });

    it('should validate job data against schema', async () => {
      const jobDefinition = {
        type: 'test-job',
        schema: z.object({ data: z.string() }),
        handle: async (): Promise<void> => {},
      };

      scheduler.registerJob(jobDefinition);
      await expect(scheduler.enqueue('test-job', { data: 123 } as unknown)).rejects.toThrow();
    });
  });

  describe('job execution', () => {
    it('should execute a job successfully', async () => {
      const handleMock = vi.fn();
      const jobDefinition = {
        type: 'test-job',
        schema: z.object({ data: z.string() }),
        handle: handleMock,
      };

      scheduler.registerJob(jobDefinition);
      const jobId = await scheduler.enqueue('test-job', { data: 'test' });

      // Wait for job to be processed
      await vi.advanceTimersByTimeAsync(100); // Wait for next poll interval
      await vi.advanceTimersByTimeAsync(100); // Wait for job execution
      await vi.advanceTimersByTimeAsync(100); // Wait for job completion

      expect(handleMock).toHaveBeenCalledTimes(1);
      expect(handleMock).toHaveBeenCalledWith({ data: 'test' }, expect.objectContaining({ jobId }));

      const job = await storage.getJob(jobId);
      expect(job?.status).toBe('completed');
    });

    it('should handle job failures', async () => {
      const error = new Error('Job failed');
      const jobDefinition = {
        type: 'test-job',
        schema: z.object({ data: z.string() }),
        handle: async (): Promise<void> => {
          throw error;
        },
      };

      scheduler.registerJob(jobDefinition);
      const jobId = await scheduler.enqueue(
        'test-job',
        { data: 'test' },
        {
          maxRetries: 1, // Set low max retries to fail faster
        }
      );

      // Wait for job to be processed and marked as failed
      await vi.advanceTimersByTimeAsync(100); // Wait for next poll interval
      await vi.advanceTimersByTimeAsync(100); // Wait for job execution
      await vi.advanceTimersByTimeAsync(100); // Wait for failure handling

      const job = await storage.getJob(jobId);
      expect(job?.status).toBe('failed');
      expect(job?.failReason).toBe(error.message);
    });
  });

  describe('scheduler metrics', () => {
    it('should track job metrics correctly', async () => {
      const jobDefinition = {
        type: 'test-job',
        schema: z.object({ data: z.string() }),
        handle: async (): Promise<void> => {},
      };

      scheduler.registerJob(jobDefinition);
      await scheduler.enqueue('test-job', { data: 'test' });

      // Wait for job to be processed
      await vi.advanceTimersByTimeAsync(100); // Wait for next poll interval
      await vi.advanceTimersByTimeAsync(100); // Wait for job execution
      await vi.advanceTimersByTimeAsync(100); // Wait for job completion

      const metrics = await scheduler.getMetrics();
      expect(metrics.completed).toBe(1);
      expect(metrics.failed).toBe(0);
      expect(metrics.running).toBe(0);
    });
  });

  describe('job scheduling', () => {
    it('should schedule a recurring job', async () => {
      const handleMock = vi.fn();
      const jobDefinition = {
        type: 'test-job',
        schema: z.object({ data: z.string() }),
        handle: handleMock,
      };

      scheduler.registerJob(jobDefinition);
      await scheduler.schedule(
        'test-recurring-job',
        'test-job',
        { data: 'test' },
        {
          scheduleType: 'cron',
          scheduleValue: '* * * * *', // Every minute
        }
      );

      // Advance time in smaller increments to avoid lock expiration
      for (let i = 0; i < 60; i++) {
        await vi.advanceTimersByTimeAsync(1000); // Advance 1 second at a time
      }

      // Process the scheduled jobs
      await vi.advanceTimersByTimeAsync(100); // Wait for next poll interval
      await vi.advanceTimersByTimeAsync(100); // Wait for scheduled job processing

      // Process the enqueued job
      await vi.advanceTimersByTimeAsync(100); // Wait for next poll interval
      await vi.advanceTimersByTimeAsync(100); // Wait for job execution
      await vi.advanceTimersByTimeAsync(100); // Wait for job completion

      expect(handleMock).toHaveBeenCalled();
      const metrics = await scheduler.getMetrics();
      expect(metrics.completed).toBeGreaterThan(0);
    });
  });

  describe('concurrency control', () => {
    it('should respect maxConcurrentJobs limit', async () => {
      const delay = (ms: number): Promise<void> =>
        new Promise<void>((resolve) => {
          setTimeout(() => {
            resolve();
          }, ms);
          vi.advanceTimersByTimeAsync(ms);
        });
      const running: string[] = [];

      const jobDefinition = {
        type: 'test-job',
        schema: z.object({ data: z.string() }),
        handle: async (data: { data: string }): Promise<void> => {
          running.push(data.data);
          await delay(500); // Longer delay to ensure overlap
          running.splice(running.indexOf(data.data), 1);
        },
      };

      scheduler.registerJob(jobDefinition);

      // Enqueue 10 jobs
      for (let i = 0; i < 10; i++) {
        await scheduler.enqueue('test-job', { data: `job-${i}` });
      }

      // Wait for some jobs to be processed
      await vi.advanceTimersByTimeAsync(100); // Wait for next poll interval
      await vi.advanceTimersByTimeAsync(500); // Wait for first batch to complete
      await vi.advanceTimersByTimeAsync(100); // Wait for next poll interval
      await vi.advanceTimersByTimeAsync(500); // Wait for second batch to complete

      // At any point, there should be no more than 5 jobs running
      expect(running.length).toBeLessThanOrEqual(5);

      // Wait for all jobs to complete
      await vi.advanceTimersByTimeAsync(100); // Wait for next poll interval
      await vi.advanceTimersByTimeAsync(500); // Wait for final batch to complete
      expect(running.length).toBe(0);
    });
  });
});
