import { z } from 'zod';
import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';

import { JobScheduler } from './index.js';
import { EmptyLogger } from '../logger/index.js';
import { InMemoryJobStorage } from '../storage/memory/index.js';
import { JobDefinitionNotFoundError, JobAlreadyRegisteredError } from './types.js';
import { defaultValues, prepareEnvironment, resetEnvironment } from '../test-helpers/defaults.js';

describe('JobScheduler', () => {
  let scheduler: JobScheduler;
  let storage: InMemoryJobStorage;

  beforeEach(() => {
    prepareEnvironment();

    storage = new InMemoryJobStorage();
    scheduler = new JobScheduler({
      storage,
      logger: new EmptyLogger(),
      worker: defaultValues.workerName,
      jobProcessingInterval: defaultValues.pollInterval,
      jobSchedulingInterval: defaultValues.pollInterval,
      maxConcurrentJobs: defaultValues.maxConcurrentJobs,
      jobLockTTL: defaultValues.jobLockTTL,
      leaderLockTTL: defaultValues.distributedLockTTL,
    });

    // Start the scheduler and wait for first interval execution
    scheduler.start();

    // Wait for job processing interval.
    vi.advanceTimersByTimeAsync(defaultValues.pollInterval);
  });

  afterEach(async () => {
    // Stop the scheduler first to clear intervals
    await scheduler.stop(true);

    // Reset the  environment
    resetEnvironment();
  });

  describe('job registration', () => {
    it('should register a job successfully', () => {
      const jobDefinition = {
        type: 'test-job',
        concurrency: 20,
        schema: z.object({ data: z.string() }),
        handle: async (): Promise<void> => {},
      };

      scheduler.registerJob(jobDefinition);
      expect(() => scheduler.registerJob(jobDefinition)).toThrow(JobAlreadyRegisteredError);
    });

    it('should throw when registering duplicate job type', () => {
      const jobDefinition = {
        type: 'test-job',
        concurrency: 20,
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
        concurrency: 20,
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
        concurrency: 20,
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
        concurrency: 20,
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
        concurrency: 20,
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
          maxRetries: 0, // Set low max retries to fail faster
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
        concurrency: 20,
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
        concurrency: 20,
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
        concurrency: 20,
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

  describe('shutdown behavior', () => {
    it('should wait for running jobs to complete during graceful shutdown', async () => {
      const running: string[] = [];
      const jobDefinition = {
        type: 'test-job',
        concurrency: 20,
        schema: z.object({ data: z.string() }),
        handle: async (data: { data: string }): Promise<void> => {
          running.push(data.data);
          await new Promise((resolve) => setTimeout(resolve, 2000)); // Job takes 2 seconds
          running.splice(running.indexOf(data.data), 1);
        },
      };

      scheduler = new JobScheduler({
        storage,
        logger: new EmptyLogger(),
        worker: 'test-node',
        shutdownTimeout: 5000, // 5 second timeout
        jobProcessingInterval: 100,
      });
      scheduler.start();
      await vi.advanceTimersByTimeAsync(100);

      scheduler.registerJob(jobDefinition);
      await scheduler.enqueue('test-job', { data: 'job-1' });

      // Wait for job to start
      await vi.advanceTimersByTimeAsync(100);
      expect(running.length).toBe(1);

      // Initiate graceful shutdown
      const stopPromise = scheduler.stop(false);

      // Advance time to complete the job (2000ms)
      await vi.advanceTimersByTimeAsync(2000);

      await stopPromise;
      expect(running.length).toBe(0);
    });

    it('should respect shutdownTimeout when jobs take too long', async () => {
      const running: string[] = [];
      const jobDefinition = {
        type: 'test-job',
        concurrency: 20,
        schema: z.object({ data: z.string() }),
        handle: async (data: { data: string }): Promise<void> => {
          running.push(data.data);
          await new Promise((resolve) => setTimeout(resolve, 10000)); // Job takes 10 seconds
          running.splice(running.indexOf(data.data), 1);
        },
      };

      scheduler = new JobScheduler({
        storage,
        logger: new EmptyLogger(),
        worker: 'test-node',
        shutdownTimeout: 2000, // 2 second timeout
        jobProcessingInterval: 100,
      });
      scheduler.start();
      await vi.advanceTimersByTimeAsync(100);

      scheduler.registerJob(jobDefinition);
      await scheduler.enqueue('test-job', { data: 'job-1' });

      // Wait for job to start
      await vi.advanceTimersByTimeAsync(100);
      expect(running.length).toBe(1);

      // Initiate graceful shutdown
      const stopPromise = scheduler.stop(false);

      // Advance time past the shutdown timeout
      await vi.advanceTimersByTimeAsync(2000);

      await stopPromise;
      expect(running.length).toBe(1); // Job should still be running
    });

    it.only('should force stop immediately when force=true', async () => {
      const running: string[] = [];
      const jobDefinition = {
        type: 'test-job',
        concurrency: 20,
        schema: z.object({ data: z.string() }),
        handle: async (data: { data: string }): Promise<void> => {
          running.push(data.data);
          await new Promise((resolve) => setTimeout(resolve, 5000)); // Job takes 5 seconds
          running.splice(running.indexOf(data.data), 1);
        },
      };

      scheduler.registerJob(jobDefinition);
      await scheduler.enqueue('test-job', { data: 'job-1' });

      // Wait for job to start
      await vi.advanceTimersByTimeAsync(100);
      expect(running.length).toBe(1);

      // Force stop
      await scheduler.stop(true);
      expect(running.length).toBe(1); // Job is still in running state but scheduler is stopped
    });
  });
});
