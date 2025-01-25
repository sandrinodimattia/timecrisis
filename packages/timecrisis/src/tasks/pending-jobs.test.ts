import { z } from 'zod';
import { describe, it, expect, vi, beforeEach, afterEach } from 'vitest';

import { EmptyLogger } from '../logger/index.js';
import { Job } from '../storage/schemas/index.js';
import { PendingJobsTask } from './pending-jobs.js';
import { JobDefinition } from '../scheduler/types.js';
import { MockJobStorage } from '../storage/mock/index.js';

describe('PendingJobsTask', () => {
  let storage: MockJobStorage;
  let jobs: Map<string, JobDefinition>;
  let task: PendingJobsTask;
  let executeForkMode: (jobDef: JobDefinition, job: Job, ctx: unknown) => Promise<void>;
  let touchJob: (jobId: string) => Promise<void>;
  const now = new Date('2025-01-23T00:00:00.000Z');

  beforeEach(() => {
    vi.useFakeTimers();
    vi.setSystemTime(now);

    storage = new MockJobStorage();
    jobs = new Map();
    executeForkMode = vi.fn();
    touchJob = vi.fn();

    task = new PendingJobsTask({
      storage,
      jobs,
      executeForkMode,
      touchJob,
      logger: new EmptyLogger(),
      worker: 'test-worker',
      maxConcurrentJobs: 20,
      jobLockTTL: 30000,
      pollInterval: 100,
    });

    vi.clearAllMocks();
  });

  afterEach(() => {
    storage.cleanup();
    vi.clearAllTimers();
    vi.clearAllMocks();
    vi.useRealTimers();
  });

  describe('execute', () => {
    it('should process pending jobs up to global concurrency limit', async () => {
      const jobPromises = Array.from({ length: 25 }, (_, i) =>
        storage.createJob({
          type: 'test',
          status: 'pending',
          data: { job: i },
          attempts: 0,
          maxRetries: 3,
          failCount: 0,
          priority: 1,
          backoffStrategy: 'exponential',
          failReason: undefined,
          runAt: null,
        })
      );
      await Promise.all(jobPromises);

      const jobDef = {
        handle: vi.fn().mockResolvedValue(undefined),
        concurrency: undefined, // Remove job-type concurrency to test global limit
      };
      jobs.set('test', jobDef as unknown as JobDefinition);

      await task.execute();

      expect(jobDef.handle).toHaveBeenCalledTimes(20);
      expect(task.getRunningCount()).toBe(0);
    });

    it('should respect job-type concurrency limits', async () => {
      // Create a concurrency tracking mechanism
      let runningJobs = 0;
      const maxConcurrency = 3;

      storage.acquireJobTypeSlot = vi.fn().mockImplementation(async () => {
        if (runningJobs >= maxConcurrency) {
          return false;
        }
        runningJobs++;
        return true;
      });

      storage.releaseJobTypeSlot = vi.fn().mockImplementation(async () => {
        runningJobs = Math.max(0, runningJobs - 1);
      });

      // Create test jobs
      const jobPromises = Array.from({ length: 10 }, (_, i) =>
        storage.createJob({
          type: 'test',
          status: 'pending',
          data: { job: i },
          attempts: 0,
          maxRetries: 3,
          failCount: 0,
          priority: 1,
          backoffStrategy: 'exponential',
          failReason: undefined,
          runAt: null,
        })
      );
      await Promise.all(jobPromises);

      const jobDef = {
        handle: vi.fn().mockImplementation(async () => {
          // Use setImmediate instead of setTimeout for testing
          await new Promise((resolve) => setImmediate(resolve));
        }),
        concurrency: maxConcurrency,
      };
      jobs.set('test', jobDef as unknown as JobDefinition);

      const executePromise = task.execute();

      // Advance all timers to complete any pending promises
      await vi.runAllTimersAsync();
      await executePromise;

      expect(storage.acquireJobTypeSlot).toHaveBeenCalledWith(
        'test',
        'test-worker',
        maxConcurrency
      );
      expect(jobDef.handle).toHaveBeenCalledTimes(maxConcurrency);
      expect(runningJobs).toBe(0); // All jobs should be finished
    });

    it('should handle job locking correctly', async () => {
      const jobDef = {
        handle: vi.fn().mockImplementation(async () => {
          await new Promise((resolve) => setImmediate(resolve));
        }),
        schema: z.object({}),
      };
      jobs.set('test', jobDef as unknown as JobDefinition);

      // Create a job
      const jobId = await storage.createJob({
        type: 'test',
        status: 'pending',
        data: {},
        attempts: 0,
        maxRetries: 3,
        failCount: 0,
        priority: 1,
        backoffStrategy: 'exponential',
      });

      // Start executing jobs
      const executePromise = task.execute();

      // Wait for job to start
      await vi.runOnlyPendingTimersAsync();

      // Complete execution
      await vi.runAllTimersAsync();
      await executePromise;

      // Job should have been processed
      expect(jobDef.handle).toHaveBeenCalled();

      // Verify job state
      const job = await storage.getJob(jobId);
      expect(job?.status).toBe('completed');
    });

    it('should handle lock expiration edge cases', async () => {
      const jobDef = {
        handle: vi.fn().mockImplementation(async () => {
          await new Promise((resolve) => setImmediate(resolve));
        }),
        schema: z.object({}),
      };
      jobs.set('test', jobDef as unknown as JobDefinition);

      // Create a job
      const jobId = await storage.createJob({
        type: 'test',
        status: 'pending',
        data: {},
        attempts: 0,
        maxRetries: 3,
        failCount: 0,
        priority: 1,
        backoffStrategy: 'exponential',
      });

      // Simulate a stale lock by another worker
      const now = new Date();
      const expiredTime = now.getTime() - 60000; // 1 minute ago
      storage.setLock(jobId, 'other-worker', expiredTime);

      // Start executing jobs
      const executePromise = task.execute();

      // Wait for job to start
      await vi.runOnlyPendingTimersAsync();

      // Complete execution
      await vi.runAllTimersAsync();
      await executePromise;

      // Job should be processed because lock expired
      expect(jobDef.handle).toHaveBeenCalled();

      // Verify job state
      const job = await storage.getJob(jobId);
      expect(job?.status).toBe('completed');
      expect(job?.lockedBy).toBe(null);
      expect(job?.lockedAt).toBe(null);
    });

    it('should handle fork mode execution', async () => {
      const jobId = await storage.createJob({
        type: 'test',
        status: 'pending',
        data: { test: true },
        attempts: 0,
        maxRetries: 3,
        failCount: 0,
        priority: 1,
        backoffStrategy: 'exponential',
        failReason: undefined,
        runAt: null,
      });

      const jobDef = {
        handle: vi.fn(),
        forkMode: true,
        concurrency: 1,
      };
      jobs.set('test', jobDef as unknown as JobDefinition);

      await task.execute();

      expect(executeForkMode).toHaveBeenCalledWith(
        jobDef,
        expect.objectContaining({ id: jobId }),
        expect.any(Object)
      );
    });

    it('should handle exponential backoff on failure', async () => {
      const jobId = await storage.createJob({
        type: 'test',
        status: 'pending',
        data: { test: true },
        attempts: 1,
        maxRetries: 3,
        failCount: 1,
        priority: 1,
        backoffStrategy: 'exponential',
        failReason: undefined,
        runAt: null,
      });

      const error = new Error('Test error');
      const jobDef = {
        handle: vi.fn().mockRejectedValue(error),
        concurrency: 1,
        schema: z.object({}),
      };
      jobs.set('test', jobDef as unknown as JobDefinition);

      await task.execute();

      const updatedJob = await storage.getJob(jobId);
      expect(updatedJob!.status).toBe('pending');
      expect(updatedJob!.runAt).toBeInstanceOf(Date);
      expect(updatedJob!.failCount).toBe(2);
      expect(updatedJob!.failReason).toBe('Test error');

      const expectedDelay = 20000;
      const actualDelay = updatedJob!.runAt!.getTime() - now.getTime();
      expect(actualDelay).toBe(expectedDelay);
    });

    it('should move job to dead letter queue after max retries', async () => {
      const jobId = await storage.createJob({
        type: 'test',
        status: 'pending',
        data: { test: true },
        attempts: 3,
        maxRetries: 3,
        failCount: 2,
        priority: 1,
        backoffStrategy: 'exponential',
        failReason: undefined,
        runAt: null,
      });

      const error = new Error('Final failure');
      const jobDef = {
        handle: vi.fn().mockRejectedValue(error),
        concurrency: 1,
        schema: z.object({}),
      };
      jobs.set('test', jobDef as unknown as JobDefinition);

      await task.execute();

      const updatedJob = await storage.getJob(jobId);
      expect(updatedJob!.status).toBe('failed');
      expect(updatedJob!.failCount).toBe(3);
      expect(updatedJob!.failReason).toBe('Final failure');

      expect(storage.createDeadLetterJob).toHaveBeenCalledWith(
        expect.objectContaining({
          jobId: jobId,
          jobType: 'test',
          reason: 'Final failure',
        })
      );
    });

    it('should handle Zod validation errors', async () => {
      const jobId = await storage.createJob({
        type: 'test',
        status: 'pending',
        data: { test: true },
        attempts: 0,
        maxRetries: 3,
        failCount: 0,
        priority: 1,
        backoffStrategy: 'exponential',
        failReason: undefined,
        runAt: null,
      });

      const zodError = new z.ZodError([
        {
          code: 'invalid_type',
          expected: 'string',
          received: 'boolean',
          path: ['test'],
          message: 'Expected string, received boolean',
        },
      ]);

      const jobDef = {
        handle: vi.fn().mockRejectedValue(zodError),
        concurrency: 1,
        schema: z.object({}),
      };
      jobs.set('test', jobDef as unknown as JobDefinition);

      await task.execute();

      const updatedJob = await storage.getJob(jobId);
      expect(updatedJob!.status).toBe('pending');
      expect(updatedJob!.failReason).toBe(
        'Zod validation error: Expected string, received boolean'
      );
      expect(updatedJob!.failCount).toBe(1);
    });

    it('should update progress correctly', async () => {
      const jobId = await storage.createJob({
        type: 'test',
        status: 'pending',
        data: { test: true },
        attempts: 0,
        maxRetries: 3,
        failCount: 0,
        priority: 1,
        backoffStrategy: 'exponential',
        failReason: undefined,
        runAt: null,
      });

      const jobDef = {
        handle: vi.fn().mockImplementation(async (_, ctx) => {
          await ctx.updateProgress(50);
          await ctx.updateProgress(100);
        }),
        concurrency: 1,
        schema: z.object({}),
      };
      jobs.set('test', jobDef as unknown as JobDefinition);

      await task.execute();

      expect(storage.updateJob).toHaveBeenCalledWith(
        jobId,
        expect.objectContaining({ progress: 50 })
      );
      expect(storage.updateJob).toHaveBeenCalledWith(
        jobId,
        expect.objectContaining({ progress: 100 })
      );
    });

    it('should handle invalid job types', async () => {
      await storage.createJob({
        type: 'invalid-type',
        status: 'pending',
        data: { test: true },
        attempts: 0,
        maxRetries: 3,
        failCount: 0,
        priority: 1,
        backoffStrategy: 'exponential',
        failReason: undefined,
        runAt: null,
      });

      await task.execute();

      expect(storage.updateJob).not.toHaveBeenCalled();
    });

    it('should track job execution duration', async () => {
      const jobId = await storage.createJob({
        type: 'test',
        status: 'pending',
        data: { test: true },
        attempts: 0,
        maxRetries: 3,
        failCount: 0,
        priority: 1,
        backoffStrategy: 'exponential',
        failReason: undefined,
        runAt: null,
      });

      const jobDef = {
        handle: vi.fn().mockImplementation(async () => {
          vi.advanceTimersByTime(5000); // Simulate 5 seconds of work
        }),
        concurrency: 1,
      };
      jobs.set('test', jobDef as unknown as JobDefinition);

      await task.execute();

      const updatedJob = await storage.getJob(jobId);
      expect(updatedJob!.executionDuration).toBe(5000);
      expect(storage.updateJob).toHaveBeenCalledWith(
        jobId,
        expect.objectContaining({
          status: 'completed',
          executionDuration: 5000,
        })
      );
    });

    it('should update job run status correctly', async () => {
      const jobId = await storage.createJob({
        type: 'test',
        status: 'pending',
        data: { test: true },
        attempts: 0,
        maxRetries: 3,
        failCount: 0,
        priority: 1,
        backoffStrategy: 'exponential',
        failReason: undefined,
        runAt: null,
      });

      const jobDef = {
        handle: vi.fn().mockResolvedValue(undefined),
        concurrency: 1,
      };
      jobs.set('test', jobDef as unknown as JobDefinition);

      await task.execute();

      // Verify job run was created and updated
      expect(storage.createJobRun).toHaveBeenCalledWith(
        expect.objectContaining({
          jobId,
          status: 'running',
        })
      );

      expect(storage.updateJobRun).toHaveBeenCalledWith(
        expect.any(String),
        expect.objectContaining({
          status: 'completed',
          progress: 100,
          finishedAt: expect.any(Date),
        })
      );
    });

    it('should handle linear backoff strategy', async () => {
      const jobId = await storage.createJob({
        type: 'test',
        status: 'pending',
        data: { test: true },
        attempts: 1,
        maxRetries: 3,
        failCount: 1,
        priority: 1,
        backoffStrategy: 'linear',
        failReason: undefined,
        runAt: null,
      });

      const error = new Error('Test error');
      const jobDef = {
        handle: vi.fn().mockRejectedValue(error),
        concurrency: 1,
      };
      jobs.set('test', jobDef as unknown as JobDefinition);

      await task.execute();

      const updatedJob = await storage.getJob(jobId);
      expect(updatedJob!.status).toBe('pending');

      // Fixed backoff should always be 10 seconds
      const expectedDelay = 10000;
      const actualDelay = updatedJob!.runAt!.getTime() - now.getTime();
      expect(actualDelay).toBe(expectedDelay);
    });

    it('should log job context messages', async () => {
      const jobId = await storage.createJob({
        type: 'test',
        status: 'pending',
        data: { test: true },
        attempts: 0,
        maxRetries: 3,
        failCount: 0,
        priority: 1,
        backoffStrategy: 'exponential',
        failReason: undefined,
        runAt: null,
      });

      const jobDef = {
        handle: vi.fn().mockImplementation(async (data, ctx) => {
          await ctx.log('info', 'Test message');
          await ctx.log('warn', 'Warning message');
          await ctx.log('error', 'Error message');
        }),
        concurrency: 1,
      };
      jobs.set('test', jobDef as unknown as JobDefinition);

      await task.execute();

      // Verify logs were created
      expect(storage.createJobLog).toHaveBeenCalledWith(
        expect.objectContaining({
          jobId,
          level: 'info',
          message: 'Test message',
        })
      );
      expect(storage.createJobLog).toHaveBeenCalledWith(
        expect.objectContaining({
          jobId,
          level: 'warn',
          message: 'Warning message',
        })
      );
      expect(storage.createJobLog).toHaveBeenCalledWith(
        expect.objectContaining({
          jobId,
          level: 'error',
          message: 'Error message',
        })
      );
    });

    it('should handle failed lock acquisition', async () => {
      await storage.createJob({
        type: 'test',
        status: 'pending',
        data: { test: true },
        attempts: 0,
        maxRetries: 3,
        failCount: 0,
        priority: 1,
        backoffStrategy: 'exponential',
        failReason: undefined,
        runAt: null,
      });

      // Mock updateJob to fail when trying to acquire lock
      storage.updateJob = vi.fn().mockRejectedValueOnce(new Error('Lock acquisition failed'));

      const jobDef = {
        handle: vi.fn().mockResolvedValue(undefined),
        concurrency: 1,
      };
      jobs.set('test', jobDef as unknown as JobDefinition);

      await task.execute();

      // Job should not be processed due to lock acquisition failure
      expect(jobDef.handle).not.toHaveBeenCalled();
      expect(storage.updateJobRun).not.toHaveBeenCalled();
    });
  });

  describe('shutdown', () => {
    it('should signal shutdown to running jobs', async () => {
      // Create a long running job that checks for shutdown
      const jobDef = {
        handle: vi.fn().mockImplementation(async (data, ctx) => {
          // Run a loop that checks for shutdown
          for (let i = 0; i < 10; i++) {
            if (ctx.isShuttingDown) {
              return; // Exit early if shutdown detected
            }
            await new Promise((resolve) => setImmediate(resolve));
          }
        }),
        schema: z.object({}),
      };
      jobs.set('test', jobDef as unknown as JobDefinition);

      // Create a job
      const jobId = await storage.createJob({
        type: 'test',
        status: 'pending',
        data: {},
        attempts: 0,
        maxRetries: 1,
        failCount: 0,
        priority: 1,
        backoffStrategy: 'exponential',
        failReason: undefined,
        runAt: null,
      });

      // Mock storage methods to track job state
      const job = await storage.getJob(jobId);
      storage.getJob = vi.fn().mockResolvedValue(job);
      storage.acquireLock = vi.fn().mockResolvedValue(true);

      // Start executing jobs
      const executePromise = task.execute();

      // Wait for job to start
      await vi.runOnlyPendingTimersAsync();

      // Signal shutdown
      await task.stop();

      // Complete execution
      await vi.runAllTimersAsync();
      await executePromise;

      // Job should have detected shutdown and exited early
      expect(jobDef.handle).toHaveBeenCalledTimes(1);
      const arr = jobDef.handle.mock.calls[0];
      expect(arr[1].isShuttingDown).toBe(true);
    });

    it('should not schedule new jobs when shutting down', async () => {
      // Signal shutdown first
      await task.stop();

      // Create a job that checks shutdown state
      const jobDef = {
        handle: vi.fn().mockImplementation(async (data, ctx) => {
          expect(ctx.isShuttingDown).toBe(true);
        }),
        schema: z.object({}),
      };
      jobs.set('test', jobDef as unknown as JobDefinition);

      // Create a job and mock storage
      const jobId = await storage.createJob({
        type: 'test',
        status: 'pending',
        data: {},
        attempts: 0,
        maxRetries: 3,
        failCount: 0,
        priority: 1,
        backoffStrategy: 'exponential',
        failReason: undefined,
        runAt: null,
      });

      // Mock storage methods
      const job = await storage.getJob(jobId);
      storage.getJob = vi.fn().mockResolvedValue(job);
      storage.acquireLock = vi.fn().mockResolvedValue(true);

      // Execute jobs
      await task.execute();
      await vi.runAllTimersAsync();

      // Job should have seen shutdown state
      expect(jobDef.handle).not.toHaveBeenCalled();
    });

    it('should maintain shutdown state across multiple executions', async () => {
      // Create a job that checks shutdown state
      const jobDef = {
        handle: vi.fn().mockImplementation(async (data, ctx) => {
          await new Promise((resolve) => setTimeout(resolve, 1000));
          expect(ctx.isShuttingDown).toBe(true);
          await new Promise((resolve) => setTimeout(resolve, 1000));
        }),
        schema: z.object({}),
      };
      jobs.set('test', jobDef as unknown as JobDefinition);

      // Create multiple jobs and mock storage
      await Promise.all([
        storage.createJob({
          type: 'test',
          status: 'pending',
          data: {},
          attempts: 0,
          maxRetries: 1,
          failCount: 0,
          priority: 1,
          backoffStrategy: 'exponential',
          failReason: undefined,
          runAt: null,
        }),
        storage.createJob({
          type: 'test',
          status: 'pending',
          data: {},
          attempts: 0,
          maxRetries: 1,
          failCount: 0,
          priority: 1,
          backoffStrategy: 'exponential',
          failReason: undefined,
          runAt: null,
        }),
      ]);

      // Start executing jobs
      const executePromise = task.execute();

      // Wait for jobs to start and reach first timeout
      await vi.advanceTimersByTimeAsync(100);
      await vi.advanceTimersByTimeAsync(400);

      // Signal shutdown
      await task.stop();

      // Advance time to complete both timeouts
      await vi.advanceTimersByTimeAsync(3000);

      // Wait for job execution to complete
      await executePromise;

      // All jobs should have seen shutdown state
      expect(jobDef.handle).toHaveBeenCalledTimes(2);

      const jobList = await storage.listJobs();
      jobList.forEach((job) => {
        expect(job.status).toBe('completed');
      });
    });
  });
});
