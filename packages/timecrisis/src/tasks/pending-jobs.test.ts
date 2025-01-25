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
      node: 'test-worker',
      maxConcurrentJobs: 20,
      jobLockTTL: 5000,
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

      storage.acquireConcurrencySlot = vi.fn().mockImplementation(async () => {
        if (runningJobs >= maxConcurrency) {
          return false;
        }
        runningJobs++;
        return true;
      });

      storage.releaseConcurrencySlot = vi.fn().mockImplementation(async () => {
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

      expect(storage.acquireConcurrencySlot).toHaveBeenCalledWith('test', maxConcurrency);
      expect(jobDef.handle).toHaveBeenCalledTimes(maxConcurrency);
      expect(runningJobs).toBe(0); // All jobs should be finished
    });

    it('should handle job locking correctly', async () => {
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
        lockedAt: new Date(Date.now() - 1000),
        lockedBy: 'other-worker',
      });

      const jobDef = {
        handle: vi.fn().mockResolvedValue(undefined),
        concurrency: 1,
      };
      jobs.set('test', jobDef as unknown as JobDefinition);

      await task.execute();

      expect(jobDef.handle).not.toHaveBeenCalled();

      await storage.updateJob(jobId, {
        lockedAt: new Date(Date.now() - 6000),
      });

      await task.execute();

      expect(jobDef.handle).toHaveBeenCalled();
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
        handle: vi.fn().mockImplementation(async (_, ctx) => {
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

    it('should handle lock expiration edge cases', async () => {
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
        lockedAt: new Date(now.getTime() - 5001), // Just over the 5000ms lock lifetime
        lockedBy: 'other-worker',
      });

      const jobDef = {
        handle: vi.fn().mockResolvedValue(undefined),
        concurrency: 1,
      };
      jobs.set('test', jobDef as unknown as JobDefinition);

      await task.execute();

      // Job should be processed because lock expired
      expect(jobDef.handle).toHaveBeenCalled();
      expect(storage.updateJob).toHaveBeenCalledWith(
        jobId,
        expect.objectContaining({
          lockedAt: expect.any(Date),
          lockedBy: 'test-worker',
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
});
