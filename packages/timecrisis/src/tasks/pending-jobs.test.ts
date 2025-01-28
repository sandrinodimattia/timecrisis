import { z } from 'zod';
import { describe, it, expect, vi, beforeEach, afterEach } from 'vitest';

import {
  createJobLock,
  defaultJob,
  defaultJobDefinition,
  defaultJobRegistrations,
  defaultValues,
  expectJobLocked,
  failedJobDefinition,
  longRunningJobDefinition,
  now,
  prepareEnvironment,
  resetEnvironment,
} from '../test-helpers/defaults.js';

import { EmptyLogger } from '../logger/index.js';
import { Job } from '../storage/schemas/index.js';
import { PendingJobsTask } from './pending-jobs.js';
import { JobDefinition } from '../scheduler/types.js';
import { MockJobStorage } from '../storage/mock/index.js';
import { JobStateMachine } from '../state-machine/index.js';
import { DistributedLock } from '../concurrency/distributed-lock.js';

describe('PendingJobsTask', () => {
  let storage: MockJobStorage;
  let task: PendingJobsTask;
  let stateMachine: JobStateMachine;
  let executeForkMode: (jobDef: JobDefinition, job: Job, ctx: unknown) => Promise<void>;

  beforeEach(() => {
    prepareEnvironment();

    executeForkMode = vi.fn();

    storage = new MockJobStorage();
    stateMachine = new JobStateMachine({
      logger: new EmptyLogger(),
      storage,
      jobs: defaultJobRegistrations,
    });
    vi.spyOn(stateMachine, 'enqueue');

    task = new PendingJobsTask({
      logger: new EmptyLogger(),
      storage,
      jobs: defaultJobRegistrations,
      stateMachine,
      executeForkMode,
      distributedLock: new DistributedLock({
        storage,
        worker: defaultValues.workerName,
        lockTTL: defaultValues.distributedLockTTL,
      }),
      worker: defaultValues.workerName,
      maxConcurrentJobs: defaultValues.maxConcurrentJobs,
      jobLockTTL: defaultValues.jobLockTTL,
      pollInterval: defaultValues.pollInterval,
    });
  });

  afterEach(() => {
    storage.cleanup();
    resetEnvironment();
  });

  describe('execute', () => {
    it('should process pending jobs up to global concurrency limit', async () => {
      const jobPromises = Array.from({ length: 25 }, (_, i) =>
        storage.createJob({
          type: longRunningJobDefinition.type,
          status: 'pending',
          data: { job: i },
          maxRetries: 3,
          failCount: 0,
          priority: 1,
          backoffStrategy: 'exponential',
          failReason: undefined,
          runAt: null,
        })
      );
      await Promise.all(jobPromises);

      // Start the execution, but advance until it is running.
      let taskPromise = task.execute();
      await vi.advanceTimersByTimeAsync(defaultValues.pollInterval);

      // 20 tasks should be running.
      expect(longRunningJobDefinition.handle).toHaveBeenCalledTimes(20);
      expect(task.getRunningCount()).toBe(20);

      // Complete the first run.
      await vi.advanceTimersByTimeAsync(defaultValues.longRunningJobDuration);
      await taskPromise;

      // Start a second run.
      taskPromise = task.execute();
      await vi.advanceTimersByTimeAsync(defaultValues.pollInterval);

      // Now 25 will have started.
      expect(longRunningJobDefinition.handle).toHaveBeenCalledTimes(25);
      expect(task.getRunningCount()).toBe(5);
      await vi.advanceTimersByTimeAsync(defaultValues.longRunningJobDuration);

      // Done.
      await taskPromise;
      expect(task.getRunningCount()).toBe(0);
    });

    it('should respect job-type concurrency limits', async () => {
      const maxConcurrency = 3;

      // Create test jobs
      await Promise.all(
        Array.from({ length: 10 }, (_, i) =>
          storage.createJob({
            type: longRunningJobDefinition.type + '-max',
            status: 'pending',
            data: { job: i },
            maxRetries: 3,
            failCount: 0,
            priority: 1,
            backoffStrategy: 'exponential',
            failReason: undefined,
            runAt: null,
          })
        )
      );

      defaultJobRegistrations.set(longRunningJobDefinition.type + '-max', {
        ...longRunningJobDefinition,
        concurrency: maxConcurrency,
      });

      const executePromise = task.execute();
      await vi.advanceTimersByTimeAsync(100);

      let jobList = await storage.listJobs({ status: ['running'] });
      expect(jobList).toHaveLength(3);

      await vi.advanceTimersByTimeAsync(defaultValues.longRunningJobDuration);
      const jobListPending = await storage.listJobs({ status: ['pending'] });
      expect(jobListPending).toHaveLength(7);

      let completed = await storage.listJobs({ status: ['completed'] });
      expect(completed).toHaveLength(3);

      expect(storage.acquireJobTypeSlot).toHaveBeenCalledWith(
        longRunningJobDefinition.type + '-max',
        'test-worker',
        maxConcurrency
      );
      expect(longRunningJobDefinition.handle).toHaveBeenCalledTimes(maxConcurrency);

      // Advance all timers to complete any pending promises
      await executePromise;
    });

    it('should handle job locking correctly', async () => {
      // Create a job
      const jobId = await storage.createJob({
        type: longRunningJobDefinition.type,
        status: 'pending',
        data: {},
        maxRetries: 3,
        failCount: 0,
        priority: 1,
        backoffStrategy: 'exponential',
      });

      // Start executing jobs
      const executePromise = task.execute();

      // Advance time to start execution
      await vi.advanceTimersByTimeAsync(defaultValues.pollInterval);

      // Job should be locked.
      await expectJobLocked(storage, jobId);

      // Wait for job to start
      await vi.runOnlyPendingTimersAsync();

      // Complete execution
      await vi.runAllTimersAsync();
      await executePromise;

      // Job should have been processed
      expect(longRunningJobDefinition.handle).toHaveBeenCalled();

      // Verify job state
      const job = await storage.getJob(jobId);
      expect(job?.status).toBe('completed');
    });

    it('should handle lock expiration edge cases', async () => {
      // Create a job
      const jobId = await storage.createJob({
        type: longRunningJobDefinition.type,
        status: 'pending',
        data: {},
        maxRetries: 3,
        failCount: 0,
        priority: 1,
        backoffStrategy: 'exponential',
      });

      await createJobLock(storage, jobId, 'other-worker', 600);

      // Start executing jobs
      let executePromise = task.execute();

      await vi.advanceTimersByTimeAsync(500);
      expect(longRunningJobDefinition.handle).not.toHaveBeenCalled();
      await vi.advanceTimersByTimeAsync(100);

      await executePromise;
      executePromise = task.execute();

      await vi.advanceTimersByTimeAsync(1500);
      expect(longRunningJobDefinition.handle).toHaveBeenCalled();

      await vi.advanceTimersByTimeAsync(defaultValues.longRunningJobDuration);
      await executePromise;

      // Verify job state
      const job = await storage.getJob(jobId);
      expect(job?.status).toBe('completed');
    });

    it('should handle fork mode execution', async () => {
      const jobId = await storage.createJob({
        type: 'test-fork',
        status: 'pending',
        data: {},
        maxRetries: 3,
        failCount: 0,
        priority: 1,
        backoffStrategy: 'exponential',
      });

      const jobDef = {
        handle: vi.fn(),
        forkMode: true,
        concurrency: 1,
      };
      defaultJobRegistrations.set('test-fork', jobDef as unknown as JobDefinition);

      const executePromise = await task.execute();

      await vi.advanceTimersByTimeAsync(100);
      await executePromise;

      expect(executeForkMode).toHaveBeenCalledWith(
        jobDef,
        expect.objectContaining({ id: jobId }),
        expect.any(Object)
      );
    });

    it('should handle exponential backoff on failure', async () => {
      const jobId = await storage.createJob({
        type: failedJobDefinition.type,
        status: 'pending',
        data: {
          test: true,
        },
        maxRetries: 3,
        failCount: 0,
        priority: 1,
        backoffStrategy: 'exponential',
      });

      await task.execute();

      const updatedJob = await storage.getJob(jobId);
      expect(updatedJob!.status).toBe('pending');
      expect(updatedJob!.runAt).toBeInstanceOf(Date);
      expect(updatedJob!.failCount).toBe(1);

      const expectedDelay = 10000;
      const actualDelay = updatedJob!.runAt!.getTime() - now.getTime();
      expect(actualDelay).toBe(expectedDelay);
    });

    it('should handle exponential backoff on failure accounting for previous run', async () => {
      const jobId = await storage.createJob({
        type: failedJobDefinition.type,
        status: 'pending',
        data: {
          test: true,
        },
        maxRetries: 3,
        failCount: 1,
        priority: 1,
        backoffStrategy: 'exponential',
      });

      await storage.createJobRun({
        jobId,
        attempt: 1,
        status: 'failed',
        startedAt: new Date(),
        executionDuration: 100,
      });

      await task.execute();

      const updatedJob = await storage.getJob(jobId);
      expect(updatedJob!.status).toBe('pending');
      expect(updatedJob!.runAt).toBeInstanceOf(Date);
      expect(updatedJob!.failCount).toBe(2);

      const expectedDelay = 20000;
      const actualDelay = updatedJob!.runAt!.getTime() - now.getTime();
      expect(actualDelay).toBe(expectedDelay);
    });

    it('should move job to dead letter queue after max retries', async () => {
      const jobId = await storage.createJob({
        type: failedJobDefinition.type,
        status: 'pending',
        data: {
          test: true,
        },
        maxRetries: 2,
        failCount: 2,
        priority: 1,
        backoffStrategy: 'exponential',
      });

      await storage.createJobRun({
        jobId,
        attempt: 1,
        status: 'failed',
        startedAt: new Date(),
        executionDuration: 100,
      });

      await storage.createJobRun({
        jobId,
        attempt: 2,
        status: 'failed',
        startedAt: new Date(),
        executionDuration: 100,
      });

      await task.execute();

      const updatedJob = await storage.getJob(jobId);
      expect(updatedJob!.status).toBe('failed');
      expect(updatedJob!.failCount).toBe(3);
      expect(updatedJob!.failReason).toBe('Test error');

      expect(storage.createDeadLetterJob).toHaveBeenCalledWith(
        expect.objectContaining({
          jobId: jobId,
          jobType: failedJobDefinition.type,
          failReason: 'Test error',
        })
      );
    });

    it('should update progress correctly', async () => {
      const jobId = await storage.createJob({
        ...defaultJob,
      });

      const jobDef = {
        handle: vi.fn().mockImplementation(async (_, ctx) => {
          await ctx.updateProgress(50);
          await ctx.updateProgress(100);
        }),
        concurrency: 1,
        schema: z.object({}),
      };
      defaultJobRegistrations.set('test', jobDef as unknown as JobDefinition);

      await task.execute();

      const jobRuns = await storage.listJobRuns(jobId);
      expect(jobRuns).toHaveLength(1);

      expect(storage.updateJobRun).toHaveBeenCalledWith(
        jobRuns[0].id,
        expect.objectContaining({ progress: 50 })
      );
      expect(storage.updateJobRun).toHaveBeenCalledWith(
        jobRuns[0].id,
        expect.objectContaining({ progress: 100 })
      );
    });

    it('should handle invalid job types', async () => {
      await storage.createJob({
        ...defaultJob,
        type: 'invalid-type',
      });

      await task.execute();

      expect(storage.updateJob).not.toHaveBeenCalled();
    });

    it('should track job execution duration', async () => {
      const jobId = await storage.createJob({
        ...defaultJob,
        type: longRunningJobDefinition.type,
      });

      const taskPromise = task.execute();
      await vi.advanceTimersByTimeAsync(2000);
      await taskPromise;

      const runs = await storage.listJobRuns(jobId);
      expect(runs[0].executionDuration).toBe(2000);
      expect(storage.updateJob).toHaveBeenCalledWith(
        jobId,
        expect.objectContaining({
          status: 'completed',
        })
      );
      expect(storage.updateJobRun).toHaveBeenCalledWith(
        runs[0].id,
        expect.objectContaining({
          status: 'completed',
          executionDuration: 2000,
        })
      );
    });

    it('should update job run status correctly', async () => {
      const jobId = await storage.createJob({
        ...defaultJob,
      });

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
        ...defaultJob,
        type: failedJobDefinition.type,
        backoffStrategy: 'linear',
      });

      await task.execute();

      const updatedJob = await storage.getJob(jobId);
      expect(updatedJob!.status).toBe('pending');

      // Linear  should always be 10 seconds
      const expectedDelay = 10000;
      const actualDelay = updatedJob!.runAt!.getTime() - now.getTime();
      expect(actualDelay).toBe(expectedDelay);
    });

    it('should log job context messages', async () => {
      const jobId = await storage.createJob({
        ...defaultJob,
        type: 'test-logger',
      });

      const jobDef = {
        handle: vi.fn().mockImplementation(async (data, ctx) => {
          await ctx.log('info', 'Test message');
          await ctx.log('warn', 'Warning message');
          await ctx.log('error', 'Error message');
        }),
        concurrency: 1,
      };
      defaultJobRegistrations.set('test-logger', jobDef as unknown as JobDefinition);

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
        ...defaultJob,
      });

      // Mock updateJob to fail when trying to acquire lock
      storage.acquireLock = vi.fn().mockRejectedValueOnce(new Error('Lock acquisition failed'));

      await task.execute();

      // Job should not be processed due to lock acquisition failure
      expect(defaultJobDefinition.handle).not.toHaveBeenCalled();
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
      defaultJobRegistrations.set('test-shutdown', jobDef as unknown as JobDefinition);

      // Create a job
      await storage.createJob({
        ...defaultJob,
        type: 'test-shutdown',
      });

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
      defaultJobRegistrations.set('test-shutdown-failure', jobDef as unknown as JobDefinition);

      // Create a job and mock storage
      await storage.createJob({
        ...defaultJob,
        type: 'test-shutdown-failure',
      });

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
          await new Promise((resolve) => setTimeout(resolve, defaultValues.longRunningJobDuration));
          expect(ctx.isShuttingDown).toBe(true);
          await new Promise((resolve) => setTimeout(resolve, defaultValues.longRunningJobDuration));
        }),
        schema: z.object({}),
      };
      defaultJobRegistrations.set('test-shutdown-expect', jobDef as unknown as JobDefinition);

      // Create multiple jobs and mock storage
      await Promise.all([
        storage.createJob({
          ...defaultJob,
          type: 'test-shutdown-expect',
        }),
        storage.createJob({
          ...defaultJob,
          type: 'test-shutdown-expect',
        }),
      ]);

      // Start executing jobs
      const executePromise = task.execute();

      // Wait for jobs to start and reach first timeout
      await vi.advanceTimersByTimeAsync(defaultValues.longRunningJobDuration - 100);

      // Signal shutdown
      await task.stop();

      // Advance time to complete both timeouts
      await vi.advanceTimersByTimeAsync(defaultValues.longRunningJobDuration + 100);

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
