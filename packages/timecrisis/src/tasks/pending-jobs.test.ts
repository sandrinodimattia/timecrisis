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

  beforeEach(() => {
    storage = new MockJobStorage();
    jobs = new Map();
    executeForkMode = vi.fn();
    touchJob = vi.fn();

    task = new PendingJobsTask(storage, jobs, executeForkMode, touchJob, new EmptyLogger(), {
      maxConcurrentJobs: 20,
      lockLifetime: 5000,
    });
  });

  afterEach(() => {
    storage.cleanup();
    vi.clearAllMocks();
  });

  describe('execute', () => {
    it('should skip execution if already executing', async () => {
      // First execution
      const listJobsPromise = Promise.resolve([]);
      vi.spyOn(storage, 'listJobs').mockReturnValue(listJobsPromise);
      const execution1 = task.execute();

      // Second execution while first is still running
      const execution2 = task.execute();
      await Promise.all([execution1, execution2]);

      expect(storage.listJobs).toHaveBeenCalledTimes(1);
    });

    it('should process pending jobs up to concurrency limit', async () => {
      const job1 = await storage.createJob({
        type: 'test',
        status: 'pending',
        data: {
          job: 1,
        },
        attempts: 0,
        maxRetries: 3,
        failCount: 0,
        priority: 1,
        backoffStrategy: 'exponential',
        failReason: undefined,
        runAt: null,
      });
      const job2 = await storage.createJob({
        type: 'test',
        status: 'pending',
        data: {
          job: 2,
        },
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

      expect(storage.listJobs).toHaveBeenCalledWith({
        limit: 20,
        status: ['pending'],
        runAtBefore: expect.any(Date),
      });

      // First, it should acquire the lock
      expect(storage.updateJob).toHaveBeenNthCalledWith(1, job1, {
        lockedAt: expect.any(Date),
      });

      // Then, it should update the job status and attempts
      expect(storage.updateJob).toHaveBeenNthCalledWith(2, job1, {
        status: 'running',
        attempts: 1,
      });

      expect(storage.createJobRun).toHaveBeenCalledWith({
        jobId: job1,
        status: 'running',
        attempt: 1,
        startedAt: expect.any(Date),
      });
      expect(jobDef.handle).toHaveBeenCalledWith({ job: 1 }, expect.any(Object));

      expect(jobDef.handle).not.toHaveBeenCalledWith({ job: 2 }, expect.any(Object));

      const pendingJobs = await storage.listJobs({
        limit: 20,
        status: ['pending'],
      });
      expect(pendingJobs).toHaveLength(1);
      expect(pendingJobs[0].id).toBe(job2);
    });

    it('should handle invalid job types', async () => {
      const invalidJob: Job = {
        id: '1',
        type: 'invalid',
        status: 'pending',
        data: {},
        attempts: 0,
        maxRetries: 3,
        failCount: 0,
        priority: 1,
        backoffStrategy: 'exponential',
        failReason: undefined,
        runAt: null,
        createdAt: new Date(),
        updatedAt: new Date(),
      };

      vi.spyOn(storage, 'listJobs').mockResolvedValue([invalidJob]);

      await task.execute();

      expect(storage.createDeadLetterJob).toHaveBeenCalledWith(
        expect.objectContaining({
          jobId: '1',
          jobType: 'invalid',
          reason: 'Unknown job type: invalid',
        })
      );

      expect(storage.updateJob).toHaveBeenCalledWith(
        '1',
        expect.objectContaining({
          status: 'failed',
          failReason: 'Unknown job type: invalid',
        })
      );
    });

    it('should respect job lock', async () => {
      await storage.createJob({
        type: 'test',
        status: 'pending',
        data: {},
        attempts: 0,
        maxRetries: 3,
        failCount: 0,
        lockedAt: new Date(),
        priority: 1,
        backoffStrategy: 'exponential',
        failReason: undefined,
        runAt: null,
      });

      const jobDef = {
        handle: vi.fn().mockResolvedValue(undefined),
      };
      jobs.set('test', jobDef as unknown as JobDefinition);

      await task.execute();

      expect(jobDef.handle).not.toHaveBeenCalled();
    });

    it('should handle job success', async () => {
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

      const jobDef = {
        handle: vi.fn().mockResolvedValue(undefined),
      };
      jobs.set('test', jobDef as unknown as JobDefinition);

      await task.execute();

      const jobRuns = await storage.listJobRuns(jobId);
      expect(jobRuns).toHaveLength(1);
      expect(jobRuns[0].status).toBe('completed');

      expect(storage.updateJob).toHaveBeenCalledWith(
        jobId,
        expect.objectContaining({
          lockedAt: expect.any(Date),
        })
      );
      expect(storage.updateJob).toHaveBeenCalledWith(
        jobId,
        expect.objectContaining({
          status: 'completed',
        })
      );
      expect(storage.createJobRun).toHaveBeenCalledWith(
        expect.objectContaining({
          jobId: jobId,
          status: 'running',
        })
      );
      expect(storage.updateJobRun).toHaveBeenCalledWith(
        jobRuns[0].id,
        expect.objectContaining({
          status: 'completed',
        })
      );
    });

    it('should handle job failure with retry', async () => {
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

      const error = new Error('Test error');
      const jobDef = {
        handle: vi.fn().mockRejectedValue(error),
      };
      jobs.set('test', jobDef as unknown as JobDefinition);

      await task.execute();

      const jobRuns = await storage.listJobRuns(jobId);
      expect(jobRuns).toHaveLength(1);

      // eslint-disable-next-line @typescript-eslint/no-explicit-any
      const updateJobCalls = (storage.updateJob as any).mock.calls;
      expect(updateJobCalls.length).toBe(3);

      // First call should be to acquire the lock
      expect(updateJobCalls[0]).toEqual([
        jobId,
        {
          lockedAt: expect.any(Date),
        },
      ]);

      expect(updateJobCalls[1]).toEqual([
        jobId,
        {
          status: 'running',
          attempts: 1,
        },
      ]);

      // Second call should be to schedule the retry
      expect(updateJobCalls[2]).toEqual([
        jobId,
        {
          status: 'pending',
          failReason: 'Test error',
          failCount: 1,
          lockedAt: null,
          executionDuration: expect.any(Number),
          runAt: expect.any(Date),
        },
      ]);

      expect(storage.updateJobRun).toHaveBeenCalledWith(
        jobRuns[0].id,
        expect.objectContaining({
          status: 'failed',
          error: 'Test error',
        })
      );
    });

    it('should handle job failure with dead letter after max retries', async () => {
      const jobId = await storage.createJob({
        type: 'test',
        status: 'pending',
        data: {},
        attempts: 3,
        maxRetries: 3,
        failCount: 2,
        priority: 1,
        backoffStrategy: 'exponential',
        failReason: undefined,
        runAt: null,
      });

      const error = new Error('Test error');
      const jobDef = {
        handle: vi.fn().mockRejectedValue(error),
      };
      jobs.set('test', jobDef as unknown as JobDefinition);

      await task.execute();

      const jobRuns = await storage.listJobRuns(jobId);
      expect(jobRuns).toHaveLength(1);

      expect(storage.createDeadLetterJob).toHaveBeenCalledWith(
        expect.objectContaining({
          jobId: jobId,
          jobType: 'test',
          reason: 'Test error',
        })
      );

      expect(storage.updateJobRun).toHaveBeenCalledWith(
        jobRuns[0].id,
        expect.objectContaining({
          status: 'failed',
          error: 'Test error',
        })
      );

      expect(storage.updateJob).toHaveBeenCalledWith(
        jobId,
        expect.objectContaining({
          status: 'failed',
          failCount: 3,
        })
      );
    });

    it('should handle exponential backoff', async () => {
      const jobId = await storage.createJob({
        type: 'test',
        status: 'pending',
        data: {},
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
      };
      jobs.set('test', jobDef as unknown as JobDefinition);

      await task.execute();

      const jobRuns = await storage.listJobRuns(jobId);
      expect(jobRuns).toHaveLength(1);

      // eslint-disable-next-line @typescript-eslint/no-explicit-any
      const updateJobCalls = (storage.updateJob as any).mock.calls;

      // First call should be to acquire the lock
      expect(updateJobCalls[1]).toEqual([
        jobId,
        {
          status: 'running',
          attempts: 2,
        },
      ]);

      // Second call should be to schedule the retry with exponential backoff
      expect(updateJobCalls[2]).toEqual([
        jobId,
        {
          status: 'pending',
          failReason: 'Test error',
          failCount: 2,
          lockedAt: null,
          executionDuration: expect.any(Number),
          runAt: expect.any(Date),
        },
      ]);

      expect(storage.updateJobRun).toHaveBeenCalledWith(
        jobRuns[0].id,
        expect.objectContaining({
          status: 'failed',
          error: 'Test error',
        })
      );
    });

    it('should handle fork mode execution', async () => {
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
      const job = await storage.getJob(jobId);
      expect(job).toBeDefined();

      const jobDef = {
        handle: vi.fn(),
        forkMode: true,
        forkHelperPath: '/path/to/helper',
      };
      jobs.set('test', jobDef as unknown as JobDefinition);

      await task.execute();

      expect(executeForkMode).toHaveBeenCalledWith(jobDef, job, expect.any(Object));
      expect(jobDef.handle).not.toHaveBeenCalled();
    });

    it('should not process jobs scheduled for the future', async () => {
      const futureDate = new Date();
      futureDate.setHours(futureDate.getHours() + 1);

      await storage.createJob({
        type: 'test',
        status: 'pending',
        data: {},
        runAt: futureDate,
        priority: 0,
        attempts: 0,
        maxRetries: 3,
        backoffStrategy: 'exponential',
      });

      jobs.set('test', {
        type: 'test',
        handle: vi.fn(),
        schema: z.object({}),
      });

      await task.execute();

      expect(storage.listJobs).toHaveBeenCalledWith(
        expect.objectContaining({
          status: ['pending'],
          runAtBefore: expect.any(Date),
        })
      );
    });

    it('should process jobs scheduled for the past', async () => {
      const pastDate = new Date();
      pastDate.setHours(pastDate.getHours() - 1);

      await storage.createJob({
        type: 'test',
        status: 'pending',
        data: {},
        runAt: pastDate,
        priority: 0,
        attempts: 0,
        maxRetries: 3,
        backoffStrategy: 'exponential',
      });

      const handler = vi.fn();
      jobs.set('test', {
        type: 'test',
        handle: handler,
        schema: z.object({}),
      });

      await task.execute();

      expect(storage.listJobs).toHaveBeenCalledWith(
        expect.objectContaining({
          limit: 20,
          status: ['pending'],
          runAtBefore: expect.any(Date),
        })
      );
      expect(handler).toHaveBeenCalled();
    });

    it('should process jobs with no runAt date', async () => {
      await storage.createJob({
        type: 'test',
        status: 'pending',
        data: {},
        priority: 0,
        attempts: 0,
        maxRetries: 3,
        backoffStrategy: 'exponential',
      });

      const handler = vi.fn();
      jobs.set('test', {
        type: 'test',
        handle: handler,
        schema: z.object({}),
      });

      await task.execute();

      expect(storage.listJobs).toHaveBeenCalledWith(
        expect.objectContaining({
          status: ['pending'],
          runAtBefore: expect.any(Date),
        })
      );
      expect(handler).toHaveBeenCalled();
    });
  });
});
