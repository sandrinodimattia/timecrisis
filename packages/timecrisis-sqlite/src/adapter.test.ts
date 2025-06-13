import { randomUUID } from 'crypto';
import { describe, it, expect, beforeEach, afterEach, vi } from 'vitest';
import { JobNotFoundError, ScheduledJobNotFoundError } from '@timecrisis/timecrisis';

import {
  createStorage,
  defaultJob,
  defaultValues,
  now,
  prepareEnvironment,
  resetEnvironment,
} from './test-helpers/defaults.js';
import { SQLiteJobStorage } from './adapter.js';

describe('SQLiteJobStorage', () => {
  let storage: SQLiteJobStorage;

  beforeEach(async () => {
    prepareEnvironment();
    const connection = await createStorage();
    storage = connection.storage;
  });

  afterEach(async () => {
    await storage.close();
    resetEnvironment();
  });

  describe('Job Management', () => {
    it('should create and retrieve a job', async () => {
      const jobData = {
        type: 'test-job',
        entityId: 'user-123',
        data: { test: 'data' },
        priority: 10,
        status: 'pending' as const,
        maxRetries: 3,
        backoffStrategy: 'exponential' as const,
        failReason: 'test failure',
        failCount: 1,
        expiresAt: new Date(now.getTime() + 3600000),
        startedAt: now,
        runAt: new Date(now.getTime() + 1800000),
        finishedAt: new Date(now.getTime() + 2000000),
      };

      const jobId = await storage.createJob(jobData);
      expect(jobId).toBeDefined();

      const job = await storage.getJob(jobId);
      expect(job).toBeDefined();

      // Verify all fields
      expect(job?.type).toBe(jobData.type);
      expect(job?.entityId).toBe(jobData.entityId);
      expect(job?.data).toEqual(jobData.data);
      expect(job?.priority).toBe(jobData.priority);
      expect(job?.status).toBe(jobData.status);
      expect(job?.maxRetries).toBe(jobData.maxRetries);
      expect(job?.backoffStrategy).toBe(jobData.backoffStrategy);
      expect(job?.failReason).toBe(jobData.failReason);
      expect(job?.failCount).toBe(jobData.failCount);
      expect(job?.expiresAt?.getTime()).toBe(jobData.expiresAt.getTime());
      expect(job?.startedAt?.getTime()).toBe(jobData.startedAt.getTime());
      expect(job?.runAt?.getTime()).toBe(jobData.runAt.getTime());
      expect(job?.finishedAt?.getTime()).toBe(jobData.finishedAt.getTime());

      // Verify system-generated fields
      expect(job?.id).toBe(jobId);
      expect(job?.createdAt).toBeInstanceOf(Date);
      expect(job?.updatedAt).toBeInstanceOf(Date);
    });

    it('should update a job', async () => {
      // First create a job
      const jobId = await storage.createJob({
        type: 'test-job',
        entityId: 'user-123',
        data: { test: 'data' },
        priority: 1,
        status: 'pending',
        maxRetries: 3,
        backoffStrategy: 'exponential',
        runAt: now,
      });

      // Update with all possible fields
      const updates = {
        entityId: 'user-456',
        data: { test: 'updated' },
        priority: 15,
        status: 'running' as const,
        maxRetries: 5,
        backoffStrategy: 'linear' as const,
        failReason: 'temporary failure',
        failCount: 1,
        expiresAt: new Date(now.getTime() + 7200000),
        startedAt: now,
        runAt: new Date(now.getTime() + 3600000),
        finishedAt: new Date(now.getTime() + 2500000),
      };

      await vi.advanceTimersByTimeAsync(1000);
      await storage.updateJob(jobId, updates);
      const job = await storage.getJob(jobId);

      // Verify all updated fields
      expect(job?.entityId).toBe(updates.entityId);
      expect(job?.data).toEqual(updates.data);
      expect(job?.priority).toBe(updates.priority);
      expect(job?.status).toBe(updates.status);
      expect(job?.maxRetries).toBe(updates.maxRetries);
      expect(job?.backoffStrategy).toBe(updates.backoffStrategy);
      expect(job?.failReason).toBe(updates.failReason);
      expect(job?.failCount).toBe(updates.failCount);
      expect(job?.expiresAt?.getTime()).toBe(updates.expiresAt.getTime());
      expect(job?.startedAt?.getTime()).toBe(updates.startedAt.getTime());
      expect(job?.runAt?.getTime()).toBe(updates.runAt.getTime());
      expect(job?.finishedAt?.getTime()).toBe(updates.finishedAt.getTime());

      // Verify system fields are maintained/updated
      expect(job?.id).toBe(jobId);
      expect(job?.type).toBe('test-job'); // type should not change
      expect(job?.createdAt).toBeInstanceOf(Date);
      expect(job?.updatedAt).toBeInstanceOf(Date);
      expect(job?.updatedAt.getTime()).toBeGreaterThan(job!.createdAt!.getTime());
    });

    it('should throw JobNotFoundError when updating non-existent job', async () => {
      await expect(
        storage.updateJob(randomUUID(), {
          status: 'running',
        })
      ).rejects.toThrow(JobNotFoundError);
    });

    it('should list jobs with filters', async () => {
      await Promise.all([
        storage.createJob({
          ...defaultJob,
          type: 'test-job-1',
          status: 'pending',
        }),
        storage.createJob({
          ...defaultJob,
          type: 'test-job-2',
          status: 'running',
          priority: 2,
        }),
      ]);

      const pendingJobs = await storage.listJobs({ status: ['pending'] });
      expect(pendingJobs).toHaveLength(1);
      expect(pendingJobs[0].type).toBe('test-job-1');

      const highPriorityJobs = await storage.listJobs({ type: 'test-job-2' });
      expect(highPriorityJobs).toHaveLength(1);
      expect(highPriorityJobs[0].priority).toBe(2);
    });

    it('should handle complex job filtering scenarios', async () => {
      await Promise.all([
        storage.createJob({
          type: 'type-1',
          status: 'pending',
          data: {},
          priority: 1,
          maxRetries: 3,
          backoffStrategy: 'exponential',
          runAt: new Date(now.getTime() + 1000),
          entityId: 'ref-1',
        }),
        storage.createJob({
          type: 'type-1',
          status: 'running',
          data: {},
          priority: 2,
          maxRetries: 3,
          backoffStrategy: 'exponential',
          runAt: new Date(now.getTime() + 2000),
          entityId: 'ref-2',
        }),
        storage.createJob({
          type: 'type-2',
          status: 'pending',
          data: {},
          priority: 3,
          maxRetries: 3,
          backoffStrategy: 'exponential',
          runAt: new Date(now.getTime() + 3000),
          entityId: 'ref-1',
        }),
      ]);

      // Test multiple status filters
      const pendingAndRunning = await storage.listJobs({ status: ['pending', 'running'] });
      expect(pendingAndRunning).toHaveLength(3);

      // Test type and entityId combination
      const type1Ref1Jobs = await storage.listJobs({
        type: 'type-1',
        entityId: 'ref-1',
      });
      expect(type1Ref1Jobs).toHaveLength(1);
      expect(type1Ref1Jobs[0].type).toBe('type-1');
      expect(type1Ref1Jobs[0].entityId).toBe('ref-1');

      // Test runAt filtering
      const futureRunAt = new Date(now.getTime() + 2500);
      const jobsBeforeTime = await storage.listJobs({
        runAtBefore: futureRunAt,
      });
      expect(jobsBeforeTime).toHaveLength(2);

      // Test priority ordering
      const orderedJobs = await storage.listJobs({});
      expect(orderedJobs).toHaveLength(3);
      expect(orderedJobs[0].priority).toBe(1);
      expect(orderedJobs[1].priority).toBe(2);
      expect(orderedJobs[2].priority).toBe(3);

      // Test limit
      const limitedJobs = await storage.listJobs({ limit: 2 });
      expect(limitedJobs).toHaveLength(2);
    });

    it('should handle null and undefined filter values correctly', async () => {
      await storage.createJob({
        ...defaultJob,
        type: 'test-job',
        status: 'pending',
        runAt: undefined,
      });

      const jobs = await storage.listJobs({
        type: 'test-job',
        runAtBefore: new Date(),
      });

      expect(jobs).toHaveLength(1);
      expect(jobs[0].runAt).toBeUndefined();
    });

    it('should handle concurrent job updates correctly', async () => {
      const jobId = await storage.createJob({
        ...defaultJob,
      });

      // Simulate concurrent updates
      await Promise.all([
        storage.updateJob(jobId, { status: 'running', failCount: 1 }),
        storage.updateJob(jobId, { status: 'running', failCount: 2 }),
      ]);

      const job = await storage.getJob(jobId);
      expect(job?.failCount).toBe(2);
    });

    it('should handle special characters and SQL injection attempts in filters', async () => {
      const jobId = await storage.createJob({
        ...defaultJob,
        entityId: "ref-1'; DROP TABLE jobs; --",
      });

      const jobs = await storage.listJobs({
        entityId: "ref-1'; DROP TABLE jobs; --",
      });
      expect(jobs).toHaveLength(1);
      expect(jobs[0].id).toBe(jobId);

      // Verify table wasn't dropped
      const allJobs = await storage.listJobs({});
      expect(allJobs).toHaveLength(1);
    });

    it('should handle null and undefined filter values correctly', async () => {
      await storage.createJob({
        ...defaultJob,
        runAt: undefined,
      });

      // Test with undefined values
      const jobsWithUndefinedFilters = await storage.listJobs({
        type: defaultJob.type,
        entityId: defaultJob.entityId!,
        runAtBefore: undefined,
      });
      expect(jobsWithUndefinedFilters).toHaveLength(1);
    });

    it('should handle multiple status values correctly', async () => {
      // Create jobs with different statuses
      await Promise.all([
        storage.createJob({
          ...defaultJob,
          status: 'pending',
        }),
        storage.createJob({
          ...defaultJob,
          status: 'running',
        }),
        storage.createJob({
          ...defaultJob,
          status: 'failed',
          backoffStrategy: 'exponential',
        }),
      ]);

      // Test single status
      const pendingJobs = await storage.listJobs({ status: ['pending'] });
      expect(pendingJobs).toHaveLength(1);
      expect(pendingJobs[0].status).toBe('pending');

      // Test multiple statuses
      const activeJobs = await storage.listJobs({ status: ['pending', 'running'] });
      expect(activeJobs).toHaveLength(2);
      expect(activeJobs.map((j) => j.status).sort()).toEqual(['pending', 'running']);

      // Test all valid statuses
      const allJobs = await storage.listJobs({
        status: ['pending', 'running', 'completed', 'failed', 'scheduled'],
      });
      expect(allJobs).toHaveLength(3);
    });

    it('should handle limit parameter correctly', async () => {
      // Create multiple jobs
      await Promise.all(
        Array.from({ length: 5 }, (_, i) =>
          storage.createJob({
            type: 'test-job',
            status: 'pending',
            data: {},
            priority: i + 1,
            maxRetries: 3,
            backoffStrategy: 'exponential',
          })
        )
      );

      // Test with different limit values
      const twoJobs = await storage.listJobs({ limit: 2 });
      expect(twoJobs).toHaveLength(2);
      expect(twoJobs[0].priority).toBe(1); // Highest priority first
      expect(twoJobs[1].priority).toBe(2);

      const allJobs = await storage.listJobs();
      expect(allJobs).toHaveLength(5);

      const zeroJobs = await storage.listJobs({ limit: 1 });
      expect(zeroJobs).toHaveLength(1);
    });
  });

  describe('Job Runs', () => {
    let jobId: string;

    beforeEach(async () => {
      jobId = await storage.createJob({
        ...defaultJob,
      });
    });

    it('should create and retrieve a job run', async () => {
      // First create a job to associate the run with
      const jobId = await storage.createJob({
        ...defaultJob,
      });

      // Create run with all possible fields
      const runData = {
        jobId,
        status: 'running' as const,
        progress: 45,
        startedAt: now,
        executionDuration: 5000,
        finishedAt: new Date(now.getTime() + 1000), // 1 second later
        attempt: 3,
        error: 'Test error message',
        error_stack: 'Error: Test error message\n    at TestFunction (/test.ts:1:1)',
      };

      const runId = await storage.createJobRun(runData);
      const runs = await storage.listJobRuns(jobId);

      expect(runs).toHaveLength(1);
      const run = runs[0];

      // Verify all fields
      expect(run.id).toBe(runId);
      expect(run.jobId).toBe(runData.jobId);
      expect(run.status).toBe(runData.status);
      expect(run.executionDuration).toBe(runData.executionDuration);
      expect(run.progress).toBe(runData.progress);
      expect(run.startedAt.getTime()).toBe(runData.startedAt.getTime());
      expect(run.finishedAt?.getTime()).toBe(runData.finishedAt.getTime());
      expect(run.attempt).toBe(runData.attempt);
      expect(run.error).toBe(runData.error);
      expect(run.error_stack).toBe(runData.error_stack);

      // Verify individual retrieval also works
      const singleRun = await storage.getJobRun(runId);
      expect(singleRun).toBeDefined();
      expect(singleRun?.id).toBe(runId);
      expect(singleRun?.jobId).toBe(runData.jobId);
      expect(singleRun?.status).toBe(runData.status);
      expect(singleRun?.progress).toBe(runData.progress);
      expect(singleRun?.startedAt.getTime()).toBe(runData.startedAt.getTime());
      expect(singleRun?.finishedAt?.getTime()).toBe(runData.finishedAt.getTime());
      expect(singleRun?.attempt).toBe(runData.attempt);
      expect(singleRun?.error).toBe(runData.error);
      expect(singleRun?.error_stack).toBe(runData.error_stack);
    });

    it('should update job run correctly', async () => {
      // Create initial job and run
      const jobId = await storage.createJob({
        type: 'test-job',
        status: 'pending',
        data: { test: 'data' },
        runAt: now,
      });

      const runId = await storage.createJobRun({
        jobId,
        status: 'running',
        progress: 0,
        startedAt: now,
        attempt: 1,
      });

      // Update with all possible fields
      const updates = {
        status: 'failed' as const,
        progress: 75,
        executionDuration: 1000,
        startedAt: new Date(now.getTime() + 1000), // 1 second after start
        finishedAt: new Date(now.getTime() + 5000), // 5 seconds after start
        attempt: 2,
        error: 'Updated error message',
        error_stack: 'Error: Updated error message\n    at UpdatedFunction (/updated.ts:1:1)',
      };

      await vi.advanceTimersByTimeAsync(1000);
      await storage.updateJobRun(runId, updates);
      const run = await storage.getJobRun(runId);

      // Verify all updated fields
      expect(run?.status).toBe(updates.status);
      expect(run?.progress).toBe(updates.progress);
      expect(run?.executionDuration).toBe(updates.executionDuration);
      expect(run?.startedAt.getTime()).toBe(updates.startedAt.getTime());
      expect(run?.finishedAt?.getTime()).toBe(updates.finishedAt.getTime());
      expect(run?.attempt).toBe(updates.attempt);
      expect(run?.error).toBe(updates.error);
      expect(run?.error_stack).toBe(updates.error_stack);

      // Verify unchanged fields
      expect(run?.id).toBe(runId);
      expect(run?.jobId).toBe(jobId);
    });

    it('should handle job run error scenarios', async () => {
      const runId = await storage.createJobRun({
        jobId,
        status: 'running',
        attempt: 1,
        startedAt: new Date(),
      });

      // Test error update
      const error = new Error('Test error');
      await storage.updateJobRun(runId, {
        status: 'failed',
        error: error.message,
        error_stack: error.stack,
        finishedAt: new Date(),
      });

      const runs = await storage.listJobRuns(jobId);
      expect(runs[0].status).toBe('failed');
      expect(runs[0].error).toBe(error.message);
      expect(runs[0].error_stack).toBe(error.stack);
    });

    it('should handle multiple runs for a single job', async () => {
      // Create multiple runs with different attempts
      await Promise.all([
        storage.createJobRun({
          jobId,
          status: 'failed',
          attempt: 1,
          startedAt: new Date(now.getTime() - 2000),
          finishedAt: new Date(now.getTime() - 1000),
          error: 'First attempt failed',
        }),
        storage.createJobRun({
          jobId,
          status: 'completed',
          attempt: 2,
          startedAt: new Date(now.getTime() - 1000),
          finishedAt: new Date(),
        }),
      ]);

      const jobRuns = await storage.listJobRuns(jobId);
      expect(jobRuns).toHaveLength(2);
      expect(jobRuns[0].attempt).toBe(1);
      expect(jobRuns[1].attempt).toBe(2);
      expect(jobRuns[0].status).toBe('failed');
      expect(jobRuns[1].status).toBe('completed');
    });

    it('should create job run with default progress and update progress', async () => {
      const runId = await storage.createJobRun({
        jobId,
        status: 'running',
        startedAt: new Date(),
        attempt: 1,
      });

      // Check default progress
      const run = await storage.getJobRun(runId);
      expect(run?.progress).toBe(0);

      // Update progress
      await storage.updateJobRun(runId, {
        progress: 75,
      });

      const updatedRun = await storage.getJobRun(runId);
      expect(updatedRun?.progress).toBe(75);
    });
  });

  describe('Job Logs', () => {
    let jobId: string;
    let runId: string;

    beforeEach(async () => {
      jobId = await storage.createJob({
        ...defaultJob,
      });

      runId = await storage.createJobRun({
        jobId,
        status: 'running',
        attempt: 1,
        startedAt: new Date(),
      });
    });

    it('should create and retrieve job logs', async () => {
      await storage.createJobLog({
        jobId,
        jobRunId: runId,
        level: 'info',
        message: 'Test log message',
        metadata: { test: 'data' },
      });

      const logs = await storage.listJobLogs(jobId);
      expect(logs).toHaveLength(1);
      expect(logs[0].level).toBe('info');
      expect(logs[0].message).toBe('Test log message');
      expect(logs[0].metadata).toEqual({ test: 'data' });
    });

    it('should handle different log levels and metadata types', async () => {
      await Promise.all([
        storage.createJobLog({
          jobId,
          jobRunId: runId,
          level: 'error',
          message: 'Error message',
          metadata: { error: new Error('Test error').message },
        }),
        storage.createJobLog({
          jobId,
          jobRunId: runId,
          level: 'debug',
          message: 'Debug message',
          metadata: { debug: true },
        }),
        storage.createJobLog({
          jobId,
          jobRunId: runId,
          level: 'info',
          message: 'Info with empty metadata',
          metadata: {},
        }),
      ]);

      const logs = await storage.listJobLogs(jobId);
      expect(logs).toHaveLength(3);
      expect(logs.map((log) => log.level)).toEqual(['error', 'debug', 'info']);
      expect(logs[2].metadata).toEqual({});
    });

    it('should handle logs for multiple job runs', async () => {
      const runId2 = await storage.createJobRun({
        jobId,
        status: 'running',
        attempt: 2,
        startedAt: new Date(),
      });

      await Promise.all([
        storage.createJobLog({
          jobId,
          jobRunId: runId,
          level: 'info',
          message: 'First run log',
        }),
        storage.createJobLog({
          jobId,
          jobRunId: runId2,
          level: 'info',
          message: 'Second run log',
        }),
      ]);

      const logs = await storage.listJobLogs(jobId);
      expect(logs).toHaveLength(2);
      expect(logs[0].jobRunId).toBe(runId);
      expect(logs[1].jobRunId).toBe(runId2);
    });
  });

  describe('Scheduled Jobs', () => {
    it('should create and retrieve a scheduled job', async () => {
      const jobData = {
        name: 'test-scheduled-job',
        type: 'test-job',
        scheduleType: 'cron',
        scheduleValue: '* * * * *',
        data: { test: 'data' },
        enabled: true,
        lastScheduledAt: new Date(now.getDate() + 1000),
        nextRunAt: new Date(now.getDate() + 2000),
      };

      const jobId = await storage.createScheduledJob({
        name: 'test-scheduled-job',
        type: 'test-job',
        scheduleType: 'cron',
        scheduleValue: '* * * * *',
        data: { test: 'data' },
        enabled: true,
        lastScheduledAt: new Date(now.getDate() + 1000),
        nextRunAt: new Date(now.getDate() + 2000),
      });
      const job = await storage.getScheduledJob(jobId);

      expect(job).toBeDefined();
      expect(job?.nextRunAt?.toISOString()).toBe(jobData.nextRunAt.toISOString());
      expect(job?.lastScheduledAt?.toISOString()).toBe(jobData.lastScheduledAt.toISOString());
      expect(job?.name).toBe(jobData.name);
      expect(job?.type).toBe(jobData.type);
      expect(job?.scheduleType).toBe(jobData.scheduleType);
      expect(job?.scheduleValue).toBe(jobData.scheduleValue);
      expect(job?.data).toEqual(jobData.data);
      expect(job?.enabled).toBe(jobData.enabled);
      expect(job?.createdAt?.toISOString()).toBe(now.toISOString());
      expect(job?.updatedAt?.toISOString()).toBe(now.toISOString());
    });

    it('should update a scheduled job', async () => {
      const jobId = await storage.createScheduledJob({
        name: 'test-scheduled-job2',
        type: 'test-job2',
        scheduleType: 'interval',
        scheduleValue: '2m',
        data: { test: 'data2' },
        enabled: false,
      });

      const updates = {
        name: 'test-scheduled-job',
        scheduleType: 'cron' as const,
        scheduleValue: '* * * * *',
        data: { test: 'data' },
        enabled: true,
        lastScheduledAt: new Date(now.getDate() + 1000),
        nextRunAt: new Date(now.getDate() + 2000),
      };

      await storage.updateScheduledJob(jobId, updates);
      const job = await storage.getScheduledJob(jobId);

      expect(job?.nextRunAt?.toISOString()).toBe(updates.nextRunAt.toISOString());
      expect(job?.lastScheduledAt?.toISOString()).toBe(updates.lastScheduledAt.toISOString());
      expect(job?.name).toBe(updates.name);
      expect(job?.scheduleType).toBe(updates.scheduleType);
      expect(job?.scheduleValue).toBe(updates.scheduleValue);
      expect(job?.data).toEqual(updates.data);
      expect(job?.enabled).toBe(updates.enabled);
      expect(job?.createdAt?.toISOString()).toBe(now.toISOString());
      expect(job?.updatedAt?.toISOString()).toBe(now.toISOString());
    });

    it('should throw ScheduledJobNotFoundError when updating non-existent job', async () => {
      await expect(
        storage.updateScheduledJob(randomUUID(), {
          enabled: false,
        })
      ).rejects.toThrow(ScheduledJobNotFoundError);
    });

    it('should list scheduled jobs with filters', async () => {
      await storage.createScheduledJob({
        name: 'job-1',
        type: 'test-job',
        scheduleType: 'cron',
        scheduleValue: '* * * * *',
        data: {},
        enabled: true,
      });

      await storage.createScheduledJob({
        name: 'job-2',
        type: 'test-job',
        scheduleType: 'cron',
        scheduleValue: '* * * * *',
        data: {},
        enabled: false,
      });

      const enabledJobs = await storage.listScheduledJobs({ enabled: true });
      expect(enabledJobs).toHaveLength(1);
      expect(enabledJobs[0].name).toBe('job-1');
    });

    it('should handle concurrent acquireTypeSlot calls correctly', async () => {
      const jobType = 'concurrent-test';
      const maxConcurrent = 2;

      const promises = [
        storage.acquireTypeSlot(jobType, 'worker-1', maxConcurrent),
        storage.acquireTypeSlot(jobType, 'worker-2', maxConcurrent),
        storage.acquireTypeSlot(jobType, 'worker-3', maxConcurrent),
      ];

      const results = await Promise.all(promises);

      // Only two of the three should succeed
      const successfulAcquisitions = results.filter((res) => res).length;
      expect(successfulAcquisitions).toBe(2);

      const runningCount = await storage.getRunningCount(jobType);
      expect(runningCount).toBe(2);
    });
  });

  describe('Dead Letter Queue', () => {
    it('should create and list dead letter jobs', async () => {
      await storage.createDeadLetterJob({
        jobId: 'test-job',
        jobType: 'test-type',
        failReason: 'test failed',
        data: { foo: 'bar' },
        failedAt: new Date(),
      });
      const jobs = await storage.listDeadLetterJobs();

      expect(jobs).toHaveLength(1);
      expect(jobs[0].jobId).toBe('test-job');
      expect(jobs[0].jobType).toBe('test-type');
      expect(jobs[0].failReason).toBe('test failed');
      expect(jobs[0].data).toEqual({ foo: 'bar' });
    });
  });

  describe('Locks', () => {
    it('should list all locks', async () => {
      const lockId1 = 'test-lock-1';
      const lockId2 = 'test-lock-2';
      const worker1 = 'worker-1';
      const worker2 = 'worker-2';

      // Acquire locks with different workers
      await storage.acquireLock(lockId1, worker1, defaultValues.lockTTL);
      await storage.acquireLock(lockId2, worker2, defaultValues.lockTTL);

      // List all locks
      const locks = await storage.listLocks();
      expect(locks).toHaveLength(2);
      expect(locks).toEqual(
        expect.arrayContaining([
          expect.objectContaining({ lockId: lockId1, worker: worker1 }),
          expect.objectContaining({ lockId: lockId2, worker: worker2 }),
        ])
      );
    });

    it('should list locks filtered by worker', async () => {
      const lockId1 = 'test-lock-1';
      const lockId2 = 'test-lock-2';
      const worker1 = 'worker-1';
      const worker2 = 'worker-2';

      // Acquire locks with different workers
      await storage.acquireLock(lockId1, worker1, defaultValues.lockTTL);
      await storage.acquireLock(lockId2, worker2, defaultValues.lockTTL);

      // List locks for worker1
      const worker1Locks = await storage.listLocks({ worker: worker1 });
      expect(worker1Locks).toHaveLength(1);
      expect(worker1Locks[0]).toEqual(
        expect.objectContaining({
          lockId: lockId1,
          worker: worker1,
        })
      );

      // List locks for worker2
      const worker2Locks = await storage.listLocks({ worker: worker2 });
      expect(worker2Locks).toHaveLength(1);
      expect(worker2Locks[0]).toEqual(
        expect.objectContaining({
          lockId: lockId2,
          worker: worker2,
        })
      );
    });

    it('should acquire, renew, and release locks', async () => {
      const lockId = 'test-lock';

      // Acquire lock
      const acquired = await storage.acquireLock(
        lockId,
        defaultValues.workerName,
        defaultValues.lockTTL
      );
      expect(acquired).toBe(true);

      // Try to acquire same lock
      const secondAcquire = await storage.acquireLock(lockId, 'other-owner', defaultValues.lockTTL);
      expect(secondAcquire).toBe(false);

      // Renew lock
      const renewed = await storage.renewLock(
        lockId,
        defaultValues.workerName,
        defaultValues.lockTTL
      );
      expect(renewed).toBe(true);

      // Release lock
      const released = await storage.releaseLock(lockId, defaultValues.workerName);
      expect(released).toBe(true);

      // Verify lock is released
      const acquiredAfterRelease = await storage.acquireLock(
        lockId,
        'other-owner',
        defaultValues.lockTTL
      );
      expect(acquiredAfterRelease).toBe(true);
    });

    it('should handle lock expiration correctly', async () => {
      const lockId = 'test-lock';

      // Acquire initial lock
      const acquired = await storage.acquireLock(
        lockId,
        defaultValues.workerName,
        defaultValues.lockTTL
      );
      expect(acquired).toBe(true);

      // Move time forward past TTL
      await vi.advanceTimersByTimeAsync(defaultValues.lockTTL + 100);

      // Another owner should be able to acquire the expired lock
      const newOwner = 'new-owner';
      const reacquired = await storage.acquireLock(lockId, newOwner, defaultValues.lockTTL);
      expect(reacquired).toBe(true);

      // Original owner's renewal should fail
      const renewed = await storage.renewLock(
        lockId,
        defaultValues.workerName,
        defaultValues.lockTTL
      );
      expect(renewed).toBe(false);
    });
  });

  describe('Cleanup', () => {
    it('should cleanup old jobs and related data', async () => {
      vi.setSystemTime(new Date(now.getTime() - 1000 * 60 * 60 * 24 * 7));

      // Create a job
      const jobId = await storage.createJob({
        type: 'test-job',
        status: 'completed',
        data: {},
        priority: 1,
        maxRetries: 3,
        backoffStrategy: 'exponential',
        runAt: new Date(),
      });

      await storage.createJobRun({
        jobId,
        status: 'completed',
        attempt: 1,
        startedAt: new Date(),
      });

      await storage.createJobLog({
        jobId,
        level: 'info',
        message: 'Test log',
      });

      // Move back to now
      vi.setSystemTime(now);

      await storage.cleanup({
        jobRetention: 1, // 1 day
        failedJobRetention: 1,
        deadLetterRetention: 1,
      });

      const job = await storage.getJob(jobId);
      expect(job).toBeUndefined();

      const runs = await storage.listJobRuns(jobId);
      expect(runs).toHaveLength(0);

      const logs = await storage.listJobLogs(jobId);
      expect(logs).toHaveLength(0);
    });

    it('should handle cleanup with different retention periods', async () => {
      // Set time to 30 days ago
      const thirtyDaysAgo = new Date(now.getTime() - 1000 * 60 * 60 * 24 * 30);
      vi.setSystemTime(thirtyDaysAgo);

      // Create old completed job (30 days old)
      const oldCompletedJobId = await storage.createJob({
        type: 'test-job',
        status: 'completed',
        data: {},
        priority: 1,
        maxRetries: 3,
        backoffStrategy: 'exponential',
        runAt: new Date(),
        finishedAt: new Date(),
      });

      // Set time to 20 days ago
      const twentyDaysAgo = new Date(now.getTime() - 1000 * 60 * 60 * 24 * 20);
      vi.setSystemTime(twentyDaysAgo);

      // Create old failed job (20 days old)
      const oldFailedJobId = await storage.createJob({
        type: 'test-job',
        status: 'failed',
        data: {},
        priority: 1,
        maxRetries: 3,
        backoffStrategy: 'exponential',
        runAt: new Date(),
        finishedAt: new Date(),
      });

      // Set time to 10 days ago
      const tenDaysAgo = new Date(now.getTime() - 1000 * 60 * 60 * 24 * 16);
      vi.setSystemTime(tenDaysAgo);

      // Create dead letter job (10 days old)
      await storage.createDeadLetterJob({
        jobId: randomUUID(),
        jobType: 'test-job',
        data: {},
        failedAt: new Date(),
        failReason: 'test',
      });

      // Move to current time
      vi.setSystemTime(now);

      // Cleanup with different retention periods
      await storage.cleanup({
        jobRetention: 15, // 15 days for completed jobs
        failedJobRetention: 25, // 25 days for failed jobs
        deadLetterRetention: 15, // 15 days for dead letter jobs
      });

      // Check cleanup results
      const completedJob = await storage.getJob(oldCompletedJobId);
      expect(completedJob).toBeUndefined(); // Should be cleaned up (30 days > 15 days retention)

      const failedJob = await storage.getJob(oldFailedJobId);
      expect(failedJob).not.toBeUndefined(); // Should not be cleaned up (20 days < 25 days retention)

      const deadLetterJobs = await storage.listDeadLetterJobs();
      expect(deadLetterJobs).toHaveLength(0); // Should be cleaned up (10 days < 15 days retention)
    });
  });

  describe('Metrics', () => {
    it('should return storage metrics', async () => {
      // Create some test data
      const jobId1 = await storage.createJob({
        type: 'test-job',
        status: 'completed',
        data: {},
        priority: 1,
        maxRetries: 3,
        backoffStrategy: 'exponential',
        runAt: new Date(),
      });

      await storage.createJobRun({
        jobId: jobId1,
        status: 'completed',
        executionDuration: 1000,
        attempt: 1,
        startedAt: new Date(),
        finishedAt: new Date(),
      });

      const jobId2 = await storage.createJob({
        type: 'test-job',
        status: 'failed',
        data: {},
        priority: 1,
        maxRetries: 3,
        backoffStrategy: 'exponential',
        runAt: new Date(),
      });

      await storage.createJobRun({
        jobId: jobId2,
        status: 'completed',
        executionDuration: 1000,
        attempt: 1,
        startedAt: new Date(),
        finishedAt: new Date(),
      });

      await storage.createDeadLetterJob({
        jobId: randomUUID(),
        jobType: 'test-job',
        data: {},
        failedAt: new Date(),
        failReason: 'test',
      });

      const metrics = await storage.getMetrics();

      // Check job counts
      expect(metrics.jobs.total).toBe(2);
      expect(metrics.jobs.completed).toBe(1);
      expect(metrics.jobs.failed).toBe(1);
      expect(metrics.jobs.deadLetter).toBe(1);
      expect(metrics.jobs.pending).toBe(0);
      expect(metrics.jobs.scheduled).toBe(0);

      // Check metrics by job type
      expect(metrics.averageDurationByType['test-job']).toBe(1000);
      expect(metrics.failureRateByType['test-job']).toBe(0.5); // 1 failed out of 2 total
    });
  });

  describe('Worker Management', () => {
    it('should register a worker', async () => {
      const worker = await storage.registerWorker({ name: 'worker-1' });
      expect(worker).toBe('worker-1');

      const savedWorker = await storage.getWorker('worker-1');
      expect(savedWorker).toBeTruthy();
      expect(savedWorker?.name).toBe('worker-1');
      expect(savedWorker?.first_seen).toBeInstanceOf(Date);
      expect(savedWorker?.last_heartbeat).toBeInstanceOf(Date);
    });

    it('should upsert worker and preserve first_seen', async () => {
      // First registration
      const firstSeen = new Date();
      await storage.registerWorker({ name: 'worker-1' });

      // Get the worker and verify first_seen
      const worker1 = await storage.getWorker('worker-1');
      expect(worker1?.first_seen.getTime()).toBeGreaterThanOrEqual(firstSeen.getTime());

      // Wait a bit and register again
      const tenSecondsLater = new Date(firstSeen.getTime() + 1000 * 10);
      vi.setSystemTime(tenSecondsLater);
      await storage.registerWorker({ name: 'worker-1' });

      // Get the worker again and verify first_seen is preserved but last_heartbeat is updated
      const worker2 = await storage.getWorker('worker-1');
      expect(worker2?.first_seen.getTime()).toBe(worker1?.first_seen.getTime());
      expect(worker2?.last_heartbeat.getTime()).toEqual(tenSecondsLater.getTime());
    });

    it('should update worker heartbeat', async () => {
      await storage.registerWorker({ name: 'worker-1' });
      const newHeartbeat = new Date();
      await storage.updateWorkerHeartbeat('worker-1', { last_heartbeat: newHeartbeat });

      const worker = await storage.getWorker('worker-1');
      expect(worker?.last_heartbeat.getTime()).toBe(newHeartbeat.getTime());
    });

    it('should throw error when updating non-existent worker', async () => {
      await expect(
        storage.updateWorkerHeartbeat('non-existent', { last_heartbeat: new Date() })
      ).rejects.toThrow();
    });

    it('should get inactive workers', async () => {
      const now = new Date();
      const oldDate = new Date(now.getTime() - 1000 * 60 * 60); // 1 hour ago

      await storage.registerWorker({ name: 'active-worker' });
      await storage.registerWorker({ name: 'inactive-worker' });
      await storage.updateWorkerHeartbeat('inactive-worker', { last_heartbeat: oldDate });

      const inactiveWorkers = await storage.getInactiveWorkers(now);
      expect(inactiveWorkers).toHaveLength(1);
      expect(inactiveWorkers[0].name).toBe('inactive-worker');
    });

    it('should delete worker', async () => {
      await storage.registerWorker({ name: 'worker-1' });
      await storage.deleteWorker('worker-1');

      const worker = await storage.getWorker('worker-1');
      expect(worker).toBeNull();
    });

    it('should throw error when deleting non-existent worker', async () => {
      await expect(storage.deleteWorker('non-existent')).rejects.toThrow();
    });
  });

  describe('Job Type Slot Management', () => {
    beforeEach(async () => {
      await storage.registerWorker({ name: 'worker-1' });
      await storage.registerWorker({ name: 'worker-2' });
    });

    it('should acquire job type slot', async () => {
      const acquired = await storage.acquireTypeSlot('test-job', 'worker-1', 2);
      expect(acquired).toBe(true);

      const count = await storage.getRunningCount('test-job');
      expect(count).toBe(1);
    });

    it('should respect max concurrent limit', async () => {
      const first = await storage.acquireTypeSlot('test-job', 'worker-1', 2);
      const second = await storage.acquireTypeSlot('test-job', 'worker-2', 2);
      const third = await storage.acquireTypeSlot('test-job', 'worker-1', 2);

      expect(first).toBe(true);
      expect(second).toBe(true);
      expect(third).toBe(false);

      const count = await storage.getRunningCount('test-job');
      expect(count).toBe(2);
    });

    it('should release job type slot', async () => {
      await storage.acquireTypeSlot('test-job', 'worker-1', 2);
      await storage.releaseTypeSlot('test-job', 'worker-1');

      const count = await storage.getRunningCount('test-job');
      expect(count).toBe(0);
    });

    it('should release all job type slots for a worker', async () => {
      await storage.acquireTypeSlot('test-job-1', 'worker-1', 2);
      await storage.acquireTypeSlot('test-job-2', 'worker-1', 2);
      await storage.releaseAllTypeSlots('worker-1');

      const count1 = await storage.getRunningCount('test-job-1');
      const count2 = await storage.getRunningCount('test-job-2');
      expect(count1).toBe(0);
      expect(count2).toBe(0);
    });

    it('should get total running count', async () => {
      await storage.acquireTypeSlot('test-job-1', 'worker-1', 2);
      await storage.acquireTypeSlot('test-job-2', 'worker-1', 2);
      await storage.acquireTypeSlot('test-job-1', 'worker-2', 2);

      const total = await storage.getRunningCount();
      expect(total).toBe(3);
    });

    it('should get running count for specific job type', async () => {
      await storage.acquireTypeSlot('test-job-1', 'worker-1', 5);
      await storage.acquireTypeSlot('test-job-1', 'worker-2', 5);
      await storage.acquireTypeSlot('test-job-2', 'worker-1', 5);

      const count = await storage.getRunningCount('test-job-1');
      expect(count).toBe(2);
    });

    it('should get total running count across all job types', async () => {
      await storage.acquireTypeSlot('test-job-1', 'worker-1', 5);
      await storage.acquireTypeSlot('test-job-1', 'worker-2', 5);
      await storage.acquireTypeSlot('test-job-2', 'worker-1', 5);
      await storage.acquireTypeSlot('test-job-3', 'worker-2', 5);

      const total = await storage.getRunningCount();
      expect(total).toBe(4);
    });

    it('should return 0 for non-existent job type', async () => {
      const count = await storage.getRunningCount('non-existent-job');
      expect(count).toBe(0);
    });

    it('should return 0 when no jobs are running', async () => {
      const total = await storage.getRunningCount();
      expect(total).toBe(0);
    });
  });
});
