import { randomUUID } from 'crypto';
import Database from 'better-sqlite3';
import { describe, it, expect, beforeEach, afterEach, vi } from 'vitest';

import { SQLiteJobStorage } from './adapter.js';
import { JobNotFoundError, ScheduledJobNotFoundError } from '@timecrisis/timecrisis';

describe('SQLiteJobStorage', () => {
  let db: Database.Database;
  let storage: SQLiteJobStorage;
  const now = new Date('2025-01-23T00:00:00.000Z');

  beforeEach(async () => {
    vi.useFakeTimers();
    vi.setSystemTime(now);

    // Create an in-memory database for testing
    db = new Database(':memory:');
    storage = new SQLiteJobStorage(db);
    await storage.init();

    // Clear all mocks before each test
    vi.clearAllMocks();
  });

  afterEach(async () => {
    await storage.close();

    // Clear all timers and mocks
    vi.clearAllTimers();
    vi.clearAllMocks();
    vi.useRealTimers();
  });

  describe('Job Management', () => {
    it('should create and retrieve a job', async () => {
      const jobData = {
        type: 'test-job',
        status: 'pending',
        data: { test: 'data' },
        priority: 1,
        attempts: 0,
        maxRetries: 3,
        backoffStrategy: 'exponential',
        runAt: new Date(),
      };

      const jobId = await storage.createJob({
        type: 'test-job',
        status: 'pending',
        data: { test: 'data' },
        priority: 1,
        attempts: 0,
        maxRetries: 3,
        backoffStrategy: 'exponential',
        runAt: new Date(),
      });
      expect(jobId).toBeDefined();

      const job = await storage.getJob(jobId);
      expect(job).toBeDefined();
      expect(job?.type).toBe(jobData.type);
      expect(job?.status).toBe(jobData.status);
      expect(job?.data).toEqual(jobData.data);
      expect(job?.priority).toBe(jobData.priority);
    });

    it('should update a job', async () => {
      const jobId = await storage.createJob({
        type: 'test-job',
        status: 'pending',
        data: { test: 'data' },
        priority: 1,
        attempts: 0,
        maxRetries: 3,
        backoffStrategy: 'exponential',
        runAt: new Date(),
      });

      const updates = {
        status: 'running',
        data: { test: 'updated' },
      };

      await storage.updateJob(jobId, {
        status: 'running',
        data: { test: 'updated' },
      });
      const job = await storage.getJob(jobId);

      expect(job?.status).toBe(updates.status);
      expect(job?.data).toEqual(updates.data);
    });

    it('should throw JobNotFoundError when updating non-existent job', async () => {
      await expect(
        storage.updateJob(randomUUID(), {
          status: 'running',
        })
      ).rejects.toThrow(JobNotFoundError);
    });

    it('should list jobs with filters', async () => {
      const now = new Date();
      await Promise.all([
        storage.createJob({
          type: 'test-job-1',
          status: 'pending',
          data: {},
          priority: 1,
          attempts: 0,
          maxRetries: 3,
          backoffStrategy: 'exponential',
          runAt: now,
        }),
        storage.createJob({
          type: 'test-job-2',
          status: 'running',
          data: {},
          priority: 2,
          attempts: 0,
          maxRetries: 3,
          backoffStrategy: 'exponential',
          runAt: now,
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
      const baseDate = new Date('2025-01-23T00:00:00.000Z');
      await Promise.all([
        storage.createJob({
          type: 'type-1',
          status: 'pending',
          data: {},
          priority: 1,
          attempts: 0,
          maxRetries: 3,
          backoffStrategy: 'exponential',
          runAt: new Date(baseDate.getTime() + 1000),
          referenceId: 'ref-1',
        }),
        storage.createJob({
          type: 'type-1',
          status: 'running',
          data: {},
          priority: 2,
          attempts: 0,
          maxRetries: 3,
          backoffStrategy: 'exponential',
          runAt: new Date(baseDate.getTime() + 2000),
          referenceId: 'ref-2',
        }),
        storage.createJob({
          type: 'type-2',
          status: 'pending',
          data: {},
          priority: 3,
          attempts: 0,
          maxRetries: 3,
          backoffStrategy: 'exponential',
          runAt: new Date(baseDate.getTime() + 3000),
          referenceId: 'ref-1',
        }),
      ]);

      // Test multiple status filters
      const pendingAndRunning = await storage.listJobs({ status: ['pending', 'running'] });
      expect(pendingAndRunning).toHaveLength(3);

      // Test type and referenceId combination
      const type1Ref1Jobs = await storage.listJobs({
        type: 'type-1',
        referenceId: 'ref-1',
      });
      expect(type1Ref1Jobs).toHaveLength(1);
      expect(type1Ref1Jobs[0].type).toBe('type-1');
      expect(type1Ref1Jobs[0].referenceId).toBe('ref-1');

      // Test runAt filtering
      const futureRunAt = new Date(baseDate.getTime() + 2500);
      const jobsBeforeTime = await storage.listJobs({
        runAtBefore: futureRunAt,
      });
      expect(jobsBeforeTime).toHaveLength(2);

      // Test priority ordering
      const orderedJobs = await storage.listJobs({});
      expect(orderedJobs).toHaveLength(3);
      expect(orderedJobs[0].priority).toBe(3);
      expect(orderedJobs[1].priority).toBe(2);
      expect(orderedJobs[2].priority).toBe(1);

      // Test limit
      const limitedJobs = await storage.listJobs({ limit: 2 });
      expect(limitedJobs).toHaveLength(2);
    });

    it('should handle null and undefined filter values correctly', async () => {
      await storage.createJob({
        type: 'test-job',
        status: 'pending',
        data: {},
        priority: 1,
        attempts: 0,
        maxRetries: 3,
        backoffStrategy: 'exponential',
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
        type: 'test-job',
        status: 'pending',
        data: { test: 'data' },
        priority: 1,
        attempts: 0,
        maxRetries: 3,
        backoffStrategy: 'exponential',
        runAt: new Date(),
      });

      // Simulate concurrent updates
      await Promise.all([
        storage.updateJob(jobId, { status: 'running', attempts: 1 }),
        storage.updateJob(jobId, { status: 'running', attempts: 2 }),
      ]);

      const job = await storage.getJob(jobId);
      expect(job?.attempts).toBe(2);
    });

    it('should handle special characters and SQL injection attempts in filters', async () => {
      const jobId = await storage.createJob({
        type: 'test-job',
        status: 'pending',
        data: {},
        priority: 1,
        attempts: 0,
        maxRetries: 3,
        backoffStrategy: 'exponential',
        runAt: new Date(),
        referenceId: "ref-1'; DROP TABLE jobs; --",
      });

      const jobs = await storage.listJobs({
        referenceId: "ref-1'; DROP TABLE jobs; --",
      });
      expect(jobs).toHaveLength(1);
      expect(jobs[0].id).toBe(jobId);

      // Verify table wasn't dropped
      const allJobs = await storage.listJobs({});
      expect(allJobs).toHaveLength(1);
    });

    it('should handle null and undefined filter values correctly', async () => {
      await storage.createJob({
        type: 'test-job',
        status: 'pending',
        data: {},
        priority: 1,
        attempts: 0,
        maxRetries: 3,
        backoffStrategy: 'exponential',
        runAt: null,
        referenceId: null,
      });

      // Test with undefined values
      const jobsWithUndefinedFilters = await storage.listJobs({
        type: 'test-job',
        referenceId: undefined,
        runAtBefore: undefined,
        lockedBefore: undefined,
      });
      expect(jobsWithUndefinedFilters).toHaveLength(1);
    });

    it('should handle multiple status values correctly', async () => {
      // Create jobs with different statuses
      await Promise.all([
        storage.createJob({
          type: 'test-job',
          status: 'pending',
          data: {},
          priority: 1,
          attempts: 0,
          maxRetries: 3,
          backoffStrategy: 'exponential',
        }),
        storage.createJob({
          type: 'test-job',
          status: 'running',
          data: {},
          priority: 1,
          attempts: 0,
          maxRetries: 3,
          backoffStrategy: 'exponential',
        }),
        storage.createJob({
          type: 'test-job',
          status: 'failed',
          data: {},
          priority: 1,
          attempts: 0,
          maxRetries: 3,
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

    it('should handle date filters correctly across timezones', async () => {
      const baseDate = new Date('2025-01-23T00:00:00.000Z');

      // Create a job with a specific UTC time
      await storage.createJob({
        type: 'test-job',
        status: 'pending',
        data: {},
        priority: 1,
        attempts: 0,
        maxRetries: 3,
        backoffStrategy: 'exponential',
        runAt: baseDate,
      });

      // Test exact UTC time
      const jobsAtExactTime = await storage.listJobs({
        runAtBefore: baseDate,
      });
      expect(jobsAtExactTime).toHaveLength(1);

      // Test with different timezone offset
      const dateInDifferentTz = new Date(baseDate.getTime() + 60 * 60 * 1000); // +1 hour
      const jobsWithTzOffset = await storage.listJobs({
        runAtBefore: dateInDifferentTz,
      });
      expect(jobsWithTzOffset).toHaveLength(1);

      // Test with date slightly before
      const dateBefore = new Date(baseDate.getTime() - 1000); // 1 second before
      const jobsWithDateBefore = await storage.listJobs({
        runAtBefore: dateBefore,
      });
      expect(jobsWithDateBefore).toHaveLength(0);
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
            attempts: 0,
            maxRetries: 3,
            backoffStrategy: 'exponential',
          })
        )
      );

      // Test with different limit values
      const twoJobs = await storage.listJobs({ limit: 2 });
      expect(twoJobs).toHaveLength(2);
      expect(twoJobs[0].priority).toBe(5); // Highest priority first
      expect(twoJobs[1].priority).toBe(4);

      const allJobs = await storage.listJobs();
      expect(allJobs).toHaveLength(5);

      const zeroJobs = await storage.listJobs({ limit: 1 });
      expect(zeroJobs).toHaveLength(1);
    });

    it('should create job with default progress and update progress', async () => {
      const jobId = await storage.createJob({
        type: 'test-job',
        status: 'pending',
        data: { test: 'data' },
        priority: 1,
        attempts: 0,
        maxRetries: 3,
        backoffStrategy: 'exponential',
        runAt: new Date(),
      });

      // Check default progress
      const job = await storage.getJob(jobId);
      expect(job?.progress).toBe(0);

      // Update progress
      await storage.updateJob(jobId, {
        progress: 50,
      });

      const updatedJob = await storage.getJob(jobId);
      expect(updatedJob?.progress).toBe(50);
    });
  });

  describe('Job Runs', () => {
    let jobId: string;

    beforeEach(async () => {
      jobId = await storage.createJob({
        type: 'test-job',
        status: 'pending',
        data: { test: 'data' },
        priority: 1,
        attempts: 0,
        maxRetries: 3,
        backoffStrategy: 'exponential',
        runAt: new Date(),
      });
    });

    it('should create and retrieve a job run', async () => {
      const runData = {
        jobId,
        status: 'running',
        attempt: 1,
        startedAt: new Date(),
      };

      const runId = await storage.createJobRun({
        jobId,
        status: 'running',
        attempt: 1,
        startedAt: new Date(),
      });
      const runs = await storage.listJobRuns(jobId);

      expect(runs).toHaveLength(1);
      expect(runs[0].id).toBe(runId);
      expect(runs[0].status).toBe(runData.status);
      expect(runs[0].attempt).toBe(runData.attempt);
    });

    it('should update job run status correctly', async () => {
      const runId = await storage.createJobRun({
        jobId,
        status: 'running',
        attempt: 1,
        startedAt: new Date(),
      });

      await storage.updateJobRun(runId, {
        status: 'completed',
        finishedAt: new Date(),
      });

      const runs = await storage.listJobRuns(jobId);
      expect(runs[0].status).toBe('completed');
      expect(runs[0].finishedAt).toBeDefined();

      // Update to a new status
      await storage.updateJobRun(runId, {
        status: 'failed',
        error: 'Test failure',
      });

      const updatedRuns = await storage.listJobRuns(jobId);
      expect(updatedRuns[0].status).toBe('failed');
      expect(updatedRuns[0].error).toBe('Test failure');
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
        type: 'test-job',
        status: 'pending',
        data: {},
        priority: 1,
        attempts: 0,
        maxRetries: 3,
        backoffStrategy: 'exponential',
        runAt: new Date(),
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
      };

      const jobId = await storage.createScheduledJob({
        name: 'test-scheduled-job',
        type: 'test-job',
        scheduleType: 'cron',
        scheduleValue: '* * * * *',
        data: { test: 'data' },
        enabled: true,
      });
      const job = await storage.getScheduledJob(jobId);

      expect(job).toBeDefined();
      expect(job?.name).toBe(jobData.name);
      expect(job?.type).toBe(jobData.type);
      expect(job?.scheduleType).toBe(jobData.scheduleType);
      expect(job?.scheduleValue).toBe(jobData.scheduleValue);
      expect(job?.data).toEqual(jobData.data);
      expect(job?.enabled).toBe(jobData.enabled);
    });

    it('should update a scheduled job', async () => {
      const jobId = await storage.createScheduledJob({
        name: 'test-scheduled-job',
        type: 'test-job',
        scheduleType: 'cron',
        scheduleValue: '* * * * *',
        data: { test: 'data' },
        enabled: true,
      });

      const updates = {
        name: 'updated-job',
        enabled: false,
      };

      await storage.updateScheduledJob(jobId, updates);
      const job = await storage.getScheduledJob(jobId);

      expect(job?.name).toBe(updates.name);
      expect(job?.enabled).toBe(updates.enabled);
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
  });

  describe('Dead Letter Queue', () => {
    it('should create and list dead letter jobs', async () => {
      await storage.createDeadLetterJob({
        jobId: 'test-job',
        jobType: 'test-type',
        reason: 'test failed',
        data: { foo: 'bar' },
        failedAt: new Date(),
      });
      const jobs = await storage.listDeadLetterJobs();

      expect(jobs).toHaveLength(1);
      expect(jobs[0].jobId).toBe('test-job');
      expect(jobs[0].jobType).toBe('test-type');
      expect(jobs[0].reason).toBe('test failed');
      expect(jobs[0].data).toEqual({ foo: 'bar' });
    });
  });

  describe('Locks', () => {
    it('should acquire, renew, and release locks', async () => {
      const lockId = 'test-lock';
      const owner = 'test-owner';
      const ttlMs = 1000;

      // Acquire lock
      const acquired = await storage.acquireLock(lockId, owner, ttlMs);
      expect(acquired).toBe(true);

      // Try to acquire same lock
      const secondAcquire = await storage.acquireLock(lockId, 'other-owner', ttlMs);
      expect(secondAcquire).toBe(false);

      // Renew lock
      const renewed = await storage.renewLock(lockId, owner, ttlMs);
      expect(renewed).toBe(true);

      // Release lock
      const released = await storage.releaseLock(lockId, owner);
      expect(released).toBe(true);

      // Verify lock is released
      const acquiredAfterRelease = await storage.acquireLock(lockId, 'other-owner', ttlMs);
      expect(acquiredAfterRelease).toBe(true);
    });

    it('should handle lock expiration correctly', async () => {
      const lockId = 'test-lock';
      const owner = 'test-owner';
      const shortTtlMs = 1000;

      // Set time to now
      const initialTime = new Date('2025-01-23T00:00:00.000Z');
      vi.setSystemTime(initialTime);

      // Acquire initial lock
      const acquired = await storage.acquireLock(lockId, owner, shortTtlMs);
      expect(acquired).toBe(true);

      // Move time forward past TTL
      vi.setSystemTime(new Date(initialTime.getTime() + shortTtlMs + 100));

      // Another owner should be able to acquire the expired lock
      const newOwner = 'new-owner';
      const reacquired = await storage.acquireLock(lockId, newOwner, shortTtlMs);
      expect(reacquired).toBe(true);

      // Original owner's renewal should fail
      const renewed = await storage.renewLock(lockId, owner, shortTtlMs);
      expect(renewed).toBe(false);
    });
  });

  describe('Cleanup', () => {
    beforeEach(() => {
      vi.useFakeTimers();
    });

    afterEach(() => {
      vi.useRealTimers();
    });

    it('should cleanup old jobs and related data', async () => {
      vi.setSystemTime(new Date(now.getTime() - 1000 * 60 * 60 * 24 * 7));

      // Create a job
      const jobId = await storage.createJob({
        type: 'test-job',
        status: 'completed',
        data: {},
        priority: 1,
        attempts: 0,
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
      expect(job).toBeNull();

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
        attempts: 0,
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
        attempts: 3,
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
        reason: 'test',
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
      expect(completedJob).toBeNull(); // Should be cleaned up (30 days > 15 days retention)

      const failedJob = await storage.getJob(oldFailedJobId);
      expect(failedJob).not.toBeNull(); // Should not be cleaned up (20 days < 25 days retention)

      const deadLetterJobs = await storage.listDeadLetterJobs();
      expect(deadLetterJobs).toHaveLength(0); // Should be cleaned up (10 days < 15 days retention)
    });
  });

  describe('Metrics', () => {
    it('should return storage metrics', async () => {
      // Create some test data
      await storage.createJob({
        type: 'test-job',
        status: 'completed',
        data: {},
        priority: 1,
        attempts: 0,
        maxRetries: 3,
        backoffStrategy: 'exponential',
        runAt: new Date(),
        executionDuration: 1000,
      });

      await storage.createJob({
        type: 'test-job',
        status: 'failed',
        data: {},
        priority: 1,
        attempts: 1,
        maxRetries: 3,
        backoffStrategy: 'exponential',
        runAt: new Date(),
      });

      await storage.createDeadLetterJob({
        jobId: randomUUID(),
        jobType: 'test-job',
        data: {},
        failedAt: new Date(),
        reason: 'test',
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
});
