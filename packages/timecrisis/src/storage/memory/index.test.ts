import { describe, test, expect, beforeEach, vi, afterEach } from 'vitest';

import { Job } from '../schemas/index.js';
import { JobNotFoundError } from '../types.js';
import { InMemoryJobStorage } from './index.js';

describe('InMemoryJobStorage', () => {
  let storage: InMemoryJobStorage;
  const now = new Date('2025-01-23T00:00:00.000Z');

  beforeEach(() => {
    storage = new InMemoryJobStorage();
    vi.useFakeTimers();
    vi.setSystemTime(now);
  });

  afterEach(() => {
    vi.resetAllMocks();
    vi.clearAllTimers();
    vi.useRealTimers();
  });

  describe('Job Management', () => {
    test('should create and retrieve a job', async () => {
      const jobData: Omit<Job, 'id' | 'createdAt' | 'updatedAt'> = {
        type: 'test-job',
        status: 'pending',
        referenceId: 'ref-123',
        data: {},
        progress: 0,
        priority: 1,
        attempts: 0,
        maxRetries: 3,
        backoffStrategy: 'exponential',
        failCount: 0,
      };

      const jobId = await storage.createJob(jobData);
      expect(jobId).toBeDefined();

      const job = await storage.getJob(jobId);
      expect(job).toBeDefined();
      expect(job?.type).toBe(jobData.type);
      expect(job?.status).toBe(jobData.status);
      expect(job?.referenceId).toBe(jobData.referenceId);
      expect(job?.createdAt).toBeInstanceOf(Date);
      expect(job?.updatedAt).toBeInstanceOf(Date);
    });

    test('should throw error when updating non-existent job', async () => {
      await expect(storage.updateJob('non-existent', { status: 'completed' })).rejects.toThrow(
        JobNotFoundError
      );
    });

    test('should list jobs with filtering', async () => {
      await storage.createJob({
        type: 'job1',
        status: 'pending',
        priority: 0,
        attempts: 0,
        maxRetries: 3,
        backoffStrategy: 'exponential',
        failCount: 0,
        data: {},
      });
      await storage.createJob({
        type: 'job2',
        status: 'completed',
        priority: 0,
        attempts: 0,
        maxRetries: 3,
        backoffStrategy: 'exponential',
        failCount: 0,
        data: {},
      });
      await storage.createJob({
        type: 'job3',
        status: 'running',
        priority: 0,
        attempts: 0,
        maxRetries: 3,
        backoffStrategy: 'exponential',
        failCount: 0,
        data: {},
      });

      // Test single status (backward compatibility)
      const pendingJobs = await storage.listJobs({ status: ['pending'] });
      expect(pendingJobs).toHaveLength(1);
      expect(pendingJobs[0].status).toBe('pending');

      // Test multiple statuses
      const activeJobs = await storage.listJobs({ status: ['pending', 'running'] });
      expect(activeJobs).toHaveLength(2);
      expect(activeJobs.map((job) => job.status).sort()).toEqual(['pending', 'running']);

      // Test no status filter
      const allJobs = await storage.listJobs({});
      expect(allJobs).toHaveLength(3);
    });

    test('should filter jobs by runAtBefore', async () => {
      const now = new Date('2024-01-01T12:00:00Z');
      vi.setSystemTime(now);

      // Create a job scheduled for the future
      await storage.createJob({
        type: 'future-job',
        status: 'pending',
        priority: 0,
        attempts: 0,
        maxRetries: 3,
        backoffStrategy: 'exponential',
        failCount: 0,
        data: {},
        runAt: new Date('2024-01-01T13:00:00Z'),
      });

      // Create a job scheduled for the past
      await storage.createJob({
        type: 'past-job',
        status: 'pending',
        priority: 0,
        attempts: 0,
        maxRetries: 3,
        backoffStrategy: 'exponential',
        failCount: 0,
        data: {},
        runAt: new Date('2024-01-01T11:00:00Z'),
      });

      // Create a job with no runAt
      await storage.createJob({
        type: 'immediate-job',
        status: 'pending',
        priority: 0,
        attempts: 0,
        maxRetries: 3,
        backoffStrategy: 'exponential',
        failCount: 0,
        data: {},
      });

      // Should return past and immediate jobs
      const readyJobs = await storage.listJobs({ runAtBefore: now });
      expect(readyJobs).toHaveLength(2);
      expect(readyJobs.map((j) => j.type)).toEqual(
        expect.arrayContaining(['past-job', 'immediate-job'])
      );

      // Should return all jobs when checking future time
      const futureTime = new Date('2024-01-01T14:00:00Z');
      const allJobs = await storage.listJobs({ runAtBefore: futureTime });
      expect(allJobs).toHaveLength(3);
    });
  });

  describe('Metrics', () => {
    test('should calculate correct metrics', async () => {
      // Create some jobs with different statuses and durations
      await storage.createJob({
        type: 'job1',
        status: 'completed',
        executionDuration: 100,
        priority: 0,
        attempts: 0,
        maxRetries: 3,
        backoffStrategy: 'exponential',
        failCount: 0,
        data: {},
      });
      await storage.createJob({
        type: 'job1',
        status: 'failed',
        priority: 0,
        attempts: 0,
        maxRetries: 3,
        backoffStrategy: 'exponential',
        failCount: 1,
        data: {},
      });
      await storage.createJob({
        type: 'job2',
        status: 'completed',
        executionDuration: 200,
        priority: 0,
        attempts: 0,
        maxRetries: 3,
        backoffStrategy: 'exponential',
        failCount: 0,
        data: {},
      });

      const metrics = await storage.getMetrics();

      // Check job counts
      expect(metrics.jobs.total).toBe(3);
      expect(metrics.jobs.completed).toBe(2);
      expect(metrics.jobs.failed).toBe(1);

      // Check average durations
      expect(metrics.averageDurationByType['job1']).toBe(100);
      expect(metrics.averageDurationByType['job2']).toBe(200);

      // Check failure rates
      expect(metrics.failureRateByType['job1']).toBe(0.5); // 1 out of 2 failed
      expect(metrics.failureRateByType['job2']).toBe(0); // 0 out of 1 failed
    });
  });

  describe('Job Runs and Logs', () => {
    let jobId: string;

    beforeEach(async () => {
      jobId = await storage.createJob({
        type: 'test-job',
        status: 'pending',
        priority: 0,
        attempts: 0,
        maxRetries: 3,
        backoffStrategy: 'exponential',
        failCount: 0,
        data: {},
      });
    });

    test('should create and retrieve job runs', async () => {
      const runId = await storage.createJobRun({
        jobId,
        status: 'running',
        startedAt: new Date(),
        attempt: 1,
      });

      const runs = await storage.listJobRuns(jobId);
      expect(runs).toHaveLength(1);
      expect(runs[0].id).toBe(runId);
    });

    test('should update job run', async () => {
      const runId = await storage.createJobRun({
        jobId,
        status: 'running',
        startedAt: new Date(),
        attempt: 1,
      });

      await storage.updateJobRun(runId, { status: 'completed' });
      const runs = await storage.listJobRuns(jobId);
      expect(runs[0].status).toBe('completed');
    });

    test('should create and retrieve job logs', async () => {
      const runId = await storage.createJobRun({
        jobId,
        status: 'running',
        startedAt: new Date(),
        attempt: 1,
      });

      await storage.createJobLog({
        jobId,
        jobRunId: runId,
        level: 'info',
        message: 'Test log',
        timestamp: new Date(),
      });

      const logs = await storage.listJobLogs(jobId);
      expect(logs).toHaveLength(1);
      expect(logs[0].message).toBe('Test log');

      const runLogs = await storage.listJobLogs(jobId, runId);
      expect(runLogs).toHaveLength(1);
    });
  });

  describe('Scheduled Jobs', () => {
    test('should create and retrieve scheduled jobs', async () => {
      const jobId = await storage.createScheduledJob({
        type: 'scheduled-job',
        name: 'Scheduled Job',
        scheduleType: 'cron',
        scheduleValue: '* * * * *',
        enabled: true,
        nextRunAt: new Date(),
      });

      const job = await storage.getScheduledJob(jobId);
      expect(job!.id).toBeDefined();
      expect(job!.type).toBe('scheduled-job');

      const jobs = await storage.listScheduledJobs();
      expect(jobs).toHaveLength(1);
    });

    test('should update scheduled job', async () => {
      const jobId = await storage.createScheduledJob({
        type: 'scheduled-job',
        name: 'Scheduled Job',
        scheduleType: 'cron',
        scheduleValue: '* * * * *',
        enabled: true,
        nextRunAt: new Date(),
      });

      await storage.updateScheduledJob(jobId, {
        enabled: false,
      });

      const updated = await storage.getScheduledJob(jobId);
      expect(updated!.enabled).toBe(false);
    });

    test('should list scheduled jobs with filters', async () => {
      await storage.createScheduledJob({
        type: 'job-1',
        name: 'Job 1',
        scheduleType: 'cron',
        scheduleValue: '* * * * *',
        enabled: true,
        nextRunAt: new Date(2024, 0, 1),
      });
      await storage.createScheduledJob({
        type: 'job-2',
        name: 'Job 2',
        scheduleType: 'cron',
        scheduleValue: '* * * * *',
        enabled: false,
        nextRunAt: new Date(2024, 0, 2),
      });

      const enabledJobs = await storage.listScheduledJobs({ enabled: true });
      expect(enabledJobs).toHaveLength(1);
      expect(enabledJobs[0].type).toBe('job-1');
    });
  });

  describe('Dead Letter Jobs', () => {
    test('should create and list dead letter jobs', async () => {
      await storage.createDeadLetterJob({
        jobId: 'job-1',
        jobType: 'job-1',
        reason: 'test failure',
        failedAt: new Date(),
      });

      const deadLetterJobs = await storage.listDeadLetterJobs();
      expect(deadLetterJobs).toHaveLength(1);
      expect(deadLetterJobs[0].reason).toBe('test failure');
    });
  });

  describe('Locking Mechanism', () => {
    test('should acquire and release locks', async () => {
      const acquired = await storage.acquireLock('lock-1', 'owner-1', 1000);
      expect(acquired).toBe(true);

      const secondAttempt = await storage.acquireLock('lock-1', 'owner-2', 1000);
      expect(secondAttempt).toBe(false);

      await storage.releaseLock('lock-1', 'owner-1');
      const afterRelease = await storage.acquireLock('lock-1', 'owner-2', 1000);
      expect(afterRelease).toBe(true);
    });

    test('should extend lock for same owner', async () => {
      await storage.acquireLock('lock-1', 'owner-1', 1000);
      const extended = await storage.renewLock('lock-1', 'owner-1', 2000);
      expect(extended).toBe(true);
    });
  });

  describe('Cleanup', () => {
    test('should clean up old jobs and related data', async () => {
      // Set current time
      const now = new Date('2024-01-01T12:00:00Z');
      vi.setSystemTime(now);

      // Create a job (will use current time)
      const jobId = await storage.createJob({
        type: 'old-job',
        status: 'completed',
      });

      // Move time forward 31 days
      const futureDate = new Date('2024-02-01T12:00:00Z');
      vi.setSystemTime(futureDate);

      // Run cleanup with 30 days retention
      await storage.cleanup({
        jobRetention: 30,
        failedJobRetention: 30,
        deadLetterRetention: 30,
      });

      const job = await storage.getJob(jobId);
      expect(job).toBeNull();
    });
  });

  describe('Concurrency Handling', () => {
    test('should handle concurrent operations using transaction queue', async () => {
      const results: string[] = [];
      const operations = [
        storage.transaction(async () => {
          results.push('1');
          return '1';
        }),
        storage.transaction(async () => {
          results.push('2');
          return '2';
        }),
        storage.transaction(async () => {
          results.push('3');
          return '3';
        }),
      ];

      await Promise.all(operations);
      expect(results).toHaveLength(3);
      expect(results).toEqual(['1', '2', '3']);
    });
  });
});
