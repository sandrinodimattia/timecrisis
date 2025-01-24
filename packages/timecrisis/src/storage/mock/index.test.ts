import { describe, it, expect, beforeEach } from 'vitest';

import { MockJobStorage } from './index';

describe('MockJobStorage', () => {
  let storage: MockJobStorage;

  beforeEach(() => {
    storage = new MockJobStorage();
  });

  describe('Job Management', () => {
    it('should create and retrieve a job', async () => {
      const jobId = await storage.createJob({
        type: 'test-job',
        data: { foo: 'bar' },
      });

      const job = await storage.getJob(jobId);
      expect(job).toBeDefined();
      expect(job?.type).toBe('test-job');
      expect(job?.data).toEqual({ foo: 'bar' });
      expect(job?.status).toBe('pending');
      expect(job?.createdAt).toBeInstanceOf(Date);
      expect(job?.updatedAt).toBeInstanceOf(Date);
    });

    it('should set default values when creating a job with minimal fields', async () => {
      const jobId = await storage.createJob({
        type: 'minimal-job',
        data: { test: true },
      });

      const job = await storage.getJob(jobId);
      expect(job).toBeDefined();
      expect(job?.priority).toBe(1);
      expect(job?.status).toBe('pending');
      expect(job?.attempts).toBe(0);
      expect(job?.maxRetries).toBe(0);
      expect(job?.backoffStrategy).toBe('exponential');
      expect(job?.failCount).toBe(0);
      expect(job?.expiresAt).toBeUndefined();
      expect(job?.lockedAt).toBeUndefined();
      expect(job?.startedAt).toBeUndefined();
      expect(job?.finishedAt).toBeUndefined();
      expect(job?.failReason).toBeUndefined();
      expect(job?.executionDuration).toBeUndefined();
    });

    it('should list jobs with filtering', async () => {
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

    it('should filter jobs by runAtBefore', async () => {
      const now = new Date('2024-01-01T12:00:00Z');

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

    it('should handle job updates', async () => {
      const jobId = await storage.createJob({
        type: 'test-job',
        data: { foo: 'bar' },
      });

      await storage.updateJob(jobId, {
        status: 'completed',
        executionDuration: 1000,
        finishedAt: new Date(),
      });

      const job = await storage.getJob(jobId);
      expect(job?.status).toBe('completed');
      expect(job?.executionDuration).toBe(1000);
      expect(job?.finishedAt).toBeInstanceOf(Date);
    });

    it('should throw error when updating non-existent job', async () => {
      await expect(storage.updateJob('non-existent', { status: 'completed' })).rejects.toThrow();
    });

    it('should cleanup all data', async () => {
      await storage.createJob({ type: 'job1' });
      await storage.createJob({ type: 'job2' });
      await storage.cleanup();

      const jobs = await storage.listJobs();
      expect(jobs).toHaveLength(0);
    });
  });

  describe('Metrics', () => {
    it('should calculate correct metrics', async () => {
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

  describe('Job Runs', () => {
    it('should create and update job runs', async () => {
      const jobId = await storage.createJob({ type: 'test-job' });
      const runId = await storage.createJobRun({
        jobId,
        status: 'running',
        startedAt: new Date(),
        attempt: 1,
      });

      const runs = await storage.listJobRuns(jobId);
      expect(runs).toHaveLength(1);
      expect(runs[0].status).toBe('running');

      await storage.updateJobRun(runId, { status: 'completed' });
      const updatedRuns = await storage.listJobRuns(jobId);
      expect(updatedRuns[0].status).toBe('completed');
    });

    it('should throw error when updating non-existent run', async () => {
      await expect(storage.updateJobRun('non-existent', { status: 'completed' })).rejects.toThrow();
    });

    it('should track run attempts and duration', async () => {
      const jobId = await storage.createJob({ type: 'test-job' });
      const runId = await storage.createJobRun({
        jobId,
        status: 'running',
        startedAt: new Date(),
        attempt: 1,
      });

      // Update with completion and duration
      const finishedAt = new Date();
      await storage.updateJobRun(runId, {
        status: 'completed',
        finishedAt,
      });

      const runs = await storage.listJobRuns(jobId);
      expect(runs[0].status).toBe('completed');
      expect(runs[0].attempt).toBe(1);
      expect(runs[0].finishedAt).toBeInstanceOf(Date);
    });
  });

  describe('Job Logs', () => {
    it('should create and list job logs', async () => {
      const jobId = await storage.createJob({ type: 'test-job' });
      await storage.createJobLog({
        jobId,
        level: 'info',
        message: 'Test message',
        timestamp: new Date(),
      });
      await storage.createJobLog({
        jobId,
        level: 'error',
        message: 'Error message',
        timestamp: new Date(),
      });

      const logs = await storage.listJobLogs(jobId);
      expect(logs).toHaveLength(2);
      expect(logs[0].message).toBe('Test message');
      expect(logs[1].level).toBe('error');
    });
  });

  describe('Scheduled Jobs', () => {
    it('should create and update scheduled jobs', async () => {
      const jobId = await storage.createScheduledJob({
        type: 'scheduled-job',
        name: 'Test Scheduled Job',
        scheduleType: 'cron',
        scheduleValue: '0 * * * *',
        enabled: true,
      });

      const job = await storage.getScheduledJob(jobId);
      expect(job).toBeDefined();
      expect(job!.type).toBe('scheduled-job');
      expect(job!.scheduleValue).toBe('0 * * * *');
      expect(job!.enabled).toBe(true);

      await storage.updateScheduledJob(job!.id, {
        enabled: false,
      });
      const updatedJob = await storage.getScheduledJob(job!.id);
      expect(updatedJob!.enabled).toBe(false);

      const jobs = await storage.listScheduledJobs({ enabled: false });
      expect(jobs).toHaveLength(1);
      expect(jobs[0].id).toBe(job!.id);
    });

    it('should set default values when creating a scheduled job with minimal fields', async () => {
      const jobId = await storage.createScheduledJob({
        name: 'Test Schedule',
        type: 'test-job',
        scheduleType: 'cron',
        scheduleValue: '0 * * * *',
      });

      const job = await storage.getScheduledJob(jobId);
      expect(job).toBeDefined();
      expect(job?.name).toBe('Test Schedule');
      expect(job?.type).toBe('test-job');
      expect(job?.scheduleValue).toBe('0 * * * *');
      // Check defaults
      expect(job?.scheduleType).toBe('cron');
      expect(job?.data).toEqual({});
      expect(job?.enabled).toBe(true);
      expect(job?.lastScheduledAt).toBeNull();
      expect(job?.nextRunAt).toBeNull();
      expect(job?.createdAt).toBeInstanceOf(Date);
      expect(job?.updatedAt).toBeInstanceOf(Date);
    });
  });

  describe('Lock Management', () => {
    it('should acquire and release locks', async () => {
      const acquired = await storage.acquireLock('test-lock', 'owner1', 1000);
      expect(acquired).toBe(true);

      const acquiredAgain = await storage.acquireLock('test-lock', 'owner2', 1000);
      expect(acquiredAgain).toBe(false);

      const released = await storage.releaseLock('test-lock', 'owner1');
      expect(released).toBe(true);

      const acquiredAfterRelease = await storage.acquireLock('test-lock', 'owner2', 1000);
      expect(acquiredAfterRelease).toBe(true);
    });

    it('should handle lock failures', async () => {
      storage.setOptions({ shouldFailAcquire: true });
      await expect(storage.acquireLock('test-lock', 'owner1', 1000)).rejects.toThrow();

      storage.setOptions({ shouldFailRelease: true });
      await expect(storage.releaseLock('test-lock', 'owner1')).rejects.toThrow();
    });

    it('should handle lock expiration', async () => {
      const now = Date.now();
      storage.setLock('test-lock', 'owner1', now - 1000); // Expired lock

      const acquired = await storage.acquireLock('test-lock', 'owner2', 1000);
      expect(acquired).toBe(true);
    });

    it('should renew locks', async () => {
      await storage.acquireLock('test-lock', 'owner1', 1000);

      const renewed = await storage.renewLock('test-lock', 'owner1', 2000);
      expect(renewed).toBe(true);

      // Different owner cannot renew
      const renewedByOther = await storage.renewLock('test-lock', 'owner2', 2000);
      expect(renewedByOther).toBe(false);
    });

    it('should simulate other leader', async () => {
      await storage.acquireLock('test-lock', 'owner1', 1000);
      await storage.simulateOtherLeader('test-lock', 2000);

      // Original owner cannot renew
      const renewed = await storage.renewLock('test-lock', 'owner1', 1000);
      expect(renewed).toBe(false);
    });
  });

  describe('Dead Letter Jobs', () => {
    it('should create and list dead letter jobs', async () => {
      const jobId = await storage.createJob({ type: 'test-job' });
      await storage.createDeadLetterJob({
        jobId,
        jobType: 'test-job',
        failedAt: new Date(),
        reason: 'Test failure',
        data: { foo: 'bar' },
      });

      const deadLetterJobs = await storage.listDeadLetterJobs();
      expect(deadLetterJobs).toHaveLength(1);
      expect(deadLetterJobs[0].jobId).toBe(jobId);
      expect(deadLetterJobs[0].reason).toBe('Test failure');
      expect(deadLetterJobs[0].data).toEqual({ foo: 'bar' });
    });
  });
});
