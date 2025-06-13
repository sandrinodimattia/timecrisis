import { describe, test, expect, beforeEach, vi, afterEach } from 'vitest';

import { Job, RegisterWorker } from '../schemas/index.js';
import { JobNotFoundError, WorkerNotFoundError } from '../types.js';
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
        entityId: 'ref-123',
        data: {},
        priority: 1,
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
      expect(job?.entityId).toBe(jobData.entityId);
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
        priority: 10,
        maxRetries: 3,
        backoffStrategy: 'exponential',
        failCount: 0,
        data: {},
      });
      await storage.createJob({
        type: 'job2',
        status: 'completed',
        priority: 10,
        maxRetries: 3,
        backoffStrategy: 'exponential',
        failCount: 0,
        data: {},
      });
      await storage.createJob({
        type: 'job3',
        status: 'running',
        priority: 10,
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
        priority: 10,
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
        priority: 10,
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
        priority: 10,
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
      const job1Id = await storage.createJob({
        type: 'job1',
        status: 'completed',
        priority: 10,
        maxRetries: 3,
        backoffStrategy: 'exponential',
        failCount: 0,
        data: {},
      });
      await storage.createJobRun({
        attempt: 1,
        jobId: job1Id,
        status: 'completed',
        startedAt: new Date(),
        executionDuration: 100,
      });
      const job2Id = await storage.createJob({
        type: 'job1',
        status: 'failed',
        priority: 10,
        maxRetries: 3,
        backoffStrategy: 'exponential',
        failCount: 1,
        data: {},
      });
      await storage.createJobRun({
        attempt: 1,
        jobId: job2Id,
        status: 'failed',
        startedAt: new Date(),
        executionDuration: 10,
      });
      const job3Id = await storage.createJob({
        type: 'job2',
        status: 'completed',
        priority: 10,
        maxRetries: 3,
        backoffStrategy: 'exponential',
        failCount: 0,
        data: {},
      });
      await storage.createJobRun({
        attempt: 1,
        jobId: job3Id,
        status: 'completed',
        startedAt: new Date(),
        executionDuration: 200,
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
        priority: 10,
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
        failReason: 'test failure',
        failedAt: new Date(),
      });

      const deadLetterJobs = await storage.listDeadLetterJobs();
      expect(deadLetterJobs).toHaveLength(1);
      expect(deadLetterJobs[0].failReason).toBe('test failure');
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
      expect(job).toBeUndefined();
    });
  });

  describe('Concurrency Handling', () => {
    test('should handle concurrent operations using transaction queue', async () => {
      const results: string[] = [];
      const operations = [
        storage.transaction(() => {
          results.push('1');
          return '1';
        }),
        storage.transaction(() => {
          results.push('2');
          return '2';
        }),
        storage.transaction(() => {
          results.push('3');
          return '3';
        }),
      ];

      await Promise.all(operations);
      expect(results).toHaveLength(3);
      expect(results).toEqual(['1', '2', '3']);
    });
  });

  describe('Worker Management', () => {
    test('should register and retrieve a worker', async () => {
      const workerData: RegisterWorker = {
        name: 'test-worker',
      };

      const workerName = await storage.registerWorker(workerData);
      expect(workerName).toBeDefined();

      const worker = await storage.getWorker(workerName);
      expect(worker).toMatchObject({
        name: 'test-worker',
        first_seen: now,
        last_heartbeat: now,
      });
    });

    test('should return null for non-existent worker', async () => {
      const worker = await storage.getWorker('non-existent');
      expect(worker).toBeNull();
    });

    test('should update worker heartbeat', async () => {
      const workerName = await storage.registerWorker({ name: 'test-worker' });
      const newHeartbeat = new Date('2025-01-23T01:00:00.000Z');

      await storage.updateWorkerHeartbeat(workerName, { last_heartbeat: newHeartbeat });

      const worker = await storage.getWorker(workerName);
      expect(worker?.last_heartbeat).toEqual(newHeartbeat);
    });

    test('should throw WorkerNotFoundError when updating non-existent worker', async () => {
      await expect(
        storage.updateWorkerHeartbeat('non-existent', { last_heartbeat: now })
      ).rejects.toThrow(WorkerNotFoundError);
    });

    test('should get inactive workers', async () => {
      // Create active worker
      const activeWorkerId = await storage.registerWorker({ name: 'active-worker' });
      await storage.updateWorkerHeartbeat(activeWorkerId, { last_heartbeat: now });

      // Create inactive worker
      const inactiveWorkerId = await storage.registerWorker({ name: 'inactive-worker' });
      const oldDate = new Date('2025-01-22T23:00:00.000Z'); // 1 hour ago
      await storage.updateWorkerHeartbeat(inactiveWorkerId, { last_heartbeat: oldDate });

      const inactiveWorkers = await storage.getInactiveWorkers(
        new Date('2025-01-22T23:30:00.000Z')
      ); // 30 minutes ago
      expect(inactiveWorkers).toHaveLength(1);
      expect(inactiveWorkers[0].name).toBe(inactiveWorkerId);
    });

    test('should get all workers', async () => {
      await storage.registerWorker({ name: 'worker-1' });
      await storage.registerWorker({ name: 'worker-2' });

      const workers = await storage.getWorkers();
      expect(workers).toHaveLength(2);
      expect(workers.map((w) => w.name).sort()).toEqual(['worker-1', 'worker-2']);
    });

    test('should clear workers on close', async () => {
      await storage.registerWorker({ name: 'worker-1' });
      await storage.close();

      const workers = await storage.getWorkers();
      expect(workers).toHaveLength(0);
    });

    test('should validate worker registration data', async () => {
      // @ts-expect-error Testing invalid data
      await expect(storage.registerWorker({})).rejects.toThrow();
      // @ts-expect-error Testing invalid data
      await expect(storage.registerWorker({ name: 123 })).rejects.toThrow();
    });

    test('should validate worker heartbeat data', async () => {
      const workerName = await storage.registerWorker({ name: 'test-worker' });

      // @ts-expect-error Testing invalid data
      await expect(storage.updateWorkerHeartbeat(workerName, {})).rejects.toThrow();
      await expect(
        // @ts-expect-error Testing invalid data
        storage.updateWorkerHeartbeat(workerName, { last_heartbeat: 'invalid' })
      ).rejects.toThrow();
    });

    describe('workers', () => {
      test('should register and retrieve a worker', async () => {
        const workerName = await storage.registerWorker({
          name: 'test-worker',
        });

        const worker = await storage.getWorker(workerName);
        expect(worker).toBeDefined();
        expect(worker?.name).toBe('test-worker');
      });

      test('should delete a worker', async () => {
        // Register a worker
        const workerName = await storage.registerWorker({
          name: 'test-worker',
        });

        // Verify it exists
        const worker = await storage.getWorker(workerName);
        expect(worker).toBeDefined();

        // Delete the worker
        await storage.deleteWorker(workerName);

        // Verify it's gone
        await expect(storage.getWorker(workerName)).resolves.toBeNull();
      });

      test('should throw WorkerNotFoundError when deleting non-existent worker', async () => {
        await expect(storage.deleteWorker('non-existent')).rejects.toThrow(WorkerNotFoundError);
      });

      test('should update worker heartbeat', async () => {
        const workerName = await storage.registerWorker({ name: 'test-worker' });
        const newHeartbeat = new Date('2025-01-23T01:00:00.000Z');

        await storage.updateWorkerHeartbeat(workerName, { last_heartbeat: newHeartbeat });

        const worker = await storage.getWorker(workerName);
        expect(worker?.last_heartbeat).toEqual(newHeartbeat);
      });
    });
  });

  describe('concurrency slots', () => {
    test('should acquire and release job type slots correctly', async () => {
      const jobType = 'test-job';
      const workerName = 'worker-1';
      const maxConcurrent = 2;

      // Should be able to acquire slots up to maxConcurrent
      expect(await storage.acquireTypeSlot(jobType, workerName, maxConcurrent)).toBe(true);
      expect(await storage.acquireTypeSlot(jobType, workerName, maxConcurrent)).toBe(true);
      expect(await storage.acquireTypeSlot(jobType, workerName, maxConcurrent)).toBe(false);

      // Running count should match acquired slots
      expect(await storage.getRunningCount(jobType)).toBe(2);

      // Release one slot
      await storage.releaseTypeSlot(jobType, workerName);
      expect(await storage.getRunningCount(jobType)).toBe(1);

      // Should be able to acquire another slot
      expect(await storage.acquireTypeSlot(jobType, workerName, maxConcurrent)).toBe(true);
    });

    test('should handle multiple workers correctly', async () => {
      const jobType = 'test-job';
      const worker1 = 'worker-1';
      const worker2 = 'worker-2';
      const maxConcurrent = 3;

      // Both workers should be able to acquire slots
      expect(await storage.acquireTypeSlot(jobType, worker1, maxConcurrent)).toBe(true);
      expect(await storage.acquireTypeSlot(jobType, worker2, maxConcurrent)).toBe(true);
      expect(await storage.acquireTypeSlot(jobType, worker1, maxConcurrent)).toBe(true);

      // Should hit the limit across workers
      expect(await storage.acquireTypeSlot(jobType, worker2, maxConcurrent)).toBe(false);

      // Release slots for worker1
      await storage.releaseTypeSlot(jobType, worker1);
      expect(await storage.getRunningCount(jobType)).toBe(2);

      // Worker2 should now be able to acquire a slot
      expect(await storage.acquireTypeSlot(jobType, worker2, maxConcurrent)).toBe(true);
    });

    test('should release all slots for a worker', async () => {
      const jobType1 = 'test-job-1';
      const jobType2 = 'test-job-2';
      const worker1 = 'worker-1';
      const worker2 = 'worker-2';
      const maxConcurrent = 5;

      // Acquire slots for both job types and workers
      await storage.acquireTypeSlot(jobType1, worker1, maxConcurrent);
      await storage.acquireTypeSlot(jobType1, worker2, maxConcurrent);
      await storage.acquireTypeSlot(jobType2, worker1, maxConcurrent);
      await storage.acquireTypeSlot(jobType2, worker2, maxConcurrent);

      // Release all slots for worker1
      await storage.releaseAllTypeSlots(worker1);

      // Check counts
      expect(await storage.getRunningCount(jobType1)).toBe(1); // worker2's slot
      expect(await storage.getRunningCount(jobType2)).toBe(1); // worker2's slot
      expect(await storage.getRunningCount()).toBe(2); // total across all types
    });

    test('should handle total running count correctly', async () => {
      const jobType1 = 'test-job-1';
      const jobType2 = 'test-job-2';
      const worker1 = 'worker-1';
      const maxConcurrent = 5;

      await storage.acquireTypeSlot(jobType1, worker1, maxConcurrent);
      await storage.acquireTypeSlot(jobType2, worker1, maxConcurrent);
      await storage.acquireTypeSlot(jobType2, worker1, maxConcurrent);

      expect(await storage.getRunningCount()).toBe(3);
      expect(await storage.getRunningCount(jobType1)).toBe(1);
      expect(await storage.getRunningCount(jobType2)).toBe(2);
    });
  });
});
