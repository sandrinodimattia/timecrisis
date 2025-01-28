import { afterEach } from 'node:test';
import { describe, it, expect, vi, beforeEach } from 'vitest';

import {
  resetEnvironment,
  prepareEnvironment,
  defaultValues,
  defaultJobSchema,
  defaultJob,
  now,
  tomorrow,
} from '../test-helpers/defaults.js';
import { EmptyLogger } from '../logger/index.js';
import { ExpiredJobsTask } from './expired-jobs.js';
import { JobDefinition } from '../scheduler/types.js';
import { MockJobStorage } from '../storage/mock/index.js';
import { JobStateMachine } from '../state-machine/index.js';
import { formatLockName } from '../concurrency/job-lock.js';
import { LeaderElection } from '../concurrency/leader-election.js';

// Mock leader election
const createMockLeaderElection = (isLeader: boolean = true): LeaderElection => {
  const leaderElection = new LeaderElection({
    storage: new MockJobStorage(),
    node: defaultValues.workerName,
    lockTTL: defaultValues.distributedLockTTL,
  });

  // Mock the methods
  vi.spyOn(leaderElection, 'isCurrentLeader').mockReturnValue(isLeader);
  vi.spyOn(leaderElection, 'start').mockResolvedValue(undefined);
  vi.spyOn(leaderElection, 'stop').mockResolvedValue(undefined);
  vi.spyOn(leaderElection as never, 'tryBecomeLeader').mockResolvedValue(isLeader);
  vi.spyOn(leaderElection as never, 'releaseLeadership').mockResolvedValue(undefined);

  return leaderElection;
};

describe('ExpiredJobsTask', () => {
  let storage: MockJobStorage;
  let leaderElection: LeaderElection;
  let task: ExpiredJobsTask;
  let stateMachine: JobStateMachine;
  let jobs: Map<string, JobDefinition>;

  beforeEach(() => {
    prepareEnvironment();

    jobs = new Map();
    jobs.set(defaultJobSchema.type, defaultJobSchema);

    storage = new MockJobStorage();
    leaderElection = createMockLeaderElection();

    stateMachine = new JobStateMachine({
      jobs,
      storage,
      logger: new EmptyLogger(),
    });
    vi.spyOn(stateMachine, 'enqueue');

    task = new ExpiredJobsTask({
      storage,
      leaderElection,
      stateMachine,
      logger: new EmptyLogger(),
      jobLockTTL: defaultValues.jobLockTTL,
      pollInterval: defaultValues.pollInterval,
    });
  });

  afterEach(() => {
    resetEnvironment();
  });

  it('should skip processing if not leader', async () => {
    leaderElection.isCurrentLeader = vi.fn().mockReturnValue(false);
    await task.execute();
    expect(storage.listJobs).not.toHaveBeenCalled();
  });

  it('should process no jobs if none are running', async () => {
    vi.mocked(storage.listJobs).mockResolvedValue([]);
    await task.execute();
    expect(storage.listJobs).toHaveBeenCalledWith({ status: ['pending'] });
    expect(storage.updateJob).not.toHaveBeenCalled();
  });

  it('should handle expired lock for job with retries remaining', async () => {
    const jobId = await storage.createJob({
      type: defaultJobSchema.type,
      data: defaultJob.data,
      priority: 0,
      status: 'running',
      maxRetries: 3,
      backoffStrategy: 'exponential',
      failCount: 0,
      startedAt: now,
    });

    const runId = await storage.createJobRun({
      jobId,
      status: 'running',
      startedAt: now,
      attempt: 1,
    });

    await storage.acquireLock(
      formatLockName(jobId),
      defaultValues.workerName,
      defaultValues.jobLockTTL
    );

    await vi.advanceTimersByTimeAsync(400000);
    await task.execute();

    expect(storage.updateJobRun).toHaveBeenCalledWith(runId, {
      status: 'failed',
      finishedAt: new Date(),
      error: 'Job lock expired (expiresAt=2025-01-01T09:02:00.000Z)',
      error_stack: undefined,
      executionDuration: 400000,
    });

    expect(storage.updateJob).toHaveBeenCalledWith(jobId, {
      status: 'pending',
      failCount: 1,
      failReason: null,
      runAt: expect.any(Date),
    });

    // Verify job was unlocked
    const locks = await storage.listLocks({ worker: defaultValues.workerName });
    const jobLock = locks.find((lock) => formatLockName(jobId) === lock.lockId);
    expect(jobLock).toBeUndefined();
  });

  it('should handle expired lock for job with no retries remaining', async () => {
    const jobId = await storage.createJob({
      type: defaultJobSchema.type,
      data: defaultJob.data,
      priority: 0,
      status: 'running',
      maxRetries: 1,
      backoffStrategy: 'exponential',
      failCount: 1,
      startedAt: now,
    });

    await storage.createJobRun({
      jobId,
      status: 'failed',
      startedAt: now,
      attempt: 1,
    });

    const runId = await storage.createJobRun({
      jobId,
      status: 'running',
      startedAt: now,
      attempt: 2,
    });

    await storage.acquireLock(
      formatLockName(jobId),
      defaultValues.workerName,
      defaultValues.jobLockTTL
    );

    await vi.advanceTimersByTimeAsync(400000);
    await task.execute();

    expect(storage.updateJobRun).toHaveBeenCalledWith(runId, {
      status: 'failed',
      finishedAt: new Date(),
      error: 'Job lock expired (expiresAt=2025-01-01T09:02:00.000Z)',
      error_stack: undefined,
      executionDuration: 400000,
    });

    expect(storage.updateJob).toHaveBeenCalledWith(jobId, {
      status: 'failed',
      finishedAt: expect.any(Date),
      failReason: 'Job lock expired (expiresAt=2025-01-01T09:02:00.000Z)',
      failCount: 2,
    });

    // Verify job was unlocked
    const locks = await storage.listLocks({ worker: defaultValues.workerName });
    const jobLock = locks.find((lock) => formatLockName(jobId) === lock.lockId);
    expect(jobLock).toBeUndefined();
  });

  it('should handle expired jobs', async () => {
    const jobId = await storage.createJob({
      type: defaultJobSchema.type,
      data: defaultJob.data,
      priority: 0,
      status: 'pending',
      maxRetries: 1,
      backoffStrategy: 'exponential',
      failCount: 1,
      startedAt: now,
      expiresAt: now,
    });

    await vi.advanceTimersByTimeAsync(100);
    await task.execute();

    expect(storage.updateJob).toHaveBeenCalledWith(jobId, {
      status: 'failed',
      failReason: expect.stringMatching('Job expired'),
      finishedAt: expect.any(Date),
      failCount: 2,
    });
  });

  it('should skip non-expired jobs', async () => {
    const jobId = await storage.createJob({
      type: defaultJobSchema.type,
      data: defaultJob.data,
      priority: 0,
      status: 'pending',
      maxRetries: 1,
      backoffStrategy: 'exponential',
      failCount: 1,
      startedAt: now,
      expiresAt: tomorrow,
    });

    await storage.acquireLock(
      formatLockName(jobId),
      defaultValues.workerName,
      defaultValues.jobLockTTL
    );

    await vi.advanceTimersByTimeAsync(100);
    await task.execute();

    expect(storage.updateJob).not.toHaveBeenCalled();
    expect(storage.updateJobRun).not.toHaveBeenCalled();

    // Verify job was untouched.
    const locks = await storage.listLocks({ worker: defaultValues.workerName });
    const jobLock = locks.find((lock) => formatLockName(jobId) === lock.lockId);
    expect(jobLock?.lockId).toBe(formatLockName(jobId));
  });

  it('should handle jobs with no current run', async () => {
    const jobId = await storage.createJob({
      type: defaultJobSchema.type,
      data: defaultJob.data,
      priority: 0,
      status: 'running',
      maxRetries: 3,
      backoffStrategy: 'exponential',
      failCount: 0,
      startedAt: now,
    });

    await storage.acquireLock(
      formatLockName(jobId),
      defaultValues.workerName,
      defaultValues.jobLockTTL
    );

    await vi.advanceTimersByTimeAsync(400000);
    await task.execute();

    expect(storage.updateJobRun).not.toHaveBeenCalled();
    expect(storage.updateJob).toHaveBeenCalledWith(jobId, {
      status: 'pending',
      failReason: null,
      failCount: 1,
      runAt: expect.any(Date),
    });

    // Verify job was unlocked
    const locks = await storage.listLocks({ worker: defaultValues.workerName });
    const jobLock = locks.find((lock) => formatLockName(jobId) === lock.lockId);
    expect(jobLock).toBeUndefined();
  });

  it('should handle storage errors gracefully', async () => {
    const error = new Error('Storage error');
    vi.mocked(storage.listJobs).mockRejectedValue(error);

    // The error should propagate up
    await expect(task.execute()).rejects.toThrow('Storage error');
  });

  it('should handle errors during job processing', async () => {
    await storage.createJob({
      type: defaultJobSchema.type,
      data: defaultJob.data,
      priority: 0,
      status: 'running',
      maxRetries: 3,
      backoffStrategy: 'exponential',
      failCount: 0,
      startedAt: now,
    });

    // Mock an error during job run listing
    vi.mocked(storage.listJobRuns).mockRejectedValueOnce(new Error('Database error'));

    // Spy on console.error
    await task.execute();

    expect(storage.updateJob).not.toHaveBeenCalled();
  });

  it('should process multiple jobs in a single execution', async () => {
    // Create two jobs - one with expired lock, one expired
    const job1Id = await storage.createJob({
      type: defaultJobSchema.type,
      data: defaultJob.data,
      priority: 0,
      status: 'running',
      maxRetries: 3,
      backoffStrategy: 'exponential',
      failCount: 0,
      startedAt: now,
    });

    const run1Id = await storage.createJobRun({
      jobId: job1Id,
      status: 'running',
      startedAt: now,
      attempt: 1,
    });

    await storage.acquireLock(
      formatLockName(job1Id),
      defaultValues.workerName,
      defaultValues.jobLockTTL
    );

    const job2Id = await storage.createJob({
      type: defaultJobSchema.type,
      data: defaultJob.data,
      priority: 0,
      status: 'pending',
      maxRetries: 3,
      backoffStrategy: 'exponential',
      failCount: 0,
      startedAt: now,
      expiresAt: now,
    });

    await vi.advanceTimersByTimeAsync(400000);
    await task.execute();

    // First job should be reset for retry due to lock expiration
    expect(storage.updateJob).toHaveBeenCalledWith(job1Id, {
      status: 'pending',
      failCount: 1,
      failReason: null,
      runAt: expect.any(Date),
    });

    // Verify job was unlocked
    const locks = await storage.listLocks({ worker: defaultValues.workerName });
    const jobLock = locks.find((lock) => formatLockName(job1Id) === lock.lockId);
    expect(jobLock).toBeUndefined();

    // Second job should be failed due to expiration
    expect(storage.updateJob).toHaveBeenCalledWith(job2Id, {
      status: 'failed',
      failReason: expect.stringMatching('Job expired'),
      failCount: 1,
      finishedAt: new Date(),
    });

    // Both runs should be marked as failed
    expect(storage.updateJobRun).toHaveBeenCalledWith(run1Id, {
      status: 'failed',
      finishedAt: new Date(),
      executionDuration: 400000,
      error: expect.stringMatching('Job lock expired'),
      error_stack: undefined,
    });
  });

  it('should handle jobs with both lock and job expiration', async () => {
    const jobId = await storage.createJob({
      type: defaultJobSchema.type,
      data: defaultJob.data,
      priority: 0,
      status: 'running',
      maxRetries: 3,
      backoffStrategy: 'exponential',
      failCount: 0,
      startedAt: now,
      expiresAt: now,
    });

    const runId = await storage.createJobRun({
      jobId,
      status: 'running',
      startedAt: now,
      attempt: 1,
    });

    await storage.acquireLock(
      formatLockName(jobId),
      defaultValues.workerName,
      defaultValues.jobLockTTL
    );

    await vi.advanceTimersByTimeAsync(400000);
    await task.execute();

    // Lock expiration should take precedence since it's checked first
    expect(storage.updateJob).toHaveBeenCalledWith(jobId, {
      status: 'pending',
      failCount: 1,
      failReason: null,
      runAt: expect.any(Date),
    });

    expect(storage.updateJobRun).toHaveBeenCalledWith(runId, {
      status: 'failed',
      finishedAt: new Date(),
      error: expect.stringMatching('Job lock expired'),
      executionDuration: 400000,
      errorStack: undefined,
    });

    // Verify job was unlocked
    const locks = await storage.listLocks({ worker: defaultValues.workerName });
    const jobLock = locks.find((lock) => formatLockName(jobId) === lock.lockId);
    expect(jobLock).toBeUndefined();

    // Now the expiration
    expect(storage.updateJob).toHaveBeenCalledWith(jobId, {
      status: 'failed',
      failCount: 2,
      finishedAt: new Date(),
      failReason: expect.stringMatching('Job expired'),
    });
  });

  it('should still update the job even if no run exists', async () => {
    const jobId = await storage.createJob({
      type: defaultJobSchema.type,
      data: defaultJob.data,
      priority: 0,
      status: 'running',
      maxRetries: 3,
      backoffStrategy: 'exponential',
      failCount: 0,
      startedAt: now,
    });

    await storage.acquireLock(
      formatLockName(jobId),
      defaultValues.workerName,
      defaultValues.jobLockTTL
    );

    await vi.advanceTimersByTimeAsync(400000);
    await task.execute();

    // Verify job was unlocked
    const locks = await storage.listLocks({ worker: defaultValues.workerName });
    const jobLock = locks.find((lock) => formatLockName(jobId) === lock.lockId);
    expect(jobLock).toBeUndefined();

    // Should still update the job even if no run exists
    expect(storage.updateJob).toHaveBeenCalledWith(jobId, {
      status: 'pending',
      failCount: 1,
      failReason: null,
      runAt: expect.any(Date),
    });
  });

  describe('start/stop functionality', () => {
    afterEach(() => {
      task.stop();
    });

    it('should execute immediately on start', async () => {
      const executeSpy = vi.spyOn(task, 'execute');
      await task.start();
      expect(executeSpy).toHaveBeenCalledTimes(1);
    });

    it('should execute periodically after start', async () => {
      const executeSpy = vi.spyOn(task, 'execute');
      await task.start();

      // First execution happens immediately
      expect(executeSpy).toHaveBeenCalledTimes(1);

      // Advance timer by cleanup interval
      await vi.advanceTimersByTimeAsync(defaultValues.pollInterval);
      expect(executeSpy).toHaveBeenCalledTimes(2);

      // Advance timer again
      await vi.advanceTimersByTimeAsync(defaultValues.pollInterval);
      expect(executeSpy).toHaveBeenCalledTimes(3);
    });

    it('should stop executing after stop is called', async () => {
      const executeSpy = vi.spyOn(task, 'execute');
      await task.start();

      // First execution happens immediately
      expect(executeSpy).toHaveBeenCalledTimes(1);

      // Advance timer by cleanup interval
      await vi.advanceTimersByTimeAsync(defaultValues.pollInterval);
      expect(executeSpy).toHaveBeenCalledTimes(2);

      // Stop the task
      task.stop();

      // Advance timer again
      await vi.advanceTimersByTimeAsync(defaultValues.pollInterval);
      expect(executeSpy).toHaveBeenCalledTimes(2); // Should not increase
    });

    it('should handle errors during periodic execution', async () => {
      const error = new Error('Test error');
      const executeSpy = vi.spyOn(task, 'execute').mockRejectedValueOnce(error);
      const loggerSpy = vi.spyOn(task['cfg'].logger, 'error');

      // Start the task - this will trigger the first execution
      await task.start();

      // Verify error was logged
      expect(executeSpy).toHaveBeenCalledTimes(1);
      expect(loggerSpy).toHaveBeenCalledWith(
        'Failed to execute expired jobs check',
        expect.objectContaining({
          error: 'Test error',
          error_stack: expect.any(String),
        })
      );

      // Reset the mock to not throw on next execution
      executeSpy.mockResolvedValueOnce(undefined);

      // Should continue executing after error
      await vi.advanceTimersByTimeAsync(defaultValues.pollInterval);
      expect(executeSpy).toHaveBeenCalledTimes(2);
    });
  });

  describe('edge cases', () => {
    it('should handle multiple job runs with only one running', async () => {
      const jobId = await storage.createJob({
        type: defaultJobSchema.type,
        data: defaultJob.data,
        priority: 0,
        status: 'running',
        maxRetries: 3,
        backoffStrategy: 'exponential',
        failCount: 0,
        startedAt: now,
      });

      await storage.acquireLock(
        formatLockName(jobId),
        defaultValues.workerName,
        defaultValues.jobLockTTL
      );

      // Create multiple runs, only one running
      await storage.createJobRun({
        jobId,
        status: 'completed',
        startedAt: now,
        finishedAt: new Date(now.getTime() + 1000),
        attempt: 1,
      });

      const runId = await storage.createJobRun({
        jobId,
        status: 'running',
        startedAt: new Date(now.getTime() + 2000),
        attempt: 1,
      });

      await vi.advanceTimersByTimeAsync(400000);
      await task.execute();

      // Should only update the running job run
      expect(storage.updateJobRun).toHaveBeenCalledTimes(1);
      expect(storage.updateJobRun).toHaveBeenCalledWith(runId, {
        status: 'failed',
        finishedAt: new Date(),
        error: expect.stringMatching('Job lock expired'),
        error_stack: undefined,
        executionDuration: 398000,
      });

      // Verify job was unlocked
      const locks = await storage.listLocks({ worker: defaultValues.workerName });
      const jobLock = locks.find((lock) => formatLockName(jobId) === lock.lockId);
      expect(jobLock).toBeUndefined();
    });

    it('should handle leadership changes during execution', async () => {
      // Set up a job that would be processed
      const jobId = await storage.createJob({
        type: defaultJobSchema.type,
        data: defaultJob.data,
        priority: 0,
        status: 'running',
        maxRetries: 3,
        backoffStrategy: 'exponential',
        failCount: 0,
        startedAt: now,
      });

      await storage.acquireLock(
        formatLockName(jobId),
        defaultValues.workerName,
        defaultValues.jobLockTTL
      );

      // Mock leadership change after job listing
      let isLeader = true;
      vi.spyOn(leaderElection, 'isCurrentLeader').mockImplementation(() => {
        isLeader = !isLeader; // Toggle leadership
        return isLeader;
      });

      await vi.advanceTimersByTimeAsync(400000);
      await task.start();

      // Should not process any jobs after leadership is lost
      expect(storage.updateJob).not.toHaveBeenCalledWith(jobId);
      expect(storage.updateJobRun).not.toHaveBeenCalled();
    });

    it('should handle errors during job updates', async () => {
      await storage.createJob({
        type: defaultJobSchema.type,
        data: defaultJob.data,
        priority: 0,
        status: 'running',
        maxRetries: 3,
        backoffStrategy: 'exponential',
        failCount: 0,
        startedAt: now,
      });

      // Mock updateJob to fail
      storage.updateJob = vi.fn().mockRejectedValueOnce(new Error('Update failed'));

      await task.execute();
    });
  });
});
