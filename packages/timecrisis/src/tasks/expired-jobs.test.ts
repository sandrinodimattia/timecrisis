import { afterEach } from 'node:test';
import { describe, it, expect, vi, beforeEach } from 'vitest';

import {
  resetEnvironment,
  prepareEnvironment,
  defaultValues,
  defaultJobDefinition,
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
    logger: new EmptyLogger(),
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
    jobs.set(defaultJobDefinition.type, defaultJobDefinition);

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
      jobs,
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
    expect(storage.listJobs).toHaveBeenCalledWith({
      status: ['pending', 'running'],
    });
    expect(storage.updateJob).not.toHaveBeenCalled();
  });

  it('should handle expired lock for job with retries remaining', async () => {
    const jobId = await storage.createJob({
      type: defaultJobDefinition.type,
      data: defaultJob.data,
      priority: 10,
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
      error: expect.stringMatching('lock expired'),
      errorStack: expect.stringMatching('lock expired'),
      executionDuration: 400000,
    });

    expect(storage.updateJob).toHaveBeenCalledWith(jobId, {
      status: 'pending',
      failCount: 1,
      failReason: null,
      runAt: expect.any(Date),
    });

    // Verify job was unlocked using the real storage
    const locks = await storage.listLocks({ worker: defaultValues.workerName });
    const jobLock = locks.find((lock) => formatLockName(jobId) === lock.lockId);
    expect(jobLock).toBeUndefined();
  });

  it('should handle expired lock for job with no retries remaining', async () => {
    const jobId = await storage.createJob({
      type: defaultJobDefinition.type,
      data: defaultJob.data,
      priority: 10,
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
      attempt: 2,
    });

    const runId = await storage.createJobRun({
      jobId,
      status: 'running',
      startedAt: now,
      attempt: 3,
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
      error: expect.stringMatching('lock expired'),
      errorStack: expect.stringMatching('lock expired'),
      executionDuration: 400000,
    });

    expect(storage.updateJob).toHaveBeenCalledWith(jobId, {
      status: 'failed',
      finishedAt: expect.any(Date),
      failReason: expect.stringMatching('lock expired at'),
      failCount: 2,
    });

    // Verify job was unlocked
    const locks = await storage.listLocks({ worker: defaultValues.workerName });
    const jobLock = locks.find((lock) => formatLockName(jobId) === lock.lockId);
    expect(jobLock).toBeUndefined();
  });

  it('should handle expired jobs', async () => {
    // Create an expired job
    const expiredJobId = await storage.createJob({
      type: defaultJobDefinition.type,
      data: defaultJob.data,
      priority: 10,
      status: 'pending',
      maxRetries: 1,
      backoffStrategy: 'exponential',
      failCount: 1,
      startedAt: now,
      expiresAt: now,
    });

    // Create a non-expired job
    await storage.createJob({
      type: defaultJobDefinition.type,
      data: defaultJob.data,
      priority: 10,
      status: 'pending',
      maxRetries: 1,
      backoffStrategy: 'exponential',
      failCount: 1,
      startedAt: now,
      expiresAt: tomorrow,
    });

    await vi.advanceTimersByTimeAsync(100);
    await task.execute();

    // Verify that only the expired job was updated
    expect(storage.updateJob).toHaveBeenCalledTimes(1);
    expect(storage.updateJob).toHaveBeenCalledWith(expiredJobId, {
      status: 'failed',
      failReason: expect.stringMatching('expired at'),
      finishedAt: expect.any(Date),
      failCount: 2,
    });

    // Verify that listJobs was called with the correct filter
    expect(storage.listJobs).toHaveBeenCalledWith({
      status: ['pending', 'running'],
    });
  });

  it('should skip non-expired jobs', async () => {
    const jobId = await storage.createJob({
      type: defaultJobDefinition.type,
      data: defaultJob.data,
      priority: 10,
      status: 'pending',
      maxRetries: 1,
      backoffStrategy: 'exponential',
      failCount: 1,
      startedAt: now,
      expiresAt: tomorrow, // Set expiration to tomorrow
    });

    await storage.acquireLock(
      formatLockName(jobId),
      defaultValues.workerName,
      defaultValues.jobLockTTL
    );

    // Mock listJobs to return empty array since job is not expired
    vi.mocked(storage.listJobs).mockResolvedValue([]);

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
      type: defaultJobDefinition.type,
      data: defaultJob.data,
      priority: 10,
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
      type: defaultJobDefinition.type,
      data: defaultJob.data,
      priority: 10,
      status: 'running',
      maxRetries: 3,
      backoffStrategy: 'exponential',
      failCount: 0,
      startedAt: now,
    });

    // Mock an error during job run listing
    vi.mocked(storage.listJobRuns).mockRejectedValueOnce(new Error('Database error'));

    await task.execute();

    expect(storage.updateJob).not.toHaveBeenCalled();
  });

  it('should process multiple jobs in a single execution', async () => {
    // Create two jobs - one with expired lock, one expired
    const job1Id = await storage.createJob({
      type: defaultJobDefinition.type,
      data: defaultJob.data,
      priority: 10,
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
      type: defaultJobDefinition.type,
      data: defaultJob.data,
      priority: 10,
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
      failReason: expect.stringMatching('expired at'),
      failCount: 1,
      finishedAt: new Date(),
    });

    // Both runs should be marked as failed
    expect(storage.updateJobRun).toHaveBeenCalledWith(run1Id, {
      status: 'failed',
      finishedAt: new Date(),
      executionDuration: 400000,
      error: expect.stringMatching('lock expired'),
      errorStack: expect.stringMatching('lock expired'),
    });
  });

  it('should handle jobs with both lock and job expiration', async () => {
    const jobId = await storage.createJob({
      type: defaultJobDefinition.type,
      data: defaultJob.data,
      priority: 10,
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
      error: expect.stringMatching('lock expired'),
      executionDuration: 400000,
      errorStack: expect.stringMatching('lock expired'),
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
      failReason: expect.stringMatching('expired at'),
    });
  });

  it('should still update the job even if no run exists', async () => {
    const jobId = await storage.createJob({
      type: defaultJobDefinition.type,
      data: defaultJob.data,
      priority: 10,
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
        type: defaultJobDefinition.type,
        data: defaultJob.data,
        priority: 10,
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
        error: expect.stringMatching('lock expired'),
        errorStack: expect.stringMatching('lock expired'),
        executionDuration: 398000,
      });

      // Verify job was unlocked
      const locks = await storage.listLocks({ worker: defaultValues.workerName });
      const jobLock = locks.find((lock) => formatLockName(jobId) === lock.lockId);
      expect(jobLock).toBeUndefined();
    });

    it('should fail running jobs that have not been touched in over an hour', async () => {
      const jobId = await storage.createJob({
        type: defaultJobDefinition.type,
        data: defaultJob.data,
        priority: 10,
        status: 'running',
        maxRetries: 3,
        backoffStrategy: 'exponential',
        failCount: 0,
        startedAt: now,
      });

      // Create a job run that was last touched more than an hour ago
      const runId = await storage.createJobRun({
        jobId,
        status: 'running',
        startedAt: now,
        touchedAt: new Date(now.getTime() - 61 * 60 * 1000), // 61 minutes ago
        attempt: 1,
      });

      await task.execute();

      // Should fail the job run
      expect(storage.updateJobRun).toHaveBeenCalledWith(runId, {
        status: 'failed',
        finishedAt: expect.any(Date),
        error: expect.stringMatching('has not been touched since'),
        errorStack: expect.stringMatching('has not been touched since'),
        executionDuration: expect.any(Number),
      });

      // Should fail the job
      expect(storage.updateJob).toHaveBeenCalledWith(jobId, {
        status: 'failed',
        failReason: expect.stringMatching('has not been touched since'),
        failCount: 1,
        finishedAt: expect.any(Date),
      });
    });

    it('should not fail running jobs that have been touched recently', async () => {
      const jobId = await storage.createJob({
        type: defaultJobDefinition.type,
        data: defaultJob.data,
        priority: 10,
        status: 'running',
        maxRetries: 3,
        backoffStrategy: 'exponential',
        failCount: 0,
        startedAt: now,
      });

      // Create a job run that was last touched less than an hour ago
      await storage.createJobRun({
        jobId,
        status: 'running',
        startedAt: now,
        touchedAt: new Date(now.getTime() - 30 * 60 * 1000), // 30 minutes ago
        attempt: 1,
      });

      await task.execute();

      // Should not update anything
      expect(storage.updateJobRun).not.toHaveBeenCalled();
      expect(storage.updateJob).not.toHaveBeenCalled();
    });

    it('should handle running jobs with no touchedAt timestamp', async () => {
      const jobId = await storage.createJob({
        type: defaultJobDefinition.type,
        data: defaultJob.data,
        priority: 10,
        status: 'running',
        maxRetries: 3,
        backoffStrategy: 'exponential',
        failCount: 0,
        startedAt: now,
      });

      // Create a job run without a touchedAt timestamp
      await storage.createJobRun({
        jobId,
        status: 'running',
        startedAt: now,
        attempt: 1,
      });

      await task.execute();

      // Should not update anything since we can't determine if it's stale
      expect(storage.updateJobRun).not.toHaveBeenCalled();
      expect(storage.updateJob).not.toHaveBeenCalled();
    });

    it('should handle leadership changes during execution', async () => {
      // Set up a job that would be processed
      const jobId = await storage.createJob({
        type: defaultJobDefinition.type,
        data: defaultJob.data,
        priority: 10,
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
        type: defaultJobDefinition.type,
        data: defaultJob.data,
        priority: 10,
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

  it('should handle expired running job based on job definition lockTTL', async () => {
    const customLockTTL = '5m'; // 5 minutes
    const customLockTTLinMs = 5 * 60 * 1000;

    const customJobDefinition: JobDefinition = {
      ...defaultJobDefinition,
      type: 'custom-lock-ttl-job',
      lockTTL: customLockTTL,
    };

    jobs.set(customJobDefinition.type, customJobDefinition);

    const jobId = await storage.createJob({
      type: customJobDefinition.type,
      data: defaultJob.data,
      priority: 10,
      status: 'running',
      maxRetries: 3,
      failCount: 0,
      startedAt: now,
    });

    await storage.createJobRun({
      jobId,
      status: 'running',
      startedAt: now,
      touchedAt: now, // touched right now
      attempt: 1,
    });

    // Advance time by more than lockTTL
    await vi.advanceTimersByTimeAsync(customLockTTLinMs + 1000);

    const failSpy = vi.spyOn(stateMachine, 'fail').mockResolvedValue(undefined);

    await task.execute();

    expect(failSpy).toHaveBeenCalledOnce();
    expect(failSpy).toHaveBeenCalledWith(
      expect.objectContaining({ id: jobId }),
      undefined,
      false,
      expect.any(Error),
      expect.stringContaining('has not been touched since'),
      expect.any(String)
    );
  });

  it('should handle expired running job based on default lockTTL', async () => {
    const defaultLockTTLinMs = 60 * 60 * 1000; // 1 hour default

    const jobId = await storage.createJob({
      type: defaultJobDefinition.type,
      data: defaultJob.data,
      priority: 10,
      status: 'running',
      maxRetries: 3,
      failCount: 0,
      startedAt: now,
    });

    await storage.createJobRun({
      jobId,
      status: 'running',
      startedAt: now,
      touchedAt: now, // touched right now
      attempt: 1,
    });

    // Advance time by more than lockTTL
    await vi.advanceTimersByTimeAsync(defaultLockTTLinMs + 1000);

    const failSpy = vi.spyOn(stateMachine, 'fail').mockResolvedValue(undefined);

    await task.execute();

    expect(failSpy).toHaveBeenCalledOnce();
    expect(failSpy).toHaveBeenCalledWith(
      expect.objectContaining({ id: jobId }),
      undefined,
      false,
      expect.any(Error),
      expect.stringContaining('has not been touched since'),
      expect.any(String)
    );
  });
});
