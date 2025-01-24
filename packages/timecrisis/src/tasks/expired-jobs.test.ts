import { afterEach } from 'node:test';
import { describe, it, expect, vi, beforeEach } from 'vitest';

import { EmptyLogger } from '../logger';
import { LeaderElection } from '../leader';
import { MockJobStorage } from '../storage/mock';
import { ExpiredJobsTask } from './expired-jobs';

// Mock leader election
const createMockLeaderElection = (isLeader: boolean = true): LeaderElection => {
  const leaderElection = new LeaderElection({
    storage: new MockJobStorage(),
    node: 'test-node',
    lockTTL: 30000,
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
  const now = new Date('2025-01-23T00:00:00.000Z');

  beforeEach(() => {
    vi.useFakeTimers();
    vi.setSystemTime(now);

    storage = new MockJobStorage();
    leaderElection = createMockLeaderElection();
    task = new ExpiredJobsTask(storage, leaderElection, new EmptyLogger(), {
      lockLifetime: 300000,
    });
  });

  afterEach(() => {
    // Clear all timers and mocks
    vi.clearAllTimers();
    vi.clearAllMocks();
    vi.useRealTimers();
  });

  it('should skip processing if not leader', async () => {
    leaderElection.isCurrentLeader = vi.fn().mockReturnValue(false);
    await task.execute();
    expect(storage.listJobs).not.toHaveBeenCalled();
  });

  it('should process no jobs if none are running', async () => {
    vi.mocked(storage.listJobs).mockResolvedValue([]);
    await task.execute();
    expect(storage.listJobs).toHaveBeenCalledWith({ status: ['pending', 'running'] });
    expect(storage.updateJob).not.toHaveBeenCalled();
  });

  it('should handle expired lock for job with retries remaining', async () => {
    const jobId = await storage.createJob({
      type: 'test',
      data: {},
      priority: 0,
      status: 'running',
      attempts: 0,
      maxRetries: 3,
      backoffStrategy: 'exponential',
      failCount: 0,
      lockedAt: new Date(now.getTime() - 400000),
    });

    const runId = await storage.createJobRun({
      jobId,
      status: 'running',
      startedAt: new Date(now.getTime() - 400000),
      attempt: 1,
    });

    await task.execute();

    expect(storage.updateJobRun).toHaveBeenCalledWith(runId, {
      status: 'failed',
      finishedAt: now,
      error: 'Job lock expired',
    });

    expect(storage.updateJob).toHaveBeenCalledWith(jobId, {
      status: 'pending',
      lockedAt: null,
      failCount: 1,
      failReason: 'Job lock expired',
    });
  });

  it('should handle expired lock for job with no retries remaining', async () => {
    const jobId = await storage.createJob({
      type: 'test',
      data: {},
      priority: 0,
      status: 'running',
      attempts: 3,
      maxRetries: 3,
      backoffStrategy: 'exponential',
      failCount: 2,
      lockedAt: new Date(now.getTime() - 400000),
    });

    const runId = await storage.createJobRun({
      jobId,
      status: 'running',
      startedAt: new Date(now.getTime() - 400000),
      attempt: 3,
    });

    await task.execute();

    expect(storage.updateJobRun).toHaveBeenCalledWith(runId, {
      status: 'failed',
      finishedAt: now,
      error: 'Job lock expired',
    });

    expect(storage.updateJob).toHaveBeenCalledWith(jobId, {
      status: 'failed',
      failReason: 'Job lock expired',
      failCount: 3,
      lockedAt: null,
    });
  });

  it('should handle expired jobs', async () => {
    const jobId = await storage.createJob({
      type: 'test',
      data: {},
      priority: 0,
      status: 'running',
      attempts: 0,
      maxRetries: 3,
      backoffStrategy: 'exponential',
      failCount: 0,
      lockedAt: new Date(now.getTime() - 100000), // Locked 100s ago (not expired)
      expiresAt: new Date(now.getTime() - 1000), // Expired 1s ago
    });

    const runId = await storage.createJobRun({
      jobId,
      status: 'running',
      startedAt: new Date(now.getTime() - 100000),
      attempt: 1,
    });

    await task.execute();

    expect(storage.updateJobRun).toHaveBeenCalledWith(runId, {
      status: 'failed',
      finishedAt: now,
      error: 'Job expired',
    });

    expect(storage.updateJob).toHaveBeenCalledWith(jobId, {
      status: 'failed',
      failReason: 'Job expired',
      failCount: 1,
      lockedAt: null,
    });
  });

  it('should skip non-expired jobs', async () => {
    await storage.createJob({
      type: 'test',
      data: {},
      priority: 0,
      status: 'running',
      attempts: 0,
      maxRetries: 3,
      backoffStrategy: 'exponential',
      failCount: 0,
      lockedAt: new Date(now.getTime() - 100000), // Locked 100s ago (not expired)
      expiresAt: new Date(now.getTime() + 1000), // Expires in 1s
    });

    await task.execute();

    expect(storage.updateJob).not.toHaveBeenCalled();
    expect(storage.updateJobRun).not.toHaveBeenCalled();
  });

  it('should handle jobs with no current run', async () => {
    const jobId = await storage.createJob({
      type: 'test',
      data: {},
      priority: 0,
      status: 'running',
      attempts: 0,
      maxRetries: 3,
      backoffStrategy: 'exponential',
      failCount: 0,
      lockedAt: new Date(now.getTime() - 400000), // Locked 400s ago (expired)
    });

    await task.execute();

    expect(storage.updateJobRun).not.toHaveBeenCalled();
    expect(storage.updateJob).toHaveBeenCalledWith(jobId, {
      status: 'pending',
      failReason: 'Job lock expired',
      failCount: 1,
      lockedAt: null,
    });
  });

  it('should handle storage errors gracefully', async () => {
    const error = new Error('Storage error');
    vi.mocked(storage.listJobs).mockRejectedValue(error);

    // The error should propagate up
    await expect(task.execute()).rejects.toThrow('Storage error');
  });

  it('should handle errors during job processing', async () => {
    await storage.createJob({
      type: 'test',
      data: {},
      priority: 0,
      status: 'running',
      attempts: 0,
      maxRetries: 3,
      backoffStrategy: 'exponential',
      failCount: 0,
      lockedAt: new Date(now.getTime() - 400000),
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
      type: 'test',
      data: {},
      priority: 0,
      status: 'running',
      attempts: 0,
      maxRetries: 3,
      backoffStrategy: 'exponential',
      failCount: 0,
      lockedAt: new Date(now.getTime() - 400000), // Lock expired
    });

    const job2Id = await storage.createJob({
      type: 'test',
      data: {},
      priority: 0,
      status: 'running',
      attempts: 0,
      maxRetries: 3,
      backoffStrategy: 'exponential',
      failCount: 0,
      lockedAt: new Date(now.getTime() - 100000), // Lock not expired
      expiresAt: new Date(now.getTime() - 1000), // Job expired
    });

    const run1Id = await storage.createJobRun({
      jobId: job1Id,
      status: 'running',
      startedAt: new Date(now.getTime() - 400000),
      attempt: 1,
    });

    const run2Id = await storage.createJobRun({
      jobId: job2Id,
      status: 'running',
      startedAt: new Date(now.getTime() - 100000),
      attempt: 1,
    });

    await task.execute();

    // First job should be reset for retry due to lock expiration
    expect(storage.updateJob).toHaveBeenCalledWith(job1Id, {
      status: 'pending',
      lockedAt: null,
      failCount: 1,
      failReason: 'Job lock expired',
    });

    // Second job should be failed due to expiration
    expect(storage.updateJob).toHaveBeenCalledWith(job2Id, {
      status: 'failed',
      failReason: 'Job expired',
      failCount: 1,
      lockedAt: null,
    });

    // Both runs should be marked as failed
    expect(storage.updateJobRun).toHaveBeenCalledWith(run1Id, {
      status: 'failed',
      finishedAt: now,
      error: 'Job lock expired',
    });

    expect(storage.updateJobRun).toHaveBeenCalledWith(run2Id, {
      status: 'failed',
      finishedAt: now,
      error: 'Job expired',
    });
  });

  it('should handle jobs with both lock and job expiration', async () => {
    const jobId = await storage.createJob({
      type: 'test',
      data: {},
      priority: 0,
      status: 'running',
      attempts: 0,
      maxRetries: 3,
      backoffStrategy: 'exponential',
      failCount: 0,
      lockedAt: new Date(now.getTime() - 400000), // Lock expired
      expiresAt: new Date(now.getTime() - 1000), // Job expired
    });

    const runId = await storage.createJobRun({
      jobId,
      status: 'running',
      startedAt: new Date(now.getTime() - 400000),
      attempt: 1,
    });

    await task.execute();

    // Lock expiration should take precedence since it's checked first
    expect(storage.updateJob).toHaveBeenCalledWith(jobId, {
      status: 'pending',
      lockedAt: null,
      failCount: 1,
      failReason: 'Job lock expired',
    });

    expect(storage.updateJobRun).toHaveBeenCalledWith(runId, {
      status: 'failed',
      finishedAt: now,
      error: 'Job lock expired',
    });
  });
});
