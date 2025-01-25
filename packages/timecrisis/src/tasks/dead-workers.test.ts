import { describe, it, expect, vi, beforeEach, afterEach } from 'vitest';

import { EmptyLogger } from '../logger/index.js';
import { DeadWorkersTask } from './dead-workers.js';
import { LeaderElection } from '../leader/index.js';
import { MockJobStorage } from '../storage/mock/index.js';

describe('DeadWorkersTask', () => {
  let storage: MockJobStorage;
  let task: DeadWorkersTask;
  let leader: LeaderElection;
  const now = new Date('2025-01-23T00:00:00.000Z');

  beforeEach(() => {
    storage = new MockJobStorage();
    leader = new LeaderElection({
      node: 'test-leader',
      storage,
      lockTTL: 1000,
    });

    // Mock leader election to return true by default
    vi.spyOn(leader, 'isCurrentLeader').mockReturnValue(true);

    task = new DeadWorkersTask({
      storage,
      leaderElection: leader,
      logger: new EmptyLogger(),
      pollInterval: 100,
      workerDeadTimeout: 30000,
    });

    vi.useFakeTimers();
    vi.setSystemTime(now);
  });

  afterEach(() => {
    task.stop();
    vi.useRealTimers();
    vi.restoreAllMocks();
  });

  it('should start and stop task', async () => {
    await task.start();
    expect(task['timer']).toBeDefined();

    task.stop();
    expect(task['timer']).toBeNull();
  });

  it('should execute and remove inactive workers', async () => {
    // Register an inactive worker
    const workerId = await storage.registerWorker({
      name: 'inactive-worker',
    });

    // Update heartbeat to old timestamp
    const oldDate = new Date('2025-01-22T23:00:00.000Z'); // 1 hour ago
    await storage.updateWorkerHeartbeat(workerId, {
      last_heartbeat: oldDate,
    });

    // Execute task
    await task.execute();

    // Verify worker was removed
    const worker = await storage.getWorker(workerId);
    expect(worker).toBeNull();
  });

  it('should not remove active workers', async () => {
    // Register an active worker
    const workerId = await storage.registerWorker({
      name: 'active-worker',
    });

    // Update heartbeat to recent timestamp
    await storage.updateWorkerHeartbeat(workerId, {
      last_heartbeat: new Date(),
    });

    // Execute task
    await task.execute();

    // Verify worker still exists
    const worker = await storage.getWorker(workerId);
    expect(worker).toBeDefined();
  });

  it('should check for inactive workers at intervals', async () => {
    // Register an inactive worker
    const workerId = await storage.registerWorker({
      name: 'inactive-worker',
    });

    // Update heartbeat to old timestamp
    const oldDate = new Date('2025-01-22T23:00:00.000Z'); // 1 hour ago
    await storage.updateWorkerHeartbeat(workerId, {
      last_heartbeat: oldDate,
    });

    // Start task and advance timer
    await task.start();
    await vi.advanceTimersByTimeAsync(100);

    // Verify worker was removed
    const worker = await storage.getWorker(workerId);
    expect(worker).toBeNull();
  });

  it('should only execute when leader', async () => {
    // Mock leader election to return false for this test
    vi.spyOn(leader, 'isCurrentLeader').mockReturnValue(false);

    // Register an inactive worker
    const workerId = await storage.registerWorker({
      name: 'inactive-worker',
    });

    // Update heartbeat to old timestamp
    const oldDate = new Date('2025-01-22T23:00:00.000Z'); // 1 hour ago
    await storage.updateWorkerHeartbeat(workerId, {
      last_heartbeat: oldDate,
    });

    // Execute task
    await task.execute();

    // Verify worker still exists (task didn't run because not leader)
    const worker = await storage.getWorker(workerId);
    expect(worker).toBeDefined();
  });

  it('should handle storage errors gracefully', async () => {
    const mockError = new Error('Storage error');

    // Create a storage that fails on getInactiveWorkers
    const failingStorage = new MockJobStorage();
    vi.spyOn(failingStorage, 'getInactiveWorkers').mockRejectedValue(mockError);

    const failingTask = new DeadWorkersTask({
      storage: failingStorage, // Use the failing storage here
      leaderElection: leader,
      logger: new EmptyLogger(),
      pollInterval: 100,
      workerDeadTimeout: 30000,
    });

    await failingTask.start();
    await vi.advanceTimersByTimeAsync(100);

    failingTask.stop();
  });

  it('should cleanup jobs locked by inactive workers', async () => {
    // Register an inactive worker
    const workerId = await storage.registerWorker({
      name: 'inactive-worker',
    });

    // Create some jobs and lock them with this worker
    const jobId1 = await storage.createJob({
      type: 'test',
      data: {},
      runAt: new Date(),
      maxRetries: 3,
      attempts: 0,
      status: 'pending',
    });

    const jobId2 = await storage.createJob({
      type: 'test',
      data: {},
      runAt: new Date(),
      maxRetries: 3,
      attempts: 0,
      status: 'pending',
    });

    // Lock both jobs with the worker
    await storage.updateJob(jobId1, {
      lockedAt: new Date(),
      lockedBy: workerId,
    });

    await storage.updateJob(jobId2, {
      lockedAt: new Date(),
      lockedBy: workerId,
    });

    // Update heartbeat to old timestamp
    const oldDate = new Date('2025-01-22T23:00:00.000Z'); // 1 hour ago
    await storage.updateWorkerHeartbeat(workerId, {
      last_heartbeat: oldDate,
    });

    // Execute task
    await task.execute();

    // Verify worker was removed
    const worker = await storage.getWorker(workerId);
    expect(worker).toBeNull();

    // Verify jobs were unlocked
    const job1 = await storage.getJob(jobId1);
    expect(job1?.lockedAt).toBeNull();
    expect(job1?.lockedBy).toBeNull();

    const job2 = await storage.getJob(jobId2);
    expect(job2?.lockedAt).toBeNull();
    expect(job2?.lockedBy).toBeNull();
  });

  it('should not cleanup jobs locked by active workers', async () => {
    // Register an active worker
    const workerId = await storage.registerWorker({
      name: 'active-worker',
    });

    // Create a job and lock it with this worker
    const jobId = await storage.createJob({
      type: 'test',
      data: {},
      runAt: new Date(),
      maxRetries: 3,
      attempts: 0,
      status: 'pending',
    });

    // Lock the job with the worker
    const lockTime = new Date();
    await storage.updateJob(jobId, {
      lockedAt: lockTime,
      lockedBy: workerId,
    });

    // Update heartbeat to recent timestamp
    await storage.updateWorkerHeartbeat(workerId, {
      last_heartbeat: new Date(),
    });

    // Execute task
    await task.execute();

    // Verify worker still exists
    const worker = await storage.getWorker(workerId);
    expect(worker).toBeDefined();

    // Verify job is still locked
    const job = await storage.getJob(jobId);
    expect(job?.lockedAt).toEqual(lockTime);
    expect(job?.lockedBy).toBe(workerId);
  });

  it('should handle jobs with retries remaining when worker becomes inactive', async () => {
    // Register an inactive worker
    const workerId = await storage.registerWorker({
      name: 'inactive-worker',
    });

    // Create a job with retries remaining
    const jobId = await storage.createJob({
      type: 'test',
      data: {},
      status: 'running',
      attempts: 1,
      maxRetries: 3,
      failCount: 0,
      lockedAt: new Date(now.getTime() - 400000),
      lockedBy: workerId,
    });

    // Create a running job run
    await storage.createJobRun({
      jobId,
      status: 'running',
      startedAt: new Date(now.getTime() - 400000),
      attempt: 1,
    });

    // Update heartbeat to old timestamp
    const oldDate = new Date('2025-01-22T23:00:00.000Z');
    await storage.updateWorkerHeartbeat(workerId, {
      last_heartbeat: oldDate,
    });

    // Execute task
    await task.execute();

    // Verify worker was removed
    const worker = await storage.getWorker(workerId);
    expect(worker).toBeNull();

    // Verify job run was marked as failed
    const jobRuns = await storage.listJobRuns(jobId);
    expect(jobRuns.length).toBe(1);
    const jobRun = jobRuns[0];
    expect(jobRun?.status).toBe('failed');
    expect(jobRun?.error).toBe('Worker which was running the task is no longer active');
    expect(jobRun?.finishedAt).toEqual(now);

    // Verify job was reset for retry
    const job = await storage.getJob(jobId);
    expect(job?.status).toBe('pending');
    expect(job?.lockedAt).toBeNull();
    expect(job?.lockedBy).toBeNull();
    expect(job?.failCount).toBe(1);
    expect(job?.failReason).toBe('Worker which was running the task is no longer active');
  });

  it('should handle jobs with no retries remaining when worker becomes inactive', async () => {
    // Register an inactive worker
    const workerId = await storage.registerWorker({
      name: 'inactive-worker',
    });

    // Create a job with no retries remaining
    const jobId = await storage.createJob({
      type: 'test',
      data: {},
      status: 'running',
      attempts: 3,
      maxRetries: 3,
      failCount: 2,
      lockedAt: new Date(now.getTime() - 400000),
      lockedBy: workerId,
    });

    // Create a running job run
    await storage.createJobRun({
      jobId,
      status: 'running',
      startedAt: new Date(now.getTime() - 400000),
      attempt: 3,
    });

    // Update heartbeat to old timestamp
    const oldDate = new Date('2025-01-22T23:00:00.000Z');
    await storage.updateWorkerHeartbeat(workerId, {
      last_heartbeat: oldDate,
    });

    // Execute task
    await task.execute();

    // Verify worker was removed
    const worker = await storage.getWorker(workerId);
    expect(worker).toBeNull();

    // Verify job run was marked as failed
    const jobRuns = await storage.listJobRuns(jobId);
    expect(jobRuns.length).toBe(1);
    const jobRun = jobRuns[0];
    expect(jobRun?.status).toBe('failed');
    expect(jobRun?.error).toBe('Worker which was running the task is no longer active');
    expect(jobRun?.finishedAt).toEqual(now);

    // Verify job was marked as failed
    const job = await storage.getJob(jobId);
    expect(job?.status).toBe('failed');
    expect(job?.lockedAt).toBeNull();
    expect(job?.lockedBy).toBeNull();
    expect(job?.failCount).toBe(3);
    expect(job?.failReason).toBe('Worker which was running the task is no longer active');
  });

  it('should not affect jobs with no current run when worker becomes inactive', async () => {
    // Register an inactive worker
    const workerId = await storage.registerWorker({
      name: 'inactive-worker',
    });

    // Create a job locked by the worker but with no current run
    const jobId = await storage.createJob({
      type: 'test',
      data: {},
      status: 'running',
      attempts: 1,
      maxRetries: 3,
      failCount: 0,
      lockedAt: new Date(now.getTime() - 400000),
      lockedBy: workerId,
    });

    // Update heartbeat to old timestamp
    const oldDate = new Date('2025-01-22T23:00:00.000Z');
    await storage.updateWorkerHeartbeat(workerId, {
      last_heartbeat: oldDate,
    });

    // Execute task
    await task.execute();

    // Verify worker was removed
    const worker = await storage.getWorker(workerId);
    expect(worker).toBeNull();

    // Verify job was reset for retry
    const job = await storage.getJob(jobId);
    expect(job?.status).toBe('pending');
    expect(job?.lockedAt).toBeNull();
    expect(job?.lockedBy).toBeNull();
    expect(job?.failCount).toBe(1);
    expect(job?.failReason).toBe('Worker which was running the task is no longer active');
  });

  it('should cleanup concurrency slots when worker becomes inactive', async () => {
    // Register an inactive worker
    const worker = await storage.registerWorker({
      name: 'inactive-worker',
    });

    // Acquire some job type slots for the worker
    await storage.acquireJobTypeSlot('job-type-1', worker, 5);
    await storage.acquireJobTypeSlot('job-type-1', worker, 5);
    await storage.acquireJobTypeSlot('job-type-2', worker, 5);

    // Verify slots are acquired
    expect(await storage.getRunningCount('job-type-1')).toBe(2);
    expect(await storage.getRunningCount('job-type-2')).toBe(1);
    expect(await storage.getRunningCount()).toBe(3);

    // Update heartbeat to old timestamp
    const oldDate = new Date('2025-01-22T23:00:00.000Z'); // 1 hour ago
    await storage.updateWorkerHeartbeat(worker, {
      last_heartbeat: oldDate,
    });

    // Execute task
    await task.execute();

    // Verify worker was removed
    const workerAfter = await storage.getWorker(worker);
    expect(workerAfter).toBeNull();

    // Verify all slots were released
    expect(await storage.getRunningCount('job-type-1')).toBe(0);
    expect(await storage.getRunningCount('job-type-2')).toBe(0);
    expect(await storage.getRunningCount()).toBe(0);
  });

  it('should cleanup concurrency slots for multiple inactive workers', async () => {
    // Register two workers
    const worker1 = await storage.registerWorker({ name: 'worker-1' });
    const worker2 = await storage.registerWorker({ name: 'worker-2' });

    // Acquire slots for both workers
    await storage.acquireJobTypeSlot('job-type-1', worker1, 5);
    await storage.acquireJobTypeSlot('job-type-1', worker2, 5);
    await storage.acquireJobTypeSlot('job-type-2', worker1, 5);
    await storage.acquireJobTypeSlot('job-type-2', worker2, 5);

    // Verify initial slot counts
    expect(await storage.getRunningCount('job-type-1')).toBe(2);
    expect(await storage.getRunningCount('job-type-2')).toBe(2);
    expect(await storage.getRunningCount()).toBe(4);

    // Make both workers inactive
    const oldDate = new Date('2025-01-22T23:00:00.000Z');
    await storage.updateWorkerHeartbeat(worker1, { last_heartbeat: oldDate });
    await storage.updateWorkerHeartbeat(worker2, { last_heartbeat: oldDate });

    // Execute task
    await task.execute();

    // Verify all slots were released
    expect(await storage.getRunningCount('job-type-1')).toBe(0);
    expect(await storage.getRunningCount('job-type-2')).toBe(0);
    expect(await storage.getRunningCount()).toBe(0);

    // Verify workers were removed
    expect(await storage.getWorker(worker1)).toBeNull();
    expect(await storage.getWorker(worker2)).toBeNull();
  });
});
