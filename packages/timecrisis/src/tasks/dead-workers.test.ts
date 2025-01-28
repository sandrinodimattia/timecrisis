import { describe, it, expect, vi, beforeEach, afterEach } from 'vitest';

import {
  defaultJob,
  defaultJobDefinition,
  defaultValues,
  lastWeek,
  now,
  prepareEnvironment,
  resetEnvironment,
} from '../test-helpers/defaults.js';
import { EmptyLogger } from '../logger/index.js';
import { DeadWorkersTask } from './dead-workers.js';
import { JobDefinition } from '../scheduler/types.js';
import { MockJobStorage } from '../storage/mock/index.js';
import { formatLockName } from '../concurrency/job-lock.js';
import { JobStateMachine } from '../state-machine/index.js';
import { LeaderElection } from '../concurrency/leader-election.js';

describe('DeadWorkersTask', () => {
  let task: DeadWorkersTask;
  let leader: LeaderElection;
  let storage: MockJobStorage;
  let jobs: Map<string, JobDefinition>;

  beforeEach(() => {
    prepareEnvironment();

    jobs = new Map();
    jobs.set('test', defaultJobDefinition);
    leader = new LeaderElection({
      storage,
      node: defaultValues.workerName,
      lockTTL: defaultValues.distributedLockTTL,
    });
    storage = new MockJobStorage();

    // Mock leader election to return true by default
    vi.spyOn(leader, 'isCurrentLeader').mockReturnValue(true);

    task = new DeadWorkersTask({
      storage,
      stateMachine: new JobStateMachine({
        jobs,
        storage,
        logger: new EmptyLogger(),
      }),
      leaderElection: leader,
      logger: new EmptyLogger(),
      pollInterval: defaultValues.pollInterval,
      workerDeadTimeout: defaultValues.workerDeadTimeout,
    });
  });

  afterEach(() => {
    task.stop();
    resetEnvironment();
  });

  it('should start and stop task', async () => {
    await task.start();
    expect(task['timer']).toBeDefined();

    task.stop();
    expect(task['timer']).toBeNull();
  });

  it('should execute and remove inactive workers', async () => {
    // Register an inactive worker
    const workerName = await storage.registerWorker({
      name: 'inactive-worker',
    });

    // Update heartbeat to old timestamp // I week ago
    await storage.updateWorkerHeartbeat(workerName, {
      last_heartbeat: lastWeek,
    });

    // Execute task
    await task.execute();

    // Verify worker was removed
    const worker = await storage.getWorker(workerName);
    expect(worker).toBeNull();
  });

  it('should not remove active workers', async () => {
    // Register an active worker
    const workerName = await storage.registerWorker({
      name: 'active-worker',
    });

    // Update heartbeat to recent timestamp
    await storage.updateWorkerHeartbeat(workerName, {
      last_heartbeat: now,
    });

    // Execute task
    await task.execute();

    // Verify worker still exists
    const worker = await storage.getWorker(workerName);
    expect(worker).toBeDefined();
  });

  it('should check for inactive workers at intervals', async () => {
    // Register an inactive worker
    const workerName = await storage.registerWorker({
      name: 'inactive-worker',
    });

    // Start task and advance timer
    await task.start();

    // Update heartbeat to old timestamp
    await storage.updateWorkerHeartbeat(workerName, {
      last_heartbeat: lastWeek,
    });

    await vi.advanceTimersByTimeAsync(defaultValues.pollInterval);

    // Verify worker was removed
    const workerDead = await storage.getWorker(workerName);
    expect(workerDead).toBeNull();
  });

  it('should only execute when leader', async () => {
    // Mock leader election to return false for this test
    vi.spyOn(leader, 'isCurrentLeader').mockReturnValue(false);

    // Register an inactive worker
    const workerName = await storage.registerWorker({
      name: 'inactive-worker',
    });

    // Update heartbeat to old timestamp
    await storage.updateWorkerHeartbeat(workerName, {
      last_heartbeat: lastWeek,
    });

    // Execute task
    await task.execute();

    // Verify worker still exists (task didn't run because not leader)
    const worker = await storage.getWorker(workerName);
    expect(worker).toBeDefined();
  });

  it('should handle storage errors gracefully', async () => {
    const mockError = new Error('Storage error');

    // Create a storage that fails on getInactiveWorkers
    const failingStorage = new MockJobStorage();
    vi.spyOn(failingStorage, 'getInactiveWorkers').mockRejectedValue(mockError);

    const failingTask = new DeadWorkersTask({
      stateMachine: new JobStateMachine({
        jobs,
        storage: failingStorage, // Use the failing storage here
        logger: new EmptyLogger(),
      }),
      storage: failingStorage,
      leaderElection: leader,
      logger: new EmptyLogger(),
      pollInterval: defaultValues.pollInterval,
      workerDeadTimeout: defaultValues.workerDeadTimeout,
    });

    await failingTask.start();
    await vi.advanceTimersByTimeAsync(defaultValues.pollInterval);

    failingTask.stop();
  });

  it('should cleanup jobs locked by inactive workers', async () => {
    // Register an inactive worker
    const workerName = await storage.registerWorker({
      name: 'inactive-worker',
    });

    // Create some jobs and lock them with this worker
    const jobId1 = await storage.createJob({
      ...defaultJob,
    });

    const jobId2 = await storage.createJob({
      ...defaultJob,
    });

    // Lock both jobs with the worker
    await storage.acquireLock(formatLockName(jobId1), workerName, defaultValues.jobLockTTL);
    await storage.acquireLock(formatLockName(jobId2), workerName, defaultValues.jobLockTTL);

    // Update heartbeat to old timestamp
    await storage.updateWorkerHeartbeat(workerName, {
      last_heartbeat: lastWeek,
    });

    // Execute task
    await task.execute();

    // Verify worker was removed
    const worker = await storage.getWorker(workerName);
    expect(worker).toBeNull();

    // Verify jobs were unlocked
    const locks = await storage.listLocks({ worker: workerName });
    const job1Lock = locks.find((lock) => formatLockName(jobId1) === lock.lockId);
    expect(job1Lock).toBeUndefined();

    const job2Lock = locks.find((lock) => formatLockName(jobId2) === lock.lockId);
    expect(job2Lock).toBeUndefined();
  });

  it('should not cleanup jobs locked by active workers', async () => {
    // Register an active worker
    const workerName = await storage.registerWorker({
      name: 'active-worker',
    });

    // Create a job and lock it with this worker
    const jobId = await storage.createJob({
      type: 'test',
      data: {},
      runAt: now,
      maxRetries: 3,
      status: 'pending',
    });

    // Lock the job with the worker
    await storage.acquireLock(formatLockName(jobId), workerName, defaultValues.jobLockTTL);

    // Update heartbeat to recent timestamp
    await storage.updateWorkerHeartbeat(workerName, {
      last_heartbeat: now,
    });

    // Execute task
    await task.execute();

    // Verify worker still exists
    const worker = await storage.getWorker(workerName);
    expect(worker).toBeDefined();

    // Verify job was still locked
    const locks = await storage.listLocks({ worker: workerName });
    const jobLock = locks.find((lock) => formatLockName(jobId) === lock.lockId);
    expect(jobLock!.worker).toBe('active-worker');
  });

  it('should handle jobs with retries remaining when worker becomes inactive', async () => {
    // Register an inactive worker
    const workerName = await storage.registerWorker({
      name: 'inactive-worker',
    });

    // Create a job with retries remaining
    const jobId = await storage.createJob({
      type: 'test',
      data: {},
      status: 'running',
      maxRetries: 3,
      failCount: 0,
    });

    // Create a running job run
    await storage.createJobRun({
      jobId,
      status: 'running',
      startedAt: now,
      attempt: 1,
    });

    // Lock the job with the worker
    await storage.acquireLock(formatLockName(jobId), workerName, defaultValues.jobLockTTL);

    // Update heartbeat to old timestamp
    await storage.updateWorkerHeartbeat(workerName, {
      last_heartbeat: lastWeek,
    });

    // Execute task
    await task.execute();

    // Verify worker was removed
    const worker = await storage.getWorker(workerName);
    expect(worker).toBeNull();

    // Verify job run was marked as failed
    const jobRuns = await storage.listJobRuns(jobId);
    expect(jobRuns.length).toBe(1);
    const jobRun = jobRuns[0];
    expect(jobRun?.status).toBe('failed');
    expect(jobRun?.error).toBe('Worker which was running the task is no longer active');
    expect(jobRun?.finishedAt).toEqual(now);

    // Verify job was unlocked
    const locks = await storage.listLocks({ worker: workerName });
    const jobLock = locks.find((lock) => formatLockName(jobId) === lock.lockId);
    expect(jobLock).toBeUndefined();

    // Verify job was reset for retry
    const job = await storage.getJob(jobId);
    expect(job?.status).toBe('pending');
    expect(job?.failCount).toBe(1);
  });

  it('should handle jobs with no retries remaining when worker becomes inactive', async () => {
    // Register an inactive worker
    const workerName = await storage.registerWorker({
      name: 'inactive-worker',
    });

    // Create a job with no retries remaining
    const jobId = await storage.createJob({
      type: 'test',
      data: {},
      status: 'running',
      maxRetries: 2,
      failCount: 2,
    });

    // Create all jobs.
    await storage.createJobRun({
      jobId,
      status: 'failed',
      startedAt: now,
      attempt: 1,
    });
    await storage.createJobRun({
      jobId,
      status: 'failed',
      startedAt: now,
      attempt: 2,
    });
    await storage.createJobRun({
      jobId,
      status: 'running',
      startedAt: now,
      attempt: 3,
    });

    // Lock the job with the worker
    await storage.acquireLock(formatLockName(jobId), workerName, defaultValues.jobLockTTL);

    // Update heartbeat to old timestamp
    const oldDate = new Date(lastWeek);
    await storage.updateWorkerHeartbeat(workerName, {
      last_heartbeat: oldDate,
    });

    // Execute task
    await task.execute();

    // Verify worker was removed
    const worker = await storage.getWorker(workerName);
    expect(worker).toBeNull();

    // Verify job run was marked as failed
    const jobRuns = await storage.listJobRuns(jobId);
    const jobRun = jobRuns[2];
    expect(jobRun?.status).toBe('failed');
    expect(jobRun?.error).toBe('Worker which was running the task is no longer active');
    expect(jobRun?.finishedAt).toEqual(now);

    // Verify job was unlocked
    const locks = await storage.listLocks({ worker: workerName });
    const jobLock = locks.find((lock) => formatLockName(jobId) === lock.lockId);
    expect(jobLock).toBeUndefined();

    // Verify job was marked as failed
    const job = await storage.getJob(jobId);
    expect(job?.status).toBe('failed');
    expect(job?.failCount).toBe(3);
    expect(job?.failReason).toBe('Worker which was running the task is no longer active');
  });

  it('should not affect jobs with no current run when worker becomes inactive', async () => {
    // Register an inactive worker
    const workerName = await storage.registerWorker({
      name: 'inactive-worker',
    });

    // Create a job locked by the worker but with no current run
    const jobId = await storage.createJob({
      type: 'test',
      data: {},
      status: 'pending',
      maxRetries: 3,
      failCount: 1,
    });

    await storage.createJobRun({
      jobId,
      status: 'failed',
      startedAt: now,
      attempt: 1,
    });

    // Lock the job with the worker
    await storage.acquireLock(formatLockName(jobId), workerName, defaultValues.jobLockTTL);

    // Update heartbeat to old timestamp
    await storage.updateWorkerHeartbeat(workerName, {
      last_heartbeat: lastWeek,
    });

    // Execute task
    await task.execute();

    // Verify worker was removed
    const worker = await storage.getWorker(workerName);
    expect(worker).toBeNull();

    // Verify job was unlocked
    const locks = await storage.listLocks({ worker: workerName });
    const jobLock = locks.find((lock) => formatLockName(jobId) === lock.lockId);
    expect(jobLock).toBeUndefined();

    // Verify job was untouched.
    const job = await storage.getJob(jobId);
    expect(job?.status).toBe('pending');
    expect(job?.failCount).toBe(1);
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
    await storage.updateWorkerHeartbeat(worker, {
      last_heartbeat: lastWeek,
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
    await storage.updateWorkerHeartbeat(worker1, { last_heartbeat: lastWeek });
    await storage.updateWorkerHeartbeat(worker2, { last_heartbeat: lastWeek });

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
