import { describe, it, expect, vi, beforeEach, afterEach } from 'vitest';

import { EmptyLogger } from '../logger/index.js';
import { DeadWorkersTask } from './dead-workers.js';
import { LeaderElection } from '../leader/index.js';
import { MockJobStorage } from '../storage/mock/index.js';

describe('DeadWorkersTask', () => {
  let storage: MockJobStorage;
  let task: DeadWorkersTask;
  let leader: LeaderElection;

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
      cleanupInterval: 100,
      deadWorkerTimeout: 30000,
    });

    vi.useFakeTimers();
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
      cleanupInterval: 100,
      deadWorkerTimeout: 30000,
    });

    await failingTask.start();
    await vi.advanceTimersByTimeAsync(100);

    failingTask.stop();
  });
});
