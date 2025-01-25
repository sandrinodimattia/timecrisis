import { describe, it, expect, vi, beforeEach, afterEach } from 'vitest';

import { EmptyLogger } from '../logger/index.js';
import { WorkerAliveTask } from './worker-alive.js';
import { MockJobStorage } from '../storage/mock/index.js';

describe('WorkerAliveTask', () => {
  let storage: MockJobStorage;
  let task: WorkerAliveTask;

  beforeEach(() => {
    storage = new MockJobStorage();
    task = new WorkerAliveTask({
      storage,
      name: 'test-worker',
      heartbeatInterval: 100,
      logger: new EmptyLogger(),
    });

    vi.useFakeTimers();
  });

  afterEach(() => {
    task.stop();
    vi.useRealTimers();
  });

  it('should register worker on start', async () => {
    await task.start();
    const workerId = task.getWorkerId();

    expect(workerId).toBeDefined();
    const worker = await storage.getWorker(workerId!);
    expect(worker).toBeDefined();
    expect(worker?.name).toBe('test-worker');
  });

  it('should update heartbeat at intervals', async () => {
    await task.start();
    const workerId = task.getWorkerId();

    // Get initial heartbeat
    const initialWorker = await storage.getWorker(workerId!);
    const initialHeartbeat = initialWorker?.last_heartbeat;

    // Advance timer and check for update
    await vi.advanceTimersByTimeAsync(100);
    const updatedWorker = await storage.getWorker(workerId!);
    expect(updatedWorker?.last_heartbeat.getTime()).toBeGreaterThan(initialHeartbeat!.getTime());
  });

  it('should stop sending heartbeats when stopped', async () => {
    await task.start();
    const workerId = task.getWorkerId();

    // Get heartbeat after first update
    await vi.advanceTimersByTimeAsync(100);
    const worker1 = await storage.getWorker(workerId!);
    const heartbeat1 = worker1?.last_heartbeat;

    // Stop the task
    task.stop();

    // Advance timer and verify no update
    await vi.advanceTimersByTimeAsync(100);
    const worker2 = await storage.getWorker(workerId!);
    expect(worker2?.last_heartbeat.getTime()).toBe(heartbeat1?.getTime());
  });

  it('should handle storage errors gracefully', async () => {
    const mockError = new Error('Storage error');

    // Create a storage that fails on heartbeat
    const failingStorage = new MockJobStorage();
    vi.spyOn(failingStorage, 'updateWorkerHeartbeat').mockRejectedValue(mockError);

    const failingTask = new WorkerAliveTask({
      storage,
      name: 'test-worker',
      heartbeatInterval: 100,
      logger: new EmptyLogger(),
    });

    await failingTask.start();
    await vi.advanceTimersByTimeAsync(100);

    failingTask.stop();
  });
});
