import { describe, it, expect, vi, beforeEach, afterEach } from 'vitest';

import {
  defaultValues,
  createLogger,
  createStorage,
  prepareEnvironment,
  resetEnvironment,
} from '../test-helpers/defaults.js';
import { WorkerAliveTask } from './worker-alive.js';
import { MockJobStorage } from '../storage/mock/index.js';

describe('WorkerAliveTask', () => {
  let storage: MockJobStorage;
  let task: WorkerAliveTask;

  beforeEach(() => {
    prepareEnvironment();

    storage = createStorage();
    task = new WorkerAliveTask({
      storage,
      name: defaultValues.workerName,
      heartbeatInterval: defaultValues.workerAliveInterval,
      logger: createLogger(),
    });
  });

  afterEach(() => {
    task.stop();
    resetEnvironment();
  });

  it('should register worker on start', async () => {
    await task.start();
    const workerName = task.getWorkerName();

    expect(workerName).toBeDefined();
    const worker = await storage.getWorker(workerName!);
    expect(worker).toBeDefined();
    expect(worker?.name).toBe(defaultValues.workerName);
  });

  it('should update heartbeat at intervals', async () => {
    await task.start();
    const workerName = task.getWorkerName();

    // Get initial heartbeat
    const initialWorker = await storage.getWorker(workerName!);
    const initialHeartbeat = initialWorker?.last_heartbeat;

    // Advance timer and check for update
    await vi.advanceTimersByTimeAsync(defaultValues.workerAliveInterval);
    const updatedWorker = await storage.getWorker(workerName!);
    expect(updatedWorker?.last_heartbeat.getTime()).toBeGreaterThan(initialHeartbeat!.getTime());
  });

  it('should stop sending heartbeats when stopped', async () => {
    await task.start();
    const workerName = task.getWorkerName();

    // Get heartbeat after first update
    await vi.advanceTimersByTimeAsync(defaultValues.workerAliveInterval);
    const worker1 = await storage.getWorker(workerName!);
    const heartbeat1 = worker1?.last_heartbeat;

    // Stop the task
    task.stop();

    // Advance timer and verify no update
    await vi.advanceTimersByTimeAsync(defaultValues.workerAliveInterval);
    const worker2 = await storage.getWorker(workerName!);
    expect(worker2?.last_heartbeat.getTime()).toBe(heartbeat1?.getTime());
  });

  it('should handle storage errors gracefully', async () => {
    const mockError = new Error('Storage error');

    // Create a storage that fails on heartbeat
    const failingStorage = new MockJobStorage();
    vi.spyOn(failingStorage, 'updateWorkerHeartbeat').mockRejectedValue(mockError);

    const failingTask = new WorkerAliveTask({
      storage,
      name: defaultValues.workerName,
      heartbeatInterval: defaultValues.workerAliveInterval,
      logger: createLogger(),
    });

    await failingTask.start();
    await vi.advanceTimersByTimeAsync(defaultValues.workerAliveInterval);

    failingTask.stop();
  });
});
