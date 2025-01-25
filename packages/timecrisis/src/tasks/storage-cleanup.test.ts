import { describe, it, expect, vi, beforeEach, afterEach } from 'vitest';

import { EmptyLogger } from '../logger/index.js';
import { MockJobStorage } from '../storage/mock/index.js';
import { StorageCleanupTask } from './storage-cleanup.js';

describe('StorageCleanupTask', () => {
  let storage: MockJobStorage;
  let task: StorageCleanupTask;
  const now = new Date('2025-01-23T00:00:00.000Z');

  beforeEach(() => {
    vi.useFakeTimers();
    vi.setSystemTime(now);

    storage = new MockJobStorage();
    task = new StorageCleanupTask({
      storage,
      logger: new EmptyLogger(),
      pollInterval: 1000,
      jobRetention: 90,
      failedJobRetention: 90,
      deadLetterRetention: 180,
    });

    // Clear all mocks before each test
    vi.clearAllMocks();
  });

  afterEach(() => {
    // Stop the task
    task.stop();

    // Clear all timers and mocks
    vi.clearAllTimers();
    vi.clearAllMocks();
    vi.useRealTimers();
  });

  it('should execute cleanup with correct retention values', async () => {
    vi.spyOn(storage, 'cleanup');
    await task.execute();

    expect(storage.cleanup).toHaveBeenCalledWith({
      jobRetention: 90,
      failedJobRetention: 90,
      deadLetterRetention: 180,
    });
  });

  it('should not execute cleanup if already running', async () => {
    vi.spyOn(storage, 'cleanup');

    // Start first execution
    const firstExecution = task.execute();

    // Try to start second execution immediately
    await task.execute();

    // Wait for first execution to complete
    await firstExecution;

    expect(storage.cleanup).toHaveBeenCalledTimes(1);
  });

  it('should run cleanup periodically when started', async () => {
    vi.spyOn(storage, 'cleanup');

    // Start the task
    await task.start();

    // Advance timer by two intervals
    await vi.advanceTimersByTimeAsync(1000);
    expect(storage.cleanup).toHaveBeenCalledTimes(1);

    await vi.advanceTimersByTimeAsync(1000);
    expect(storage.cleanup).toHaveBeenCalledTimes(2);
  });

  it('should stop running cleanup when stopped', async () => {
    vi.spyOn(storage, 'cleanup');

    // Start the task
    await task.start();

    // Advance timer by one interval
    await vi.advanceTimersByTimeAsync(1000);
    expect(storage.cleanup).toHaveBeenCalledTimes(1);

    // Stop the task
    task.stop();

    // Advance timer by another interval
    await vi.advanceTimersByTimeAsync(1000);
    expect(storage.cleanup).toHaveBeenCalledTimes(1); // Should not have increased
  });
});
