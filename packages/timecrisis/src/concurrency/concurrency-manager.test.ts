import { describe, it, expect, beforeEach } from 'vitest';

import { EmptyLogger } from '../logger/index.js';
import { ConcurrencyManager } from './concurrency-manager.js';

describe('ConcurrencyManager', () => {
  let manager: ConcurrencyManager;

  beforeEach(() => {
    manager = new ConcurrencyManager(new EmptyLogger(), { maxConcurrentJobs: 2 });
  });

  it('should respect concurrency limits', () => {
    // Should be able to acquire first two slots
    expect(manager.acquire('job-1')).toBe(true);
    expect(manager.acquire('job-2')).toBe(true);
    expect(manager.getRunningCount()).toBe(2);

    // Should not be able to acquire third slot
    expect(manager.acquire('job-3')).toBe(false);
    expect(manager.getRunningCount()).toBe(2);

    // After releasing a slot, should be able to acquire another
    manager.release('job-1');
    expect(manager.getRunningCount()).toBe(1);
    expect(manager.acquire('job-3')).toBe(true);
    expect(manager.getRunningCount()).toBe(2);
  });

  it('should handle releasing non-existent jobs', () => {
    // Should not throw when releasing non-existent job
    expect(() => manager.release('non-existent')).not.toThrow();

    // Should not throw when releasing already released job
    manager.acquire('job-1');
    manager.release('job-1');
    expect(() => manager.release('job-1')).not.toThrow();
  });

  it('should correctly report if more jobs can run', () => {
    expect(manager.canRunMore()).toBe(true);

    manager.acquire('job-1');
    expect(manager.canRunMore()).toBe(true);

    manager.acquire('job-2');
    expect(manager.canRunMore()).toBe(false);

    manager.release('job-1');
    expect(manager.canRunMore()).toBe(true);
  });
});
