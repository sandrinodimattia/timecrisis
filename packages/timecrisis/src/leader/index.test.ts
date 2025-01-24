import { describe, it, expect, vi, beforeEach, afterEach } from 'vitest';

import { LeaderElection } from './index.js';
import { MockJobStorage } from '../storage/mock/index.js';
import { DistributedLockError } from '../distributed-lock/index.js';

describe('LeaderElection', () => {
  const node = 'test-node';
  const lockTTL = 1000;
  let storage: MockJobStorage;
  let leader: LeaderElection;

  beforeEach(() => {
    vi.useFakeTimers();
    storage = new MockJobStorage();
    leader = new LeaderElection({
      storage,
      node,
      lockTTL,
    });
  });

  afterEach(async () => {
    try {
      await leader.stop();
    } finally {
      storage.reset();
      // Clear all timers and mocks
      vi.clearAllTimers();
      vi.clearAllMocks();
      vi.useRealTimers();
    }
  });

  it('should successfully acquire leadership', async () => {
    await leader.start();
    expect(leader.isCurrentLeader()).toBe(true);
  });

  it('should not be leader before starting', () => {
    expect(leader.isCurrentLeader()).toBe(false);
  });

  it('should handle failed leadership acquisition', async () => {
    storage.setOptions({ shouldFailAcquire: true });
    await expect(leader.start()).rejects.toThrow(DistributedLockError);
    expect(leader.isCurrentLeader()).toBe(false);
  });

  it('should release leadership on stop', async () => {
    await leader.start();
    expect(leader.isCurrentLeader()).toBe(true);
    await leader.stop();
    expect(leader.isCurrentLeader()).toBe(false);
  });

  it('should handle failed leadership release', async () => {
    await leader.start();
    expect(leader.isCurrentLeader()).toBe(true);
    storage.setOptions({ shouldFailRelease: true });
    await expect(leader.stop()).rejects.toThrow('Failed to release lock');
    // Even if release fails, we should mark ourselves as not being the leader
    expect(leader.isCurrentLeader()).toBe(false);
  });

  it('should periodically try to renew leadership', async () => {
    const acquireSpy = vi.spyOn(storage, 'acquireLock');
    const renewSpy = vi.spyOn(storage, 'renewLock');
    await leader.start();

    // Fast-forward half the TTL
    await vi.advanceTimersByTimeAsync(lockTTL / 2);
    expect(acquireSpy).toHaveBeenCalledTimes(1); // Initial acquisition
    expect(renewSpy).toHaveBeenCalledTimes(1); // First renewal

    // Fast-forward another half TTL
    await vi.advanceTimersByTimeAsync(lockTTL / 2);
    expect(acquireSpy).toHaveBeenCalledTimes(1); // Still just initial acquisition
    expect(renewSpy).toHaveBeenCalledTimes(2); // Second renewal
  });

  it('should handle concurrent leader elections', async () => {
    // First node becomes leader
    await leader.start();
    expect(leader.isCurrentLeader()).toBe(true);

    // Another node takes over leadership
    await storage.simulateOtherLeader('SCHEDULER_LEADER', lockTTL);

    // Force an immediate leadership check
    await leader.stop();
    await leader.start();
    expect(leader.isCurrentLeader()).toBe(false);
  });

  it('should stop periodic checks after stopping', async () => {
    const renewalSpy = vi.spyOn(storage, 'acquireLock');
    await leader.start();
    await leader.stop();

    // Fast-forward time
    await vi.advanceTimersByTimeAsync(lockTTL * 2);
    expect(renewalSpy).toHaveBeenCalledTimes(1); // Only the initial acquisition
  });

  it('should be safe to stop multiple times', async () => {
    await leader.start();
    await leader.stop();
    await leader.stop(); // Should not throw
    expect(leader.isCurrentLeader()).toBe(false);
  });

  it('should handle rapid start/stop cycles', async () => {
    for (let i = 0; i < 5; i++) {
      await leader.start();
      expect(leader.isCurrentLeader()).toBe(true);
      await leader.stop();
      expect(leader.isCurrentLeader()).toBe(false);
    }
  });

  it('should call onAcquired when leadership is acquired', async () => {
    const onAcquired = vi.fn();
    leader = new LeaderElection({
      storage,
      node,
      lockTTL,
      onAcquired,
    });

    await leader.start();
    expect(onAcquired).toHaveBeenCalledTimes(1);
  });

  it('should call onLost when leadership is released', async () => {
    const onLost = vi.fn();
    leader = new LeaderElection({
      storage,
      node,
      lockTTL,
      onLost,
    });

    await leader.start();
    await leader.stop();
    expect(onLost).toHaveBeenCalledTimes(1);
  });

  it('should handle async callbacks', async () => {
    const onAcquired = vi.fn().mockImplementation(() => Promise.resolve());
    const onLost = vi.fn().mockImplementation(() => Promise.resolve());

    leader = new LeaderElection({
      storage,
      node,
      lockTTL,
      onAcquired,
      onLost,
    });

    await leader.start();
    expect(onAcquired).toHaveBeenCalledTimes(1);

    await leader.stop();
    expect(onLost).toHaveBeenCalledTimes(1);
  });

  it('should call onLost when leadership is lost due to acquisition failure', async () => {
    const onLost = vi.fn();
    leader = new LeaderElection({
      storage,
      node,
      lockTTL,
      onLost,
    });

    // First become leader
    await leader.start();
    expect(leader.isCurrentLeader()).toBe(true);

    // Simulate another node taking leadership
    await storage.simulateOtherLeader('SCHEDULER_LEADER', lockTTL);

    // Force an immediate leadership check by triggering a renewal
    // eslint-disable-next-line @typescript-eslint/no-explicit-any
    await (leader as any).tryBecomeLeader();

    expect(onLost).toHaveBeenCalledTimes(1);
    expect(leader.isCurrentLeader()).toBe(false);
  });

  it('should not call callbacks unnecessarily during periodic renewals', async () => {
    const onAcquired = vi.fn();
    const onLost = vi.fn();

    leader = new LeaderElection({
      storage,
      node,
      lockTTL,
      onAcquired,
      onLost,
    });

    // Should not call anything before start
    expect(onAcquired).not.toHaveBeenCalled();
    expect(onLost).not.toHaveBeenCalled();

    // Start should call onAcquired once
    await leader.start();
    expect(onAcquired).toHaveBeenCalledTimes(1);
    expect(onLost).not.toHaveBeenCalled();

    // Reset the mock to ensure we can track new calls
    onAcquired.mockClear();
    onLost.mockClear();

    // Run a few renewal cycles
    for (let i = 0; i < 3; i++) {
      // Advance to next interval
      await vi.advanceTimersByTimeAsync(lockTTL / 2);
      expect(onAcquired).not.toHaveBeenCalled();
      expect(onLost).not.toHaveBeenCalled();
    }
  });

  it('should handle leadership loss and reacquisition', async () => {
    const onAcquired = vi.fn();
    const onLost = vi.fn();

    leader = new LeaderElection({
      storage,
      node,
      lockTTL,
      onAcquired,
      onLost,
    });

    // Initial acquisition
    await leader.start();
    expect(onAcquired).toHaveBeenCalledTimes(1);
    expect(onLost).not.toHaveBeenCalled();

    // Reset the mocks to ensure we can track new calls
    onAcquired.mockClear();
    onLost.mockClear();

    // Lose leadership
    await storage.simulateOtherLeader('SCHEDULER_LEADER', lockTTL);

    // Wait for the next check interval
    await vi.advanceTimersByTimeAsync(lockTTL / 2);
    expect(onAcquired).not.toHaveBeenCalled();
    expect(onLost).toHaveBeenCalledTimes(1);

    // Reset the mocks again
    onAcquired.mockClear();
    onLost.mockClear();

    // Wait for lock to expire and next check interval
    await vi.advanceTimersByTimeAsync(lockTTL * 2);
    expect(onAcquired).toHaveBeenCalledTimes(1);
    expect(onLost).not.toHaveBeenCalled();
  });

  it('should not call callbacks unnecessarily', async () => {
    const onAcquired = vi.fn();
    const onLost = vi.fn();

    leader = new LeaderElection({
      storage,
      node,
      lockTTL,
      onAcquired,
      onLost,
    });

    // Initial acquisition
    await leader.start();
    expect(onAcquired).toHaveBeenCalledTimes(1);
    expect(onLost).not.toHaveBeenCalled();

    // Reset the mocks
    onAcquired.mockClear();
    onLost.mockClear();

    // Run multiple renewal cycles
    for (let i = 0; i < 3; i++) {
      await vi.advanceTimersByTimeAsync(lockTTL / 2);
      expect(onAcquired).not.toHaveBeenCalled();
      expect(onLost).not.toHaveBeenCalled();
    }
  });
});
