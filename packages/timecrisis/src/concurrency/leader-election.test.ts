import { describe, it, expect, vi, beforeEach, afterEach } from 'vitest';

import { EmptyLogger } from '../logger/index.js';
import { LeaderElection } from './leader-election.js';
import { MockJobStorage } from '../storage/mock/index.js';
import { DistributedLockError } from './distributed-lock.js';
import { defaultValues, prepareEnvironment, resetEnvironment } from '../test-helpers/defaults.js';

describe('LeaderElection', () => {
  let storage: MockJobStorage;
  let leader: LeaderElection;

  beforeEach(() => {
    prepareEnvironment();
    storage = new MockJobStorage();
    leader = new LeaderElection({
      logger: new EmptyLogger(),
      storage,
      node: defaultValues.workerName,
      lockTTL: defaultValues.lockTTL,
    });
  });

  afterEach(async () => {
    try {
      await leader.stop();
    } finally {
      storage.reset();

      resetEnvironment();
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
    await vi.advanceTimersByTimeAsync(defaultValues.lockTTL / 2);
    expect(acquireSpy).toHaveBeenCalledTimes(1); // Initial acquisition
    expect(renewSpy).toHaveBeenCalledTimes(1); // First renewal

    // Fast-forward another half TTL
    await vi.advanceTimersByTimeAsync(defaultValues.lockTTL / 2);
    expect(acquireSpy).toHaveBeenCalledTimes(1); // Still just initial acquisition
    expect(renewSpy).toHaveBeenCalledTimes(2); // Second renewal
  });

  it('should handle concurrent leader elections', async () => {
    // First node becomes leader
    await leader.start();
    expect(leader.isCurrentLeader()).toBe(true);

    // Another node takes over leadership
    await storage.simulateOtherLeader('timecrisis/leader', defaultValues.lockTTL);

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
    await vi.advanceTimersByTimeAsync(defaultValues.lockTTL * 2);
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
      logger: new EmptyLogger(),
      storage,
      node: defaultValues.workerName,
      lockTTL: defaultValues.lockTTL,
      onAcquired,
    });

    await leader.start();
    expect(onAcquired).toHaveBeenCalledTimes(1);
  });

  it('should call onLost when leadership is released', async () => {
    const onLost = vi.fn();
    leader = new LeaderElection({
      logger: new EmptyLogger(),
      storage,
      node: defaultValues.workerName,
      lockTTL: defaultValues.lockTTL,
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
      logger: new EmptyLogger(),
      storage,
      node: defaultValues.workerName,
      lockTTL: defaultValues.lockTTL,
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
      logger: new EmptyLogger(),
      storage,
      node: defaultValues.workerName,
      lockTTL: defaultValues.lockTTL,
      onLost,
    });

    // First become leader
    await leader.start();
    expect(leader.isCurrentLeader()).toBe(true);

    // Simulate another node taking leadership
    await storage.simulateOtherLeader('timecrisis/leader', defaultValues.lockTTL);

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
      logger: new EmptyLogger(),
      storage,
      node: defaultValues.workerName,
      lockTTL: defaultValues.lockTTL,
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
      await vi.advanceTimersByTimeAsync(defaultValues.lockTTL / 2);
      expect(onAcquired).not.toHaveBeenCalled();
      expect(onLost).not.toHaveBeenCalled();
    }
  });

  it('should elect only one leader when multiple nodes start simultaneously', async () => {
    const leader1 = new LeaderElection({
      logger: new EmptyLogger(),
      storage,
      node: 'node-1',
      lockTTL: defaultValues.lockTTL,
    });
    const leader2 = new LeaderElection({
      logger: new EmptyLogger(),
      storage,
      node: 'node-2',
      lockTTL: defaultValues.lockTTL,
    });
    const leader3 = new LeaderElection({
      logger: new EmptyLogger(),
      storage,
      node: 'node-3',
      lockTTL: defaultValues.lockTTL,
    });

    // Start all election processes concurrently
    await Promise.all([leader1.start(), leader2.start(), leader3.start()]);

    const leaders = [
      leader1.isCurrentLeader(),
      leader2.isCurrentLeader(),
      leader3.isCurrentLeader(),
    ];

    const leaderCount = leaders.filter((isLeader) => isLeader).length;

    // Assert that exactly one of them became the leader
    expect(leaderCount).toBe(1);
  });

  it('should handle leadership loss and reacquisition', async () => {
    const onAcquired = vi.fn();
    const onLost = vi.fn();

    leader = new LeaderElection({
      logger: new EmptyLogger(),
      storage,
      node: defaultValues.workerName,
      lockTTL: defaultValues.lockTTL,
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
    await storage.simulateOtherLeader('timecrisis/leader', defaultValues.lockTTL);

    // Wait for the next check interval
    await vi.advanceTimersByTimeAsync(defaultValues.lockTTL / 2);
    expect(onAcquired).not.toHaveBeenCalled();
    expect(onLost).toHaveBeenCalledTimes(1);

    // Reset the mocks again
    onAcquired.mockClear();
    onLost.mockClear();

    // Wait for lock to expire and next check interval
    await vi.advanceTimersByTimeAsync(defaultValues.lockTTL * 2);
    expect(onAcquired).toHaveBeenCalledTimes(1);
    expect(onLost).not.toHaveBeenCalled();
  });

  it('should not call callbacks unnecessarily', async () => {
    const onAcquired = vi.fn();
    const onLost = vi.fn();

    leader = new LeaderElection({
      logger: new EmptyLogger(),
      storage,
      node: defaultValues.workerName,
      lockTTL: defaultValues.lockTTL,
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
      await vi.advanceTimersByTimeAsync(defaultValues.lockTTL / 2);
      expect(onAcquired).not.toHaveBeenCalled();
      expect(onLost).not.toHaveBeenCalled();
    }
  });

  it('should register worker when leadership is acquired', async () => {
    await leader.start();
    expect(leader.isCurrentLeader()).toBe(true);

    // Verify worker was registered
    const worker = await storage.getWorker(defaultValues.workerName);
    expect(worker).toBeDefined();
    expect(worker?.name).toBe(defaultValues.workerName);
    expect(worker?.first_seen).toBeDefined();
    expect(worker?.last_heartbeat).toBeDefined();
  });

  it('should preserve worker registration when leadership is renewed', async () => {
    const initialTime = new Date('2024-01-01T00:00:00.000Z');
    const laterTime = new Date('2024-01-01T00:01:00.000Z');

    vi.setSystemTime(initialTime);
    await leader.start();
    expect(leader.isCurrentLeader()).toBe(true);

    // Get initial worker state
    const initialWorker = await storage.getWorker(defaultValues.workerName);
    expect(initialWorker).toBeDefined();
    const initialFirstSeen = initialWorker?.first_seen;
    expect(initialFirstSeen?.getTime()).toEqual(initialTime.getTime());

    await leader.stop();
    // Set a later time for renewal
    vi.setSystemTime(laterTime);

    // Fast-forward time to trigger renewal
    await vi.advanceTimersByTimeAsync(defaultValues.lockTTL);

    // Verify worker still exists with same first_seen
    const renewedWorker = await storage.getWorker(defaultValues.workerName);
    expect(renewedWorker).toBeDefined();
    expect(renewedWorker?.first_seen).toEqual(initialFirstSeen);
    expect(renewedWorker?.last_heartbeat).toEqual(initialTime);
  });

  it('should preserve worker registration when leadership is lost', async () => {
    await leader.start();
    expect(leader.isCurrentLeader()).toBe(true);

    // Get initial worker state
    const initialWorker = await storage.getWorker(defaultValues.workerName);
    expect(initialWorker).toBeDefined();
    const initialFirstSeen = initialWorker?.first_seen;

    // Simulate another node taking leadership
    await storage.simulateOtherLeader('timecrisis/leader', defaultValues.lockTTL);

    // Force an immediate leadership check
    await leader.stop();
    await leader.start();
    expect(leader.isCurrentLeader()).toBe(false);

    // Verify worker still exists with same first_seen
    const workerAfterLoss = await storage.getWorker(defaultValues.workerName);
    expect(workerAfterLoss).toBeDefined();
    expect(workerAfterLoss?.first_seen).toEqual(initialFirstSeen);
  });
});
