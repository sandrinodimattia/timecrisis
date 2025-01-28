import { describe, it, expect, beforeEach, vi, afterEach } from 'vitest';

import { MockJobStorage } from '../storage/mock/index.js';
import { InMemoryJobStorage } from '../storage/memory/index.js';
import { DistributedLock, DistributedLockError } from './distributed-lock.js';
import { defaultValues, prepareEnvironment, resetEnvironment } from '../test-helpers/defaults.js';

describe('DistributedLock', () => {
  describe('with MemoryJobStorage', () => {
    let storage: InMemoryJobStorage;
    let distributedLock: DistributedLock;

    beforeEach(() => {
      prepareEnvironment();
      storage = new InMemoryJobStorage();
      distributedLock = new DistributedLock({
        storage,
        worker: defaultValues.workerName,
        lockTTL: defaultValues.distributedLockTTL,
      });
    });

    afterEach(() => {
      resetEnvironment();
    });

    it('should successfully acquire a lock', async () => {
      const acquired = await distributedLock.acquire(defaultValues.lockName);
      expect(acquired).not.toBeUndefined();
    });

    it('should fail to acquire the same lock twice', async () => {
      await distributedLock.acquire(defaultValues.lockName);
      const secondAcquire = await distributedLock.acquire(defaultValues.lockName);
      expect(secondAcquire).toBeUndefined();
    });

    it('should allow different locks to be acquired', async () => {
      const lock1 = await distributedLock.acquire('test-lock-1');
      const lock2 = await distributedLock.acquire('test-lock-2');
      expect(lock1).not.toBe(undefined);
      expect(lock2).not.toBe(undefined);
    });

    it('should successfully renew an existing lock', async () => {
      const lock = await distributedLock.acquire(defaultValues.lockName);
      const renewed = await lock!.renew();
      expect(renewed).toBe(true);
    });

    it('should fail to renew an expired lock', async () => {
      const shortLock = new DistributedLock({
        storage,
        worker: defaultValues.workerName,
        lockTTL: 1, // 1ms TTL
      });

      const short = await shortLock.acquire(defaultValues.lockName);
      await vi.advanceTimersByTimeAsync(10);
      const renewed = await short!.renew();
      expect(renewed).toBe(false);
    });

    it('should fail to renew a lock owned by another node', async () => {
      // First node acquires the lock
      const node1Lock = new DistributedLock({
        storage,
        worker: 'node-1',
        lockTTL: defaultValues.distributedLockTTL,
      });
      const lock1 = await node1Lock.acquire(defaultValues.lockName);
      await lock1!.release();

      // Second node tries to renew the lock
      const node2Lock = new DistributedLock({
        storage,
        worker: 'node-2',
        lockTTL: defaultValues.distributedLockTTL,
      });
      await node2Lock.acquire(defaultValues.lockName);
      const renewed = await lock1!.renew();
      expect(renewed).toBe(false);
    });

    it('should successfully release a lock', async () => {
      const lock = await distributedLock.acquire(defaultValues.lockName);
      await lock!.release();
      const reacquired = await distributedLock.acquire(defaultValues.lockName);
      expect(reacquired).not.toBeUndefined();
    });

    it('should allow acquiring a lock after TTL expiration', async () => {
      const shortLock = new DistributedLock({
        storage,
        worker: defaultValues.workerName,
        lockTTL: 1, // 1ms TTL
      });

      await shortLock.acquire(defaultValues.lockName);
      await vi.advanceTimersByTimeAsync(10);
      const reacquired = await shortLock.acquire(defaultValues.lockName);
      expect(reacquired!.name).toBe(defaultValues.lockName);
    });

    it('should wait and acquire a lock when it becomes available', async () => {
      // First acquire the lock
      const lock1 = await distributedLock.acquire(defaultValues.lockName);
      expect(lock1).not.toBeUndefined();

      // Start waiting for the lock in a separate promise
      const waitPromise = distributedLock.waitForLock(defaultValues.lockName);

      // Release the lock after a short delay
      await vi.advanceTimersByTimeAsync(100);
      await lock1!.release();
      await vi.advanceTimersByTimeAsync(1000);

      // Wait for the lock
      const lock2 = await waitPromise;
      expect(lock2).not.toBeUndefined();
    });

    it('should acquire lock immediately if no one holds it', async () => {
      const lock = await distributedLock.waitForLock(defaultValues.lockName);
      expect(lock).not.toBeUndefined();
    });

    it('should timeout while waiting for a lock', async () => {
      // First acquire the lock
      await distributedLock.acquire(defaultValues.lockName);

      // Create a new lock with a short timeout
      const shortLock = new DistributedLock({
        storage,
        worker: defaultValues.workerName,
        lockTTL: defaultValues.lockTTL,
      });

      // Try to wait for the lock with a short timeout
      const lockPromise = shortLock.waitForLock(defaultValues.lockName);
      await vi.advanceTimersByTimeAsync(defaultValues.lockTTL);
      expect(await lockPromise).toBeUndefined();
    });

    it('should handle multiple waiters for the same lock', async () => {
      // First acquire the lock
      const lock1 = await distributedLock.acquire(defaultValues.lockName);
      expect(lock1).not.toBeUndefined();

      // Start multiple waiters
      const waiter1 = distributedLock.waitForLock(defaultValues.lockName);
      const waiter2 = distributedLock.waitForLock(defaultValues.lockName);

      // Release the lock after a short delay
      await vi.advanceTimersByTimeAsync(100);
      await lock1!.release();
      await vi.advanceTimersByTimeAsync(defaultValues.lockTTL);

      // Wait for both waiters
      const [result1, result2] = await Promise.all([waiter1, waiter2]);

      // Only one waiter should get the lock
      expect(result1 !== undefined || result2 !== undefined).toBe(true);
      expect(result1 === undefined || result2 === undefined).toBe(true);
    });
  });

  describe('with MockJobStorage', () => {
    let storage: MockJobStorage;
    let distributedLock: DistributedLock;

    beforeEach(() => {
      storage = new MockJobStorage();
      distributedLock = new DistributedLock({
        storage,
        worker: defaultValues.workerName,
        lockTTL: defaultValues.distributedLockTTL,
      });
    });

    it('should handle acquire failures', async () => {
      storage.setOptions({ shouldFailAcquire: true });
      await expect(distributedLock.acquire(defaultValues.lockName)).rejects.toThrow(
        DistributedLockError
      );
    });

    it('should handle release failures', async () => {
      storage.setOptions({ shouldFailRelease: true });
      const lock = await distributedLock.acquire(defaultValues.lockName);
      await expect(lock!.release()).rejects.toThrow();
    });

    it('should respect node ownership', async () => {
      // First node acquires the lock
      const node1Lock = new DistributedLock({
        storage,
        worker: 'node-1',
        lockTTL: defaultValues.distributedLockTTL,
      });
      await node1Lock.acquire(defaultValues.lockName);

      // Second node tries to acquire the same lock
      const node2Lock = new DistributedLock({
        storage,
        worker: 'node-2',
        lockTTL: defaultValues.distributedLockTTL,
      });
      const acquired = await node2Lock.acquire(defaultValues.lockName);
      expect(acquired).toBeUndefined();
    });

    it('should allow a different node to acquire after release', async () => {
      // First node acquires and releases
      const node1Lock = new DistributedLock({
        storage,
        worker: 'node-1',
        lockTTL: defaultValues.distributedLockTTL,
      });
      const lock1 = await node1Lock.acquire(defaultValues.lockName);
      await lock1!.release();

      // Second node should now be able to acquire
      const node2Lock = new DistributedLock({
        storage,
        worker: 'node-2',
        lockTTL: defaultValues.distributedLockTTL,
      });
      const otherLock = await node2Lock.acquire(defaultValues.lockName);
      expect(otherLock!.name).toBe(defaultValues.lockName);
    });

    it('should handle extend failures', async () => {
      storage.setOptions({ shouldFailExtend: true });
      const lock = await distributedLock.acquire(defaultValues.lockName);
      await expect(lock!.renew()).rejects.toThrow(DistributedLockError);
    });

    it('should handle lock expiration during extend', async () => {
      // First acquire the lock
      const lock = await distributedLock.acquire(defaultValues.lockName);

      // Simulate lock expiration by setting a past expiry time
      const now = Date.now();
      storage.setLock(
        defaultValues.lockName,
        defaultValues.workerName,
        now - defaultValues.distributedLockTTL
      );

      // Try to extend the expired lock
      const extended = await lock!.renew();
      expect(extended).toBe(false);
    });
  });
});
