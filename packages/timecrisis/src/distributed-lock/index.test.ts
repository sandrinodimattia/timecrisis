import { describe, it, expect, beforeEach } from 'vitest';

import { MockJobStorage } from '../storage/mock';
import { InMemoryJobStorage } from '../storage/memory';
import { DistributedLock, DistributedLockError } from './index';

describe('DistributedLock', () => {
  describe('with MemoryJobStorage', () => {
    let storage: InMemoryJobStorage;
    let lock: DistributedLock;

    beforeEach(() => {
      storage = new InMemoryJobStorage();
      lock = new DistributedLock({
        storage,
        node: 'test-node',
        lockTTL: 1000,
      });
    });

    it('should successfully acquire a lock', async () => {
      const acquired = await lock.acquire('test-lock');
      expect(acquired).toBe(true);
    });

    it('should fail to acquire the same lock twice', async () => {
      await lock.acquire('test-lock');
      const secondAcquire = await lock.acquire('test-lock');
      expect(secondAcquire).toBe(false);
    });

    it('should allow different locks to be acquired', async () => {
      const lock1 = await lock.acquire('test-lock-1');
      const lock2 = await lock.acquire('test-lock-2');
      expect(lock1).toBe(true);
      expect(lock2).toBe(true);
    });

    it('should successfully renew an existing lock', async () => {
      await lock.acquire('test-lock');
      const renewed = await lock.renew('test-lock');
      expect(renewed).toBe(true);
    });

    it('should fail to renew an expired lock', async () => {
      const shortLock = new DistributedLock({
        storage,
        node: 'test-node',
        lockTTL: 1, // 1ms TTL
      });

      await shortLock.acquire('test-lock');
      await new Promise((resolve) => setTimeout(resolve, 10)); // Wait for TTL to expire
      const renewed = await shortLock.renew('test-lock');
      expect(renewed).toBe(false);
    });

    it('should fail to renew a lock owned by another node', async () => {
      // First node acquires the lock
      const node1Lock = new DistributedLock({
        storage,
        node: 'node-1',
        lockTTL: 1000,
      });
      await node1Lock.acquire('test-lock');

      // Second node tries to renew the lock
      const node2Lock = new DistributedLock({
        storage,
        node: 'node-2',
        lockTTL: 1000,
      });
      const renewed = await node2Lock.renew('test-lock');
      expect(renewed).toBe(false);
    });

    it('should successfully release a lock', async () => {
      await lock.acquire('test-lock');
      await lock.release('test-lock');
      const reacquired = await lock.acquire('test-lock');
      expect(reacquired).toBe(true);
    });

    it('should allow acquiring a lock after TTL expiration', async () => {
      const shortLock = new DistributedLock({
        storage,
        node: 'test-node',
        lockTTL: 1, // 1ms TTL
      });

      await shortLock.acquire('test-lock');
      await new Promise((resolve) => setTimeout(resolve, 10));
      const reacquired = await shortLock.acquire('test-lock');
      expect(reacquired).toBe(true);
    });
  });

  describe('with MockJobStorage', () => {
    let storage: MockJobStorage;
    let lock: DistributedLock;

    beforeEach(() => {
      storage = new MockJobStorage();
      lock = new DistributedLock({
        storage,
        node: 'test-node',
        lockTTL: 1000,
      });
    });

    it('should handle acquire failures', async () => {
      storage.setOptions({ shouldFailAcquire: true });
      await expect(lock.acquire('test-lock')).rejects.toThrow(DistributedLockError);
    });

    it('should handle release failures', async () => {
      storage.setOptions({ shouldFailRelease: true });
      await lock.acquire('test-lock');
      await expect(lock.release('test-lock')).rejects.toThrow();
    });

    it('should respect node ownership', async () => {
      // First node acquires the lock
      const node1Lock = new DistributedLock({
        storage,
        node: 'node-1',
        lockTTL: 1000,
      });
      await node1Lock.acquire('test-lock');

      // Second node tries to acquire the same lock
      const node2Lock = new DistributedLock({
        storage,
        node: 'node-2',
        lockTTL: 1000,
      });
      const acquired = await node2Lock.acquire('test-lock');
      expect(acquired).toBe(false);
    });

    it('should allow a different node to acquire after release', async () => {
      // First node acquires and releases
      const node1Lock = new DistributedLock({
        storage,
        node: 'node-1',
        lockTTL: 1000,
      });
      await node1Lock.acquire('test-lock');
      await node1Lock.release('test-lock');

      // Second node should now be able to acquire
      const node2Lock = new DistributedLock({
        storage,
        node: 'node-2',
        lockTTL: 1000,
      });
      const acquired = await node2Lock.acquire('test-lock');
      expect(acquired).toBe(true);
    });

    it('should handle extend failures', async () => {
      storage.setOptions({ shouldFailExtend: true });
      await lock.acquire('test-lock');
      await expect(lock.renew('test-lock')).rejects.toThrow(DistributedLockError);
    });

    it('should handle lock expiration during extend', async () => {
      // First acquire the lock
      await lock.acquire('test-lock');

      // Simulate lock expiration by setting a past expiry time
      const now = Date.now();
      storage.setLock('test-lock', 'test-node', now - 1000);

      // Try to extend the expired lock
      const extended = await lock.renew('test-lock');
      expect(extended).toBe(false);
    });
  });
});
