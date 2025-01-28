import { z } from 'zod';
import { vi } from 'vitest';

import { CreateJob } from '../storage/schemas/job.js';
import { EmptyLogger, Logger } from '../logger/index.js';
import { MockJobStorage } from '../storage/mock/index.js';

export const now = new Date('2025-01-01T09:00:00.000Z');
export const lastWeek = new Date('2024-12-25T09:00:00.000Z');
export const tomorrow = new Date('2025-01-02T09:00:00.000Z');

export function prepareEnvironment(): void {
  vi.clearAllMocks();
  vi.useFakeTimers();
  vi.setSystemTime(now);
}

export function resetEnvironment(): void {
  vi.clearAllTimers();
  vi.clearAllMocks();
  vi.useRealTimers();
}

export const createStorage = function createStorage(): MockJobStorage {
  return new MockJobStorage();
};

export const createLogger = function createLogger(): Logger {
  return new EmptyLogger();
};

export const defaultValues = {
  workerName: 'test-worker',
  lockName: 'test-lock',
  lockNameAlternative: 'test-lock-other',
  pollInterval: 1000,
  jobLockTTL: 120000,
  lockTTL: 30000,
  distributedLockTTL: 30000,
  executionDuration: 5000,
  schedulerJobMaxStaleAge: 60000,
  workerAliveInterval: 100,
  workerDeadTimeout: 45000,
};

export const defaultJobSchema = {
  type: 'test',
  schema: z.object({ test: z.boolean() }),
  handle: (): Promise<void> => Promise.resolve(),
};

export const defaultJob: CreateJob = {
  type: 'test',
  entityId: 'ref-123',
  data: {
    test: true,
  },
  priority: 0,
  maxRetries: 3,
  status: 'pending',
  backoffStrategy: 'exponential',
  failCount: 0,
};
