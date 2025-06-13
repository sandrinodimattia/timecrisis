import { z } from 'zod';
import { vi } from 'vitest';
import Database from 'better-sqlite3';

import { SQLiteJobStorage } from '../adapter.js';
import { CreateJob, EmptyLogger, JobDefinition, Logger } from '@timecrisis/timecrisis';

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

export const createStorage = async function createStorage(
  databasePath?: string
): Promise<{ db: Database.Database; storage: SQLiteJobStorage }> {
  // Create an in-memory database for testing
  const db = new Database(databasePath ?? ':memory:');
  db.pragma('journal_mode = WAL');
  db.pragma('busy_timeout = 5000');
  const storage = new SQLiteJobStorage(db);
  await storage.init();
  return { db, storage };
};

export const createLogger = function createLogger(): Logger {
  return new EmptyLogger();
};

export const defaultValues = {
  workerName: 'test-worker',
  lockName: 'test-lock',
  lockNameAlternative: 'test-lock-other',
  maxConcurrentJobs: 20,
  pollInterval: 750,
  jobLockTTL: 120000,
  lockTTL: 30000,
  longRunningJobDuration: 2000,
  distributedLockTTL: 30000,
  executionDuration: 5000,
  schedulerJobMaxStaleAge: 240000,
  workerAliveInterval: 100,
  workerDeadTimeout: 45000,
  scatterInterval: 25,
};

export const defaultJob: CreateJob = {
  type: 'test',
  entityId: 'ref-123',
  data: {
    test: true,
  },
  priority: 10,
  maxRetries: 3,
  status: 'pending',
  backoffStrategy: 'exponential',
  failCount: 0,
};

export const defaultJobDefinition = {
  type: 'test',
  schema: z.object({ test: z.boolean() }),
  handle: (): Promise<void> => Promise.resolve(),
} as unknown as JobDefinition;
vi.spyOn(defaultJobDefinition, 'handle');

export const longRunningJobDefinition = {
  type: 'test-long',
  schema: z.object({ test: z.boolean() }),
  handle: vi.fn().mockImplementation(async () => {
    await new Promise((resolve) => setTimeout(resolve, 2000));
  }),
} as unknown as JobDefinition;
vi.spyOn(longRunningJobDefinition, 'handle');

export const failedJobDefinition = {
  type: 'test-failed',
  handle: vi.fn().mockRejectedValue(new Error('Test error')),
  concurrency: 1,
  schema: z.object({ test: z.boolean() }),
};
vi.spyOn(failedJobDefinition, 'handle');

const jobRegistrations = new Map<string, JobDefinition>();
jobRegistrations.set(failedJobDefinition.type, failedJobDefinition);
jobRegistrations.set(defaultJobDefinition.type, defaultJobDefinition);
jobRegistrations.set(longRunningJobDefinition.type, longRunningJobDefinition);

export const defaultJobRegistrations = jobRegistrations;
