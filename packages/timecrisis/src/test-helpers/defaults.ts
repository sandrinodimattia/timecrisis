import { z } from 'zod';
import { expect, vi } from 'vitest';

import { TaskContext } from '../tasks/types.js';
import { JobStorage } from '../storage/types.js';
import { JobDefinition } from '../scheduler/types.js';
import { CreateJob } from '../storage/schemas/job.js';
import { EmptyLogger, Logger } from '../logger/index.js';
import { MockJobStorage } from '../storage/mock/index.js';
import { formatLockName } from '../concurrency/job-lock.js';
import { JobStateMachine } from '../state-machine/index.js';
import { LeaderElection } from '../concurrency/leader-election.js';
import { DistributedLock } from '../concurrency/distributed-lock.js';
import { ConcurrencyManager } from '../concurrency/concurrency-manager.js';

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

export const createJobLock = async function (
  storage: JobStorage,
  jobId: string,
  worker?: string,
  lockTTL?: number
): Promise<void> {
  await storage.acquireLock(
    formatLockName(jobId),
    worker ?? defaultValues.workerName,
    lockTTL ?? defaultValues.jobLockTTL
  );
};

export const createTaskContext = function ({
  logger,
  storage,
}: {
  logger?: Logger;
  storage?: JobStorage;
} = {}): TaskContext {
  const loggerInstance = logger ?? createLogger();
  const storageInstance = storage ?? createStorage();
  return {
    jobs: jobRegistrations,
    worker: defaultValues.workerName,
    logger: loggerInstance,
    storage: storageInstance,
    stateMachine: new JobStateMachine({
      logger: loggerInstance,
      storage: storageInstance,
      jobs: defaultJobRegistrations,
    }),
    leaderElection: new LeaderElection({
      logger: loggerInstance,
      storage: storageInstance,
      node: defaultValues.workerName,
      lockTTL: defaultValues.distributedLockTTL,
    }),
    lock: new DistributedLock({
      storage: storageInstance,
      lockTTL: defaultValues.jobLockTTL,
      worker: defaultValues.workerName,
    }),
    concurrency: new ConcurrencyManager(loggerInstance, {
      maxConcurrentJobs: defaultValues.maxConcurrentJobs,
    }),
  };
};

export const expectJobLocked = async function (storage: JobStorage, jobId: string): Promise<void> {
  const locks = await storage.listLocks({ worker: defaultValues.workerName });
  const jobLock = locks.find((lock) => formatLockName(jobId) === lock.lockId);
  expect(jobLock?.lockId).toBe(formatLockName(jobId));
};

export const expectJobUnlocked = async function (
  storage: JobStorage,
  jobId: string
): Promise<void> {
  const locks = await storage.listLocks({ worker: defaultValues.workerName });
  const jobLock = locks.find((lock) => formatLockName(jobId) === lock.lockId);
  expect(jobLock?.lockId).toBe(formatLockName(jobId));
};

export const defaultValues = {
  workerName: 'test-worker',
  lockName: 'test-lock',
  lockNameAlternative: 'test-lock-other',
  maxConcurrentJobs: 20,
  pollInterval: 1000,
  jobLockTTL: 120000,
  lockTTL: 30000,
  longRunningJobDuration: 2000,
  distributedLockTTL: 30000,
  executionDuration: 5000,
  schedulerJobMaxStaleAge: 240000,
  workerAliveInterval: 100,
  workerDeadTimeout: 45000,
  scatterWaitDelay: 25,
};

export const defaultJob: CreateJob = {
  type: 'test',
  referenceId: 'ref-123',
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
  concurrency: 20,
  schema: z.object({ test: z.boolean() }),
  handle: (): Promise<void> => Promise.resolve(),
} as unknown as JobDefinition;
vi.spyOn(defaultJobDefinition, 'handle');

export const longRunningJobDefinition = {
  type: 'test-long',
  concurrency: 20,
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
