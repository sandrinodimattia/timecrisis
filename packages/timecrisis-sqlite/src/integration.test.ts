import * as fs from 'fs/promises';
import * as os from 'os';
import * as path from 'path';
import { z } from 'zod';
import { promisify } from 'node:util';
import { exec, fork } from 'child_process';

import { JobScheduler, EmptyLogger } from '@timecrisis/timecrisis';
import { describe, it, expect, beforeEach, afterEach, vi } from 'vitest';

import {
  createStorage,
  defaultJob,
  defaultValues,
  longRunningJobDefinition,
  prepareEnvironment,
  resetEnvironment,
} from './test-helpers/defaults.js';
import { SQLiteJobStorage } from './adapter.js';

describe('SQLite Integration Tests', () => {
  let tempFolder: string;
  let tempDbPath: string;
  let storage: SQLiteJobStorage;
  let scheduler: JobScheduler;

  beforeEach(async () => {
    prepareEnvironment();

    // Create an in-memory database for testing
    tempFolder = await fs.mkdtemp(`${os.tmpdir()}${path.sep}`);
    tempDbPath = path.join(tempFolder, `${Date.now()}.db`);

    // Connect.
    const connection = await createStorage(tempDbPath);
    storage = connection.storage;
  });

  afterEach(async () => {
    // Clear all timers and mocks
    resetEnvironment();

    // Clean the data.
    await fs.rm(tempFolder, { recursive: true });
  });

  describe('Job Registration and Execution', () => {
    beforeEach(async () => {
      scheduler = new JobScheduler({
        storage,
        logger: new EmptyLogger(),
        worker: defaultValues.workerName,
        jobProcessingInterval: defaultValues.pollInterval,
        jobSchedulingInterval: defaultValues.pollInterval,
        maxConcurrentJobs: defaultValues.maxConcurrentJobs,
        jobLockTTL: defaultValues.jobLockTTL,
        leaderLockTTL: defaultValues.distributedLockTTL,
      });
      await scheduler.start();
    });

    afterEach(async () => {
      // Stop the scheduler first to clear intervals
      await scheduler.stop(true);

      await vi.advanceTimersByTimeAsync(1000);
      await storage.close();
    });

    it('should register and execute a job successfully', async () => {
      scheduler.registerJob(longRunningJobDefinition);
      const jobId = await scheduler.enqueue(longRunningJobDefinition.type, defaultJob.data);

      // Advance time to allow job processing
      await vi.advanceTimersByTimeAsync(defaultValues.pollInterval);
      await vi.advanceTimersByTimeAsync(defaultValues.longRunningJobDuration);

      expect(longRunningJobDefinition.handle).toHaveBeenCalledWith(
        defaultJob.data,
        expect.any(Object)
      );

      const job = await storage.getJob(jobId);
      expect(job?.status).toBe('completed');

      const jobRuns = await storage.listJobRuns(job!.id);
      expect(jobRuns.length).toBe(1);
      expect(jobRuns[0].status).toBe('completed');
      expect(jobRuns[0].executionDuration).toBe(defaultValues.longRunningJobDuration);
      expect(jobRuns[0].attempt).toBe(1);
      expect(jobRuns[0].startedAt).toBeInstanceOf(Date);
      expect(jobRuns[0].finishedAt).toBeInstanceOf(Date);
      expect(jobRuns[0].error).toBeUndefined();
      expect(jobRuns[0].errorStack).toBeUndefined();
      expect(jobRuns[0].progress).toBe(100);
    });

    it('should handle job failures and retries', async () => {
      const handleFn = vi
        .fn()
        .mockRejectedValueOnce(new Error('First attempt failed'))
        .mockRejectedValueOnce(new Error('Second attempt failed'))
        .mockResolvedValueOnce(undefined);

      const jobDefinition = {
        type: 'retry-job',
        concurrency: 10,
        schema: z.object({ data: z.string() }),
        handle: handleFn,
      };

      scheduler.registerJob(jobDefinition);
      const jobId = await scheduler.enqueue(
        'retry-job',
        { data: 'test' },
        {
          maxRetries: 3,
          backoffStrategy: 'linear',
        }
      );

      // Process first attempt
      await vi.advanceTimersByTimeAsync(defaultValues.pollInterval);
      await vi.advanceTimersByTimeAsync(100); // Wait for job to fail
      await vi.advanceTimersByTimeAsync(100); // Wait for status update
      let job = await storage.getJob(jobId);
      expect(job?.status).toBe('pending');
      expect(job?.failCount).toBe(1);

      // Process second attempt (linear backoff: 10 seconds)
      await vi.advanceTimersByTimeAsync(10000); // Wait for backoff
      await vi.advanceTimersByTimeAsync(defaultValues.pollInterval);
      await vi.advanceTimersByTimeAsync(100); // Wait for job to fail
      await vi.advanceTimersByTimeAsync(100); // Wait for status update
      job = await storage.getJob(jobId);
      expect(job?.status).toBe('pending');
      expect(job?.failCount).toBe(2);

      // Process final successful attempt (linear backoff: 10 seconds)
      await vi.advanceTimersByTimeAsync(20000); // Wait for backoff
      await vi.advanceTimersByTimeAsync(defaultValues.pollInterval);
      await vi.advanceTimersByTimeAsync(100); // Wait for job to succeed
      await vi.advanceTimersByTimeAsync(100); // Wait for status update
      job = await storage.getJob(jobId);
      expect(job?.status).toBe('completed');
      expect(handleFn).toHaveBeenCalledTimes(3);
    });

    it('should respect job concurrency limits', async () => {
      const maxConcurrent = 2;

      scheduler.registerJob({
        ...longRunningJobDefinition,
        type: 'concurrent-job',
        concurrency: maxConcurrent,
      });

      // Enqueue multiple jobs
      const jobIds = await Promise.all(
        Array.from({ length: 5 }, (_, i) =>
          scheduler.enqueue('concurrent-job', { id: `job-${i + 1}`, test: true })
        )
      );

      // Initial state check
      let jobs = await Promise.all(jobIds.map((id) => storage.getJob(id)));
      expect(jobs.every((job) => job?.status === 'pending')).toBe(true);

      // First batch: jobs 1 and 2
      jobs = await Promise.all(jobIds.map((id) => storage.getJob(id)));
      expect(jobs[0]?.status).toBe('pending');
      expect(jobs[1]?.status).toBe('pending');
      expect(jobs[2]?.status).toBe('pending');
      expect(jobs[3]?.status).toBe('pending');
      expect(jobs[4]?.status).toBe('pending');

      // Wait for lock acquisition and processing
      await vi.advanceTimersByTimeAsync(
        defaultValues.pollInterval + 5 * defaultValues.scatterInterval
      );
      jobs = await Promise.all(jobIds.map((id) => storage.getJob(id)));
      expect(jobs[0]?.status).toBe('running'); // Now the job should be running
      expect(jobs[1]?.status).toBe('running'); // Now the job should be running
      expect(jobs[2]?.status).toBe('pending');
      expect(jobs[3]?.status).toBe('pending');
      expect(jobs[4]?.status).toBe('pending');

      // Complete first batch
      await vi.advanceTimersByTimeAsync(defaultValues.longRunningJobDuration);
      jobs = await Promise.all(jobIds.map((id) => storage.getJob(id)));
      expect(jobs[0]?.status).toBe('completed');
      expect(jobs[1]?.status).toBe('completed');
      expect(jobs[2]?.status).toBe('pending');
      expect(jobs[3]?.status).toBe('pending');
      expect(jobs[4]?.status).toBe('pending');

      // Second batch running
      await vi.advanceTimersByTimeAsync(
        defaultValues.pollInterval + 3 * defaultValues.scatterInterval
      );
      jobs = await Promise.all(jobIds.map((id) => storage.getJob(id)));
      expect(jobs[0]?.status).toBe('completed');
      expect(jobs[1]?.status).toBe('completed');
      expect(jobs[2]?.status).toBe('running');
      expect(jobs[3]?.status).toBe('running');
      expect(jobs[4]?.status).toBe('pending');

      // Start final batch
      await vi.advanceTimersByTimeAsync(defaultValues.longRunningJobDuration);
      jobs = await Promise.all(jobIds.map((id) => storage.getJob(id)));
      expect(jobs[0]?.status).toBe('completed');
      expect(jobs[1]?.status).toBe('completed');
      expect(jobs[2]?.status).toBe('completed');
      expect(jobs[3]?.status).toBe('completed');
      expect(jobs[4]?.status).toBe('running');

      // Final batch complete
      await vi.advanceTimersByTimeAsync(defaultValues.pollInterval);
      await vi.advanceTimersByTimeAsync(defaultValues.longRunningJobDuration);
      jobs = await Promise.all(jobIds.map((id) => storage.getJob(id)));
      expect(jobs[0]?.status).toBe('completed');
      expect(jobs[1]?.status).toBe('completed');
      expect(jobs[2]?.status).toBe('completed');
      expect(jobs[3]?.status).toBe('completed');
      expect(jobs[4]?.status).toBe('completed');

      // Complete final batch
      jobs = await Promise.all(jobIds.map((id) => storage.getJob(id)));
      expect(jobs.every((job) => job?.status === 'completed')).toBe(true);
    });
  });

  describe('Scheduled Jobs', () => {
    beforeEach(async () => {
      scheduler = new JobScheduler({
        storage,
        logger: new EmptyLogger(),
        worker: defaultValues.workerName,
        jobProcessingInterval: defaultValues.pollInterval,
        jobSchedulingInterval: defaultValues.pollInterval,
        maxConcurrentJobs: defaultValues.maxConcurrentJobs,
        jobLockTTL: defaultValues.jobLockTTL,
        leaderLockTTL: defaultValues.distributedLockTTL,
      });
      await scheduler.start();
    });

    afterEach(async () => {
      // Stop the scheduler first to clear intervals
      await scheduler.stop(true);

      await vi.advanceTimersByTimeAsync(1000);
      await storage.close();
    });

    it('should schedule and execute recurring jobs', async () => {
      const handleFn = vi.fn();
      const jobDefinition = {
        type: 'scheduled-job',
        concurrency: 10,
        schema: z.object({ data: z.string() }),
        handle: handleFn,
      };

      scheduler.registerJob(jobDefinition);
      await scheduler.schedule(
        'test-schedule',
        'scheduled-job',
        {
          data: 'scheduled',
        },
        {
          scheduleType: 'cron',
          scheduleValue: '*/5 * * * *',
        }
      );

      await vi.advanceTimersByTimeAsync(300000); // 5 minutes
      await vi.advanceTimersByTimeAsync(defaultValues.pollInterval);

      expect(handleFn).toHaveBeenCalledWith({ data: 'scheduled' }, expect.any(Object));

      // Verify scheduled job record exists
      const scheduledJobs = await storage.listScheduledJobs();
      expect(scheduledJobs).toHaveLength(1);
      expect(scheduledJobs[0].name).toBe('test-schedule');
    });

    it('should handle scheduled job failures', async () => {
      const handleFn = vi.fn().mockRejectedValue(new Error('Scheduled job failed'));
      const jobDefinition = {
        type: 'failing-scheduled-job',
        concurrency: 10,
        schema: z.object({ data: z.string() }),
        handle: handleFn,
      };

      scheduler.registerJob(jobDefinition);
      await scheduler.schedule(
        'failing-schedule',
        'failing-scheduled-job',
        {
          data: 'scheduled',
        },
        {
          scheduleType: 'cron',
          scheduleValue: '*/5 * * * *',
        }
      );

      // Advance time to trigger scheduled job
      await vi.advanceTimersByTimeAsync(300000); // 5 minutes

      await vi.advanceTimersByTimeAsync(defaultValues.pollInterval);
      await vi.advanceTimersByTimeAsync(defaultValues.pollInterval);
      await vi.advanceTimersByTimeAsync(defaultValues.pollInterval);

      // Verify job was created and failed
      const jobs = await storage.listJobs({ status: ['failed'] });
      expect(jobs).toHaveLength(1);
      expect(jobs[0].type).toBe('failing-scheduled-job');
    });
  });

  describe('Job Progress and Logs', () => {
    beforeEach(async () => {
      scheduler = new JobScheduler({
        storage,
        logger: new EmptyLogger(),
        worker: defaultValues.workerName,
        jobProcessingInterval: defaultValues.pollInterval,
        jobSchedulingInterval: defaultValues.pollInterval,
        maxConcurrentJobs: defaultValues.maxConcurrentJobs,
        jobLockTTL: defaultValues.jobLockTTL,
        leaderLockTTL: defaultValues.distributedLockTTL,
      });
      await scheduler.start();
    });

    afterEach(async () => {
      // Stop the scheduler first to clear intervals
      await scheduler.stop(true);

      await vi.advanceTimersByTimeAsync(1000);
      await storage.close();
    });

    it('should track job progress and logs', async () => {
      const jobDefinition = {
        type: 'progress-job',
        schema: z.object({ steps: z.number() }),
        handle: async (
          data: { steps: number },
          ctx: {
            updateProgress: (value: number) => Promise<void>;
            persistLog(
              level: 'info' | 'warn' | 'error',
              message: string,
              metadata?: Record<string, unknown>
            ): Promise<void>;
          }
        ): Promise<void> => {
          for (let i = 0; i < data.steps; i++) {
            await ctx.updateProgress(((i + 1) / data.steps) * 100);
            await ctx.persistLog('info', `Completed step ${i + 1}`);
            await new Promise((resolve) => setTimeout(resolve, 100));
          }
        },
      };

      // eslint-disable-next-line @typescript-eslint/no-explicit-any
      scheduler.registerJob(jobDefinition as any);
      const jobId = await scheduler.enqueue('progress-job', { steps: 3 });

      await vi.advanceTimersByTimeAsync(defaultValues.pollInterval);

      // Process each step (100ms per step)
      for (let i = 0; i < 3; i++) {
        await vi.advanceTimersByTimeAsync(100);
      }

      // Wait for next poll interval to update status
      await vi.advanceTimersByTimeAsync(100);

      const job = await storage.getJob(jobId);
      expect(job?.status).toBe('completed');

      // Verify progress updates
      const runs = await storage.listJobRuns(jobId);
      expect(runs[0].progress).toBe(100);

      // Verify logs
      const logs = await storage.listJobLogs(jobId, runs[0].id);
      expect(logs).toHaveLength(4);
      expect(logs.map((log) => log.message)).toEqual([
        'Completed step 1',
        'Completed step 2',
        'Completed step 3',
        'Job completed successfully',
      ]);
    });
  });

  describe('Worker Failover', () => {
    beforeEach(async () => {
      const command =
        'tsup src/test-helpers/leader-worker.ts --format esm --dts --sourcemap --no-splitting';
      const execPromise = promisify(exec);
      const { stdout } = await execPromise(command);
      expect(stdout).toContain('Build success');
    });

    it('should handle worker failover and cleanup job type slots', async () => {
      resetEnvironment();

      // Create backup scheduler
      const backupScheduler = new JobScheduler({
        storage: storage,
        logger: new EmptyLogger(),
        worker: 'backup-worker',
        maxConcurrentJobs: 5,
        jobLockTTL: 1000,
        leaderLockTTL: 250,
        // Set this to a very high number to avoid the backup scheduler to pick up any work
        jobProcessingInterval: 30000000,
        jobSchedulingInterval: 30000000,
        scheduledJobMaxStaleAge: 60000,
        expiredJobCheckInterval: 1000,
        shutdownTimeout: 15000,
        workerHeartbeatInterval: 50,
        // Set worker inactive check interval to 100ms, allowing us to quickly spot when the leader dies.
        workerInactiveCheckInterval: 100,
      });

      // Wait for migrations to complete.
      await new Promise((resolve) => setTimeout(resolve, 250));

      // Start leader scheduler in separate process
      const leaderProcess = fork('./dist/leader-worker.js', [tempDbPath], {
        silent: true,
        execArgv: ['--loader', 'ts-node/esm'], // Correct loader flag for ESM
        env: {
          ...process.env,
          NODE_NO_WARNINGS: '1', // Suppress warnings if necessary
        },
      });

      // Uncomment in case of errors, this will print the stderr and stdout of the leader process to the console
      // leaderProcess.stderr?.pipe(process.stderr);
      // leaderProcess.stdout?.pipe(process.stdout);

      // Wait for the leader to start.
      await new Promise((resolve) => setTimeout(resolve, 250));

      // Register job type on backup
      backupScheduler.registerJob({
        type: 'concurrent-job',
        schema: z.object({ id: z.string() }),
        handle: async (): Promise<void> => {
          await new Promise((resolve) => setTimeout(resolve, 1000));
        },
        concurrency: 2,
      });

      // Enqueue multiple jobs using storage directly
      const jobIds = await Promise.all(
        Array.from({ length: 5 }, (_, i) =>
          backupScheduler.enqueue('concurrent-job', { id: `job-${i + 1}` }, { maxRetries: 3 })
        )
      );

      // Wait for jobs to be started by the leader.
      await new Promise((resolve) => setTimeout(resolve, 200));

      // Start backup scheduler and wait for it to also start its own timers.
      await backupScheduler.start();
      await new Promise((resolve) => setTimeout(resolve, 200));
      await new Promise((resolve) => setTimeout(resolve, 200));
      await new Promise((resolve) => setTimeout(resolve, 200));

      // Check if only 2 were started by the leader, matching the concurrency.
      let jobs = await Promise.all(jobIds.map((id) => storage.getJob(id)));
      expect(jobs[0]?.status).toBe('running');
      expect(jobs[1]?.status).toBe('running');
      expect(jobs[2]?.status).toBe('pending');
      expect(jobs[3]?.status).toBe('pending');
      expect(jobs[4]?.status).toBe('pending');

      // Kill the leader process
      leaderProcess.kill('SIGTERM');

      // Wait for the backup worker to take leadership and reset locks.
      await new Promise((resolve) => setTimeout(resolve, 1000));

      // Jobs should be reset to pending.
      jobs = await Promise.all(jobIds.map((id) => storage.getJob(id)));
      expect(jobs[0]?.status).toBe('pending');
      expect(jobs[1]?.status).toBe('pending');
      expect(jobs[2]?.status).toBe('pending');
      expect(jobs[3]?.status).toBe('pending');
      expect(jobs[4]?.status).toBe('pending');

      // Stop backup scheduler
      await backupScheduler.stop(true);
      await storage.close();
    });
  });
});
