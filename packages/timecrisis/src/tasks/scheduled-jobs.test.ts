import cronParser from 'cron-parser';
import { describe, it, expect, vi, beforeEach, afterEach } from 'vitest';

import { EmptyLogger } from '../logger';
import { MockJobStorage } from '../storage/mock';
import { ScheduledJob } from '../storage/schemas';
import { ScheduledJobsTask } from './scheduled-jobs';

describe('ScheduledJobsTask', () => {
  let storage: MockJobStorage;
  let enqueueJob: <T>(type: string, data: T) => Promise<void>;
  let task: ScheduledJobsTask;
  const now = new Date('2025-01-23T00:00:00.000Z');

  beforeEach(() => {
    vi.useFakeTimers();
    vi.setSystemTime(now);

    storage = new MockJobStorage();
    enqueueJob = vi.fn().mockResolvedValue(undefined);
    task = new ScheduledJobsTask(storage, enqueueJob);

    // Clear all mocks before each test
    vi.clearAllMocks();
  });

  afterEach(() => {
    // Clear all timers and mocks
    vi.clearAllTimers();
    vi.clearAllMocks();
    vi.useRealTimers();
  });

  it('should not schedule any jobs if none are enabled', async () => {
    vi.spyOn(storage, 'listScheduledJobs').mockResolvedValue([]);
    await task.execute();
    expect(storage.listScheduledJobs).toHaveBeenCalledWith({
      enabled: true,
      nextRunBefore: now,
    });
    expect(enqueueJob).not.toHaveBeenCalled();
  });

  it('should schedule an exact job correctly', async () => {
    const futureDate = new Date(now.getTime() + 60000); // 1 minute in the future
    const job: ScheduledJob = {
      id: 'job1',
      name: 'Job 1',
      type: 'test',
      data: { test: true },
      enabled: true,
      scheduleType: 'exact',
      scheduleValue: futureDate.toISOString(),
      nextRunAt: now,
      createdAt: now,
      updatedAt: now,
    };

    vi.spyOn(storage, 'listScheduledJobs').mockResolvedValue([job]);
    await task.execute();

    expect(enqueueJob).toHaveBeenCalledWith('test', { test: true });
    expect(storage.updateScheduledJob).toHaveBeenCalledWith('job1', {
      lastScheduledAt: expect.any(Date),
      enabled: false,
    });
  });

  it('should schedule an interval job correctly', async () => {
    const job: ScheduledJob = {
      id: 'job2',
      name: 'Job 2',
      type: 'test',
      data: { test: true },
      enabled: true,
      scheduleType: 'interval',
      scheduleValue: '1m',
      nextRunAt: now,
      createdAt: now,
      updatedAt: now,
    };

    vi.spyOn(storage, 'listScheduledJobs').mockResolvedValue([job]);
    await task.execute();

    expect(enqueueJob).toHaveBeenCalledWith('test', { test: true });
    expect(storage.updateScheduledJob).toHaveBeenCalledWith('job2', {
      lastScheduledAt: expect.any(Date),
      nextRunAt: expect.any(Date),
    });
  });

  it('should schedule a cron job correctly', async () => {
    const job: ScheduledJob = {
      id: 'job3',
      name: 'Job 3',
      type: 'test',
      data: { test: true },
      enabled: true,
      scheduleType: 'cron',
      scheduleValue: '*/5 * * * *',
      nextRunAt: now,
      createdAt: now,
      updatedAt: now,
    };

    vi.spyOn(storage, 'listScheduledJobs').mockResolvedValue([job]);
    await task.execute();

    const interval = cronParser.parseExpression(job.scheduleValue);
    const nextRun = interval.next().toDate();

    expect(enqueueJob).toHaveBeenCalledWith('test', { test: true });
    expect(storage.updateScheduledJob).toHaveBeenCalledWith('job3', {
      lastScheduledAt: expect.any(Date),
      nextRunAt: nextRun,
    });
  });

  it('should stop scheduling when job is disabled', async () => {
    const job: ScheduledJob = {
      id: 'job4',
      name: 'Job 4',
      type: 'test',
      data: { test: true },
      enabled: false,
      scheduleType: 'interval',
      scheduleValue: '1m',
      nextRunAt: now,
      createdAt: now,
      updatedAt: now,
    };

    vi.spyOn(storage, 'listScheduledJobs').mockResolvedValue([job]);
    await task.execute();

    expect(enqueueJob).not.toHaveBeenCalled();
  });

  it('should handle invalid schedule types', async () => {
    const job: ScheduledJob = {
      id: 'job5',
      name: 'Job 5',
      type: 'test',
      data: { test: true },
      enabled: true,
      scheduleType: 'invalid' as never,
      scheduleValue: '1m',
      nextRunAt: now,
      createdAt: now,
      updatedAt: now,
    };

    vi.spyOn(storage, 'listScheduledJobs').mockResolvedValue([job]);
    await task.execute();

    expect(storage.updateScheduledJob).not.toHaveBeenCalled();
  });

  it('should handle stale jobs by updating nextRunAt without executing', async () => {
    // Create a job that's stale (nextRunAt is older than maxStaleAge)
    const staleDate = new Date(now.getTime() - 6 * 60 * 1000); // 6 minutes ago (beyond default 5 min maxStaleAge)
    const job: ScheduledJob = {
      id: 'stale-job',
      name: 'Stale Job',
      type: 'test',
      data: { test: true },
      enabled: true,
      scheduleType: 'interval',
      scheduleValue: '1m',
      nextRunAt: staleDate,
      createdAt: now,
      updatedAt: now,
    };

    vi.spyOn(storage, 'listScheduledJobs').mockResolvedValue([job]);
    await task.execute();

    // Should not have executed the job
    expect(enqueueJob).not.toHaveBeenCalled();

    // Should have updated the nextRunAt
    expect(storage.updateScheduledJob).toHaveBeenCalledWith('stale-job', {
      nextRunAt: expect.any(Date),
    });
  });

  it('should respect custom maxStaleAge configuration', async () => {
    const customMaxStaleAge = 10 * 60 * 1000; // 10 minutes
    task = new ScheduledJobsTask(storage, enqueueJob, new EmptyLogger(), {
      maxStaleAge: customMaxStaleAge,
    });

    // Create a job that's stale by default standards but not by custom config
    const staleDate = new Date(now.getTime() - 7 * 60 * 1000); // 7 minutes ago
    const job: ScheduledJob = {
      id: 'custom-stale-job',
      name: 'Custom Stale Job',
      type: 'test',
      data: { test: true },
      enabled: true,
      scheduleType: 'interval',
      scheduleValue: '1m',
      nextRunAt: staleDate,
      createdAt: now,
      updatedAt: now,
    };

    vi.spyOn(storage, 'listScheduledJobs').mockResolvedValue([job]);
    await task.execute();

    // Should execute the job since it's not considered stale with custom config
    expect(enqueueJob).toHaveBeenCalledWith('test', { test: true });
  });

  it('should prevent concurrent execution', async () => {
    const job: ScheduledJob = {
      id: 'concurrent-job',
      name: 'Concurrent Job',
      type: 'test',
      data: { test: true },
      enabled: true,
      scheduleType: 'interval',
      scheduleValue: '1m',
      nextRunAt: now,
      createdAt: now,
      updatedAt: now,
    };

    vi.spyOn(storage, 'listScheduledJobs').mockResolvedValue([job]);

    // Start first execution
    const firstExecution = task.execute();

    // Try to start second execution immediately
    const secondExecution = task.execute();

    await Promise.all([firstExecution, secondExecution]);

    // Should only have checked for jobs once
    expect(storage.listScheduledJobs).toHaveBeenCalledTimes(1);
  });

  it('should handle errors during job execution and continue processing other jobs', async () => {
    const jobs: ScheduledJob[] = [
      {
        id: 'error-job',
        name: 'Error Job',
        type: 'test',
        data: { test: true },
        enabled: true,
        scheduleType: 'interval',
        scheduleValue: '1m',
        nextRunAt: now,
        createdAt: now,
        updatedAt: now,
      },
      {
        id: 'success-job',
        name: 'Success Job',
        type: 'test',
        data: { test: true },
        enabled: true,
        scheduleType: 'interval',
        scheduleValue: '1m',
        nextRunAt: now,
        createdAt: now,
        updatedAt: now,
      },
    ];

    vi.spyOn(storage, 'listScheduledJobs').mockResolvedValue(jobs);
    vi.mocked(enqueueJob).mockImplementationOnce(() => Promise.reject(new Error('Test error')));

    await task.execute();

    // Should have tried to process both jobs
    expect(enqueueJob).toHaveBeenCalledTimes(2);
  });

  it('should handle invalid schedule values', async () => {
    const invalidCronJob: ScheduledJob = {
      id: 'invalid-cron',
      name: 'Invalid Cron',
      type: 'test',
      data: { test: true },
      enabled: true,
      scheduleType: 'cron',
      scheduleValue: 'invalid cron',
      nextRunAt: now,
      createdAt: now,
      updatedAt: now,
    };

    vi.spyOn(storage, 'listScheduledJobs').mockResolvedValue([invalidCronJob]);
    await task.execute();

    expect(storage.updateScheduledJob).not.toHaveBeenCalled();
  });

  it('should run interval job repeatedly at configured interval', async () => {
    const job: ScheduledJob = {
      id: 'repeat-interval-job',
      name: 'Repeat Interval Job',
      type: 'test',
      data: { test: true },
      enabled: true,
      scheduleType: 'interval',
      scheduleValue: '1m', // Run every minute
      nextRunAt: now,
      createdAt: now,
      updatedAt: now,
    };

    // Mock storage to always return the job with updated nextRunAt
    vi.spyOn(storage, 'listScheduledJobs').mockImplementation(async () => {
      return [job];
    });

    // Run for 5 minutes and check if job runs each minute
    for (let minute = 0; minute < 5; minute++) {
      // Clear previous calls
      vi.clearAllMocks();

      // Run the task
      await task.execute();

      // Verify job was executed
      expect(enqueueJob).toHaveBeenCalledWith('test', { test: true });
      expect(enqueueJob).toHaveBeenCalledTimes(1);

      // Verify nextRunAt was updated to next minute
      expect(storage.updateScheduledJob).toHaveBeenCalledWith(job.id, {
        lastScheduledAt: expect.any(Date),
        nextRunAt: new Date(job.nextRunAt!.getTime() + 60000), // Next minute
      });

      // Advance time by one minute
      vi.setSystemTime(new Date(now.getTime() + (minute + 1) * 60000));

      // Update job's nextRunAt to match new time
      job.nextRunAt = new Date(now.getTime() + (minute + 1) * 60000);
    }
  });

  it('should run cron job repeatedly according to schedule', async () => {
    const job: ScheduledJob = {
      id: 'repeat-cron-job',
      name: 'Repeat Cron Job',
      type: 'test',
      data: { test: true },
      enabled: true,
      scheduleType: 'cron',
      scheduleValue: '*/1 * * * *', // Run every minute
      nextRunAt: now,
      createdAt: now,
      updatedAt: now,
    };

    // Mock storage to always return the job with updated nextRunAt
    vi.spyOn(storage, 'listScheduledJobs').mockImplementation(async () => {
      return [job];
    });

    // Run for 5 minutes and check if job runs each minute
    for (let minute = 0; minute < 5; minute++) {
      // Clear previous calls
      vi.clearAllMocks();

      // Run the task
      await task.execute();

      // Verify job was executed
      expect(enqueueJob).toHaveBeenCalledWith('test', { test: true });
      expect(enqueueJob).toHaveBeenCalledTimes(1);

      // Get next run time using cron parser
      const interval = cronParser.parseExpression(job.scheduleValue, {
        currentDate: new Date(now.getTime() + minute * 60000),
        tz: 'UTC',
      });
      const expectedNextRun = interval.next().toDate();

      // Verify nextRunAt was updated correctly
      expect(storage.updateScheduledJob).toHaveBeenCalledWith(job.id, {
        lastScheduledAt: expect.any(Date),
        nextRunAt: expectedNextRun,
      });

      // Advance time by one minute
      vi.setSystemTime(new Date(now.getTime() + (minute + 1) * 60000));

      // Update job's nextRunAt to match new time
      job.nextRunAt = new Date(now.getTime() + (minute + 1) * 60000);
    }
  });
});
