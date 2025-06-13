import cronParser from 'cron-parser';
import { describe, it, expect, vi, beforeEach, afterEach } from 'vitest';

import {
  defaultJob,
  defaultJobDefinition,
  defaultValues,
  now,
  prepareEnvironment,
  resetEnvironment,
} from '../test-helpers/defaults.js';

import { EmptyLogger } from '../logger/index.js';
import { JobDefinition } from '../scheduler/types.js';
import { ScheduledJobsTask } from './scheduled-jobs.js';
import { MockJobStorage } from '../storage/mock/index.js';
import { JobStateMachine } from '../state-machine/index.js';
import { LeaderElection } from '../concurrency/leader-election.js';

describe('ScheduledJobsTask', () => {
  let storage: MockJobStorage;
  let task: ScheduledJobsTask;
  let leader: LeaderElection;
  let stateMachine: JobStateMachine;
  let jobs: Map<string, JobDefinition>;

  beforeEach(() => {
    prepareEnvironment();

    jobs = new Map();
    jobs.set(defaultJobDefinition.type, defaultJobDefinition);

    storage = new MockJobStorage();
    leader = new LeaderElection({
      logger: new EmptyLogger(),
      node: defaultValues.workerName,
      storage,
      lockTTL: defaultValues.distributedLockTTL,
    });
    vi.spyOn(leader, 'isCurrentLeader').mockReturnValue(true);

    stateMachine = new JobStateMachine({
      jobs,
      storage,
      logger: new EmptyLogger(),
    });
    vi.spyOn(stateMachine, 'enqueue');

    task = new ScheduledJobsTask({
      storage,
      logger: new EmptyLogger(),
      leaderElection: leader,
      stateMachine,
      pollInterval: defaultValues.pollInterval,
      scheduledJobMaxStaleAge: defaultValues.schedulerJobMaxStaleAge,
    });
  });

  afterEach(() => {
    resetEnvironment();
  });

  it('should not schedule any jobs if none are enabled', async () => {
    vi.spyOn(storage, 'listScheduledJobs').mockResolvedValue([]);
    await task.execute();
    expect(storage.listScheduledJobs).toHaveBeenCalledWith({
      enabled: true,
      nextRunBefore: now,
    });
    expect(stateMachine.enqueue).not.toHaveBeenCalled();
  });

  it('should schedule an exact job correctly', async () => {
    const scheduledJobId = await storage.createScheduledJob({
      name: 'Job 1',
      type: defaultJobDefinition.type,
      data: defaultJob.data,
      enabled: true,
      scheduleType: 'exact',
      scheduleValue: now.toISOString(),
      nextRunAt: now,
    });

    await vi.advanceTimersByTimeAsync(1000);
    await task.execute();

    expect(stateMachine.enqueue).toHaveBeenCalledWith(defaultJobDefinition.type, defaultJob.data, {
      scheduledJobId: scheduledJobId,
    });
    expect(storage.updateScheduledJob).toHaveBeenCalledWith(scheduledJobId, {
      lastScheduledAt: new Date(),
      enabled: false,
    });
  });

  it('should schedule an interval job correctly', async () => {
    const scheduledJobId = await storage.createScheduledJob({
      name: 'Job 1',
      type: defaultJobDefinition.type,
      data: defaultJob.data,
      enabled: true,
      scheduleType: 'interval',
      scheduleValue: '1m',
      nextRunAt: now,
    });

    await task.execute();

    expect(stateMachine.enqueue).toHaveBeenCalledWith(defaultJobDefinition.type, defaultJob.data, {
      scheduledJobId: scheduledJobId,
    });
    expect(storage.updateScheduledJob).toHaveBeenCalledWith(scheduledJobId, {
      nextRunAt: new Date(now.getTime() + 60000),
      lastScheduledAt: new Date(),
    });
  });

  it('should schedule a cron job correctly', async () => {
    const scheduledJobId = await storage.createScheduledJob({
      name: 'Job 1',
      type: defaultJobDefinition.type,
      data: defaultJob.data,
      enabled: true,
      scheduleType: 'cron',
      scheduleValue: '*/5 * * * *',
      nextRunAt: now,
    });

    await task.execute();

    const interval = cronParser.parseExpression('*/5 * * * *');
    const nextRun = interval.next().toDate();

    expect(stateMachine.enqueue).toHaveBeenCalledWith(defaultJobDefinition.type, defaultJob.data, {
      scheduledJobId: scheduledJobId,
    });
    expect(storage.updateScheduledJob).toHaveBeenCalledWith(scheduledJobId, {
      nextRunAt: nextRun,
      lastScheduledAt: new Date(),
    });
  });

  it('should stop scheduling when job is disabled', async () => {
    await storage.createScheduledJob({
      name: 'Job 1',
      type: defaultJobDefinition.type,
      data: defaultJob.data,
      enabled: false,
      scheduleType: 'cron',
      scheduleValue: '*/5 * * * *',
      nextRunAt: now,
    });

    await task.execute();

    expect(stateMachine.enqueue).not.toHaveBeenCalled();
  });

  it('should handle stale jobs by updating nextRunAt without executing', async () => {
    const scheduledJobId = await storage.createScheduledJob({
      name: 'Job 1',
      type: defaultJobDefinition.type,
      data: defaultJob.data,
      enabled: true,
      scheduleType: 'interval',
      scheduleValue: '1m',
      nextRunAt: new Date(now.getTime() - (defaultValues.schedulerJobMaxStaleAge + 10)),
    });

    await task.execute();

    // Should not have executed the job
    expect(stateMachine.enqueue).not.toHaveBeenCalled();

    // Should have updated the nextRunAt
    expect(storage.updateScheduledJob).toHaveBeenCalledWith(scheduledJobId, {
      nextRunAt: new Date(now.getTime() + 60000),
    });
  });

  it('should prevent concurrent execution', async () => {
    await storage.createScheduledJob({
      name: 'Job 1',
      type: defaultJobDefinition.type,
      data: defaultJob.data,
      enabled: true,
      scheduleType: 'interval',
      scheduleValue: '1m',
    });

    // Start first execution
    const firstExecution = task.execute();

    // Try to start second execution immediately
    const secondExecution = task.execute();

    await Promise.all([firstExecution, secondExecution]);

    // Should only have checked for jobs once
    expect(storage.listScheduledJobs).toHaveBeenCalledTimes(1);
  });

  it('should handle errors during job execution and continue processing other jobs', async () => {
    await storage.createScheduledJob({
      name: 'Job 1',
      type: defaultJobDefinition.type,
      data: defaultJob.data,
      enabled: true,
      scheduleType: 'interval',
      scheduleValue: '1m',
      nextRunAt: now,
    });
    await storage.createScheduledJob({
      name: 'Job 2',
      type: defaultJobDefinition.type,
      data: defaultJob.data,
      enabled: true,
      scheduleType: 'interval',
      scheduleValue: '1m',
      nextRunAt: now,
    });

    vi.mocked(stateMachine.enqueue).mockImplementationOnce(() =>
      Promise.reject(new Error('Test error'))
    );

    await task.execute();

    // Should have tried to process both jobs
    expect(stateMachine.enqueue).toHaveBeenCalledTimes(2);
  });

  it('should handle invalid schedule values', async () => {
    await storage.createScheduledJob({
      name: 'Job 1',
      type: defaultJobDefinition.type,
      data: defaultJob.data,
      enabled: true,
      scheduleType: 'cron',
      scheduleValue: 'invalid cron',
      nextRunAt: now,
    });

    await task.execute();

    expect(storage.updateScheduledJob).not.toHaveBeenCalled();
  });

  it('should run interval job repeatedly at configured interval', async () => {
    const scheduledJobId = await storage.createScheduledJob({
      name: 'Job 1',
      type: defaultJobDefinition.type,
      data: defaultJob.data,
      enabled: true,
      scheduleType: 'interval',
      scheduleValue: '1m', // Run every minute
      nextRunAt: now,
    });

    // Run for 5 minutes and check if job runs each minute
    for (let minute = 1; minute <= 5; minute++) {
      // Clear previous calls
      vi.clearAllMocks();

      // Run the task
      await task.execute();

      // Verify job was executed
      expect(stateMachine.enqueue).toHaveBeenCalledWith(
        'test',
        { test: true },
        {
          scheduledJobId: scheduledJobId,
        }
      );
      expect(stateMachine.enqueue).toHaveBeenCalledTimes(1);

      // Verify nextRunAt was updated to next minute
      expect(storage.updateScheduledJob).toHaveBeenCalledWith(scheduledJobId, {
        lastScheduledAt: new Date(),
        nextRunAt: new Date(new Date().getTime() + 60000),
      });

      // Advance time by one minute
      await vi.advanceTimersByTimeAsync(minute * 60000);
    }
  });

  it('should run cron job repeatedly according to schedule', async () => {
    const scheduledJobId = await storage.createScheduledJob({
      name: 'Job 1',
      type: defaultJobDefinition.type,
      data: defaultJob.data,
      enabled: true,
      scheduleType: 'cron',
      scheduleValue: '*/1 * * * *', // Run every minute
      nextRunAt: now,
    });

    // Run for 5 minutes and check if job runs each minute
    for (let minute = 1; minute <= 5; minute++) {
      // Clear previous calls
      vi.clearAllMocks();

      // Run the task
      await task.execute();

      // Verify job was executed
      expect(stateMachine.enqueue).toHaveBeenCalledWith(
        'test',
        { test: true },
        {
          scheduledJobId: scheduledJobId,
        }
      );
      expect(stateMachine.enqueue).toHaveBeenCalledTimes(1);

      // Verify nextRunAt was updated to next minute
      expect(storage.updateScheduledJob).toHaveBeenCalledWith(scheduledJobId, {
        lastScheduledAt: new Date(),
        nextRunAt: new Date(new Date().getTime() + 60000),
      });

      // Advance time by one minute
      await vi.advanceTimersByTimeAsync(minute * 60000);
    }
  });

  it('should only run cron job once per scheduled time even with quick execution', async () => {
    const scheduledJobId = await storage.createScheduledJob({
      name: 'Job 1',
      type: defaultJobDefinition.type,
      data: defaultJob.data,
      enabled: true,
      scheduleType: 'cron',
      scheduleValue: '*/1 * * * *', // Run every minute
      nextRunAt: now,
    });

    // First execution
    await task.execute();
    expect(stateMachine.enqueue).toHaveBeenCalledTimes(1);
    expect(storage.updateScheduledJob).toHaveBeenCalledWith(scheduledJobId, {
      lastScheduledAt: expect.any(Date),
      nextRunAt: expect.any(Date),
    });

    // Clear mocks
    vi.clearAllMocks();

    // Try to execute again immediately (simulating quick execution)
    await task.execute();

    // Should not have executed again
    expect(stateMachine.enqueue).not.toHaveBeenCalled();
    expect(storage.updateScheduledJob).not.toHaveBeenCalled();

    // Advance time to just before the next scheduled run
    await vi.advanceTimersByTimeAsync(59000); // 59 seconds

    // Try to execute again
    await task.execute();

    // Should not have executed yet
    expect(stateMachine.enqueue).not.toHaveBeenCalled();
    expect(storage.updateScheduledJob).not.toHaveBeenCalled();

    // Advance time to the next minute
    await vi.advanceTimersByTimeAsync(1000); // 1 second

    // Now it should execute
    await task.execute();
    expect(stateMachine.enqueue).toHaveBeenCalledTimes(1);
    expect(storage.updateScheduledJob).toHaveBeenCalledWith(scheduledJobId, {
      lastScheduledAt: expect.any(Date),
      nextRunAt: expect.any(Date),
    });
  });

  it('should handle cron jobs with specific minute scheduling', async () => {
    const scheduledJobId = await storage.createScheduledJob({
      name: 'Job 1',
      type: defaultJobDefinition.type,
      data: defaultJob.data,
      enabled: true,
      scheduleType: 'cron',
      scheduleValue: '29 * * * *', // Run at minute 29 of every hour
      nextRunAt: now,
    });

    // First execution
    await task.execute();
    expect(stateMachine.enqueue).toHaveBeenCalledTimes(1);
    const firstUpdate = vi.mocked(storage.updateScheduledJob).mock.calls[0][1];
    const nextRun = firstUpdate.nextRunAt;
    expect(nextRun).toBeDefined();

    // Clear mocks
    vi.clearAllMocks();

    // Try to execute again immediately
    await task.execute();

    // Should not have executed again
    expect(stateMachine.enqueue).not.toHaveBeenCalled();
    expect(storage.updateScheduledJob).not.toHaveBeenCalled();

    // Advance time to the next scheduled minute (29)
    const nextMinute = new Date(nextRun!);
    await vi.advanceTimersByTimeAsync(nextMinute.getTime() - Date.now());

    // Now it should execute
    await task.execute();
    expect(stateMachine.enqueue).toHaveBeenCalledTimes(1);
    expect(storage.updateScheduledJob).toHaveBeenCalledWith(scheduledJobId, {
      lastScheduledAt: expect.any(Date),
      nextRunAt: expect.any(Date),
    });
  });

  it('should use the highest date between lastScheduledAt and current time', async () => {
    const scheduledJobId = await storage.createScheduledJob({
      name: 'Job 1',
      type: defaultJobDefinition.type,
      data: defaultJob.data,
      enabled: true,
      scheduleType: 'cron',
      scheduleValue: '*/1 * * * *', // Run every minute
      nextRunAt: now,
      lastScheduledAt: new Date(now.getTime() + 30000), // 30 seconds in the future
    });

    // First execution - should not run because lastScheduledAt is in the future
    await task.execute();
    expect(stateMachine.enqueue).not.toHaveBeenCalled();
    expect(storage.updateScheduledJob).not.toHaveBeenCalled();

    // Clear mocks
    vi.clearAllMocks();

    // Advance time past lastScheduledAt
    await vi.advanceTimersByTimeAsync(31000); // 31 seconds

    // Now it should execute
    await task.execute();
    expect(stateMachine.enqueue).toHaveBeenCalledTimes(1);
    expect(storage.updateScheduledJob).toHaveBeenCalledWith(scheduledJobId, {
      lastScheduledAt: expect.any(Date),
      nextRunAt: expect.any(Date),
    });

    // Get the last scheduled time from the mock
    const lastUpdate = vi.mocked(storage.updateScheduledJob).mock.calls[0][1];
    const lastScheduledAt = lastUpdate.lastScheduledAt;
    expect(lastScheduledAt).toBeDefined();

    // Clear mocks
    vi.clearAllMocks();

    // Simulate system time going backwards by setting it to before lastScheduledAt
    const pastDate = new Date(lastScheduledAt!.getTime() - 1000); // 1 second before lastScheduledAt
    vi.setSystemTime(pastDate);

    // Try to execute again
    await task.execute();

    // Should not have executed because we're using the lastScheduledAt time
    expect(stateMachine.enqueue).not.toHaveBeenCalled();
    expect(storage.updateScheduledJob).not.toHaveBeenCalled();

    // Reset system time
    vi.useRealTimers();
  });
});
