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

    expect(stateMachine.enqueue).toHaveBeenCalledWith(defaultJobDefinition.type, defaultJob.data);
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

    expect(stateMachine.enqueue).toHaveBeenCalledWith(defaultJobDefinition.type, defaultJob.data);
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

    expect(stateMachine.enqueue).toHaveBeenCalledWith(defaultJobDefinition.type, defaultJob.data);
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
      expect(stateMachine.enqueue).toHaveBeenCalledWith('test', { test: true });
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
      expect(stateMachine.enqueue).toHaveBeenCalledWith('test', { test: true });
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
});
