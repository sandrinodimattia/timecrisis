import { describe, it, expect, beforeEach, afterEach, vi } from 'vitest';

import {
  defaultJob,
  defaultJobDefinition,
  prepareEnvironment,
  resetEnvironment,
} from '../test-helpers/defaults.js';
import { EmptyLogger } from '../logger/index.js';
import { JobStorage } from '../storage/types.js';
import { JobDefinition } from '../scheduler/types.js';
import { JobState, JobStateMachine } from './index.js';
import { InvalidStateTransitionError } from './types.js';
import { MockJobStorage } from '../storage/mock/index.js';
import { InMemoryJobStorage } from '../storage/memory/index.js';

const stores = [
  { name: 'InMemoryJobStore', store: (): JobStorage => new InMemoryJobStorage() },
  { name: 'MockJobStore', store: (): JobStorage => new MockJobStorage() },
];

stores.forEach(({ name, store }) => {
  describe(`JobStateMachine with ${name}`, () => {
    let jobStore: ReturnType<typeof store>;
    let stateMachine: JobStateMachine;
    let jobs: Map<string, JobDefinition>;

    beforeEach(() => {
      prepareEnvironment();

      jobs = new Map();
      jobs.set(defaultJobDefinition.type, defaultJobDefinition);
      jobStore = store();
      stateMachine = new JobStateMachine({ storage: jobStore, jobs, logger: new EmptyLogger() });
    });

    afterEach(() => {
      resetEnvironment();
    });

    describe('job creation', () => {
      it('should create a new job in pending state', async () => {
        const jobId = await stateMachine.enqueue(defaultJobDefinition.type, defaultJob.data);
        const job = await jobStore.getJob(jobId);

        expect(job).toBeDefined();
        expect(job?.id).toBe(jobId);
        expect(job?.type).toBe(defaultJobDefinition.type);
        expect(job?.status).toBe(JobState.Pending);
        expect(job?.data).toEqual(defaultJob.data);
      });

      it('should validate job data against schema', async () => {
        await expect(
          stateMachine.enqueue(defaultJobDefinition.type, { invalid: 'data' })
        ).rejects.toThrow();
      });

      it('should throw for unknown job type', async () => {
        await expect(stateMachine.enqueue('unknown-job', { data: 'test' })).rejects.toThrow();
      });
    });

    describe('transitions', () => {
      it('should transition from pending to running', async () => {
        const jobId = await stateMachine.enqueue(defaultJobDefinition.type, defaultJob.data);

        const job = await jobStore.getJob(jobId);
        expect(job?.status).toBe(JobState.Pending);
        await stateMachine.start(job!);

        const updatedJob = await jobStore.getJob(jobId);
        expect(updatedJob?.status).toBe(JobState.Running);
      });

      it('should transition from running to completed', async () => {
        const jobId = await stateMachine.enqueue(defaultJobDefinition.type, defaultJob.data);
        const job = await jobStore.getJob(jobId);
        const { jobRunId } = await stateMachine.start(job!);
        await stateMachine.complete(job!, jobRunId);

        const updatedJob = await jobStore.getJob(jobId);
        expect(updatedJob?.status).toBe(JobState.Completed);
      });

      it('should transition from running to failed', async () => {
        const jobId = await stateMachine.enqueue(defaultJobDefinition.type, defaultJob.data);
        const job = await jobStore.getJob(jobId);
        const { jobRunId } = await stateMachine.start(job!);
        await stateMachine.fail(job!, jobRunId, false, 'Test error');

        const updatedJob = await jobStore.getJob(jobId);
        expect(updatedJob?.status).toBe(JobState.Failed);
        expect(updatedJob?.failReason).toBe('Test error');
      });

      it('should transition from failed to pending', async () => {
        const jobId = await stateMachine.enqueue(defaultJobDefinition.type, defaultJob.data, {
          maxRetries: 2,
        });
        let job = await jobStore.getJob(jobId);
        const { jobRunId } = await stateMachine.start(job!);

        job = await jobStore.getJob(jobId);
        await stateMachine.fail(job!, jobRunId, true, 'Test error');

        const updatedJob = await jobStore.getJob(jobId);
        expect(updatedJob?.status).toBe(JobState.Pending);
        expect(updatedJob?.failReason).toBeNull();
      });
    });

    describe('invalid transitions', () => {
      it('should not allow starting a completed job', async () => {
        const jobId = await stateMachine.enqueue(defaultJobDefinition.type, defaultJob.data);
        const job = await jobStore.getJob(jobId);
        const { jobRunId } = await stateMachine.start(job!);
        await stateMachine.complete(job!, jobRunId);

        await expect(stateMachine.start(job!)).rejects.toThrow(InvalidStateTransitionError);
      });

      it('should not allow completing a pending job', async () => {
        const jobId = await stateMachine.enqueue(defaultJobDefinition.type, defaultJob.data);
        const job = await jobStore.getJob(jobId);

        await expect(stateMachine.complete(job!, 'job-run-id')).rejects.toThrow(
          InvalidStateTransitionError
        );
      });
    });

    describe('metadata updates', () => {
      it('should update timestamps on state changes', async () => {
        const jobId = await stateMachine.enqueue(defaultJobDefinition.type, defaultJob.data);
        const job = await jobStore.getJob(jobId);

        const createdAt = job!.createdAt;

        vi.advanceTimersByTime(100);
        await stateMachine.start(job!);
        const updatedJob = await jobStore.getJob(jobId);

        expect(updatedJob?.createdAt).toEqual(createdAt);
        expect(updatedJob?.updatedAt.getTime()).toBeGreaterThan(createdAt.getTime());
      });

      it('should track attempts', async () => {
        const jobId = await stateMachine.enqueue(defaultJobDefinition.type, defaultJob.data, {
          maxRetries: 1,
        });

        let job = await jobStore.getJob(jobId);
        const { jobRunId } = await stateMachine.start(job!);
        await stateMachine.fail(job!, jobRunId, true, 'Test error');

        job = await jobStore.getJob(jobId);
        await stateMachine.start(job!);

        const runs = await jobStore.listJobRuns(job!.id);
        expect(runs).toHaveLength(2);
      });
    });

    describe('job runs and logs', () => {
      it('should create a job run when starting a job', async () => {
        const jobId = await stateMachine.enqueue(defaultJobDefinition.type, defaultJob.data);
        let job = await jobStore.getJob(jobId);
        const { jobRunId } = await stateMachine.start(job!);

        const jobRun = await jobStore.getJobRun(job!.id, jobRunId);
        expect(jobRun).toMatchObject({
          id: jobRunId,
          jobId: jobId,
          attempt: 1,
          status: JobState.Running,
          startedAt: expect.any(Date),
        });
      });

      it('should update job run status when completing a job', async () => {
        const jobId = await stateMachine.enqueue(defaultJobDefinition.type, defaultJob.data);
        let job = await jobStore.getJob(jobId);
        const { jobRunId } = await stateMachine.start(job!);
        await stateMachine.complete(job!, jobRunId);

        const jobRun = await jobStore.getJobRun(job!.id, jobRunId);
        expect(jobRun).toMatchObject({
          id: jobRunId,
          jobId: jobId,
          attempt: 1,
          executionDuration: expect.any(Number),
          status: JobState.Completed,
          startedAt: expect.any(Date),
          finishedAt: expect.any(Date),
        });
      });

      it('should update job run status and error when failing a job', async () => {
        const jobId = await stateMachine.enqueue(defaultJobDefinition.type, defaultJob.data);
        let job = await jobStore.getJob(jobId);
        const { jobRunId } = await stateMachine.start(job!);
        await stateMachine.fail(job!, jobRunId, false, 'Test failure reason');

        const jobRun = await jobStore.getJobRun(job!.id, jobRunId);
        expect(jobRun).toMatchObject({
          id: jobRunId,
          jobId: jobId,
          attempt: 1,
          status: JobState.Failed,
          executionDuration: expect.any(Number),
          startedAt: expect.any(Date),
          finishedAt: expect.any(Date),
          error: 'Test failure reason',
        });
      });

      it('should maintain job run history through retries', async () => {
        const jobId = await stateMachine.enqueue(defaultJobDefinition.type, defaultJob.data, {
          maxRetries: 2,
        });

        // First attempt
        let job = await jobStore.getJob(jobId);
        const { jobRunId: firstRunId } = await stateMachine.start(job!);
        await stateMachine.fail(job!, firstRunId, true, 'First error');

        // Second attempt
        job = await jobStore.getJob(jobId);
        const { jobRunId: secondRunId } = await stateMachine.start(job!);
        await stateMachine.fail(job!, secondRunId, true, 'Second error');

        const runs = await jobStore.listJobRuns(jobId);
        expect(runs).toHaveLength(2);
        expect(runs[0]).toMatchObject({
          id: firstRunId,
          attempt: 1,
          executionDuration: expect.any(Number),
          status: JobState.Failed,
          error: 'First error',
        });
        expect(runs[1]).toMatchObject({
          id: secondRunId,
          attempt: 2,
          executionDuration: expect.any(Number),
          status: JobState.Failed,
          error: 'Second error',
        });
      });

      it('should create job logs during job execution', async () => {
        const jobId = await stateMachine.enqueue(defaultJobDefinition.type, defaultJob.data);
        let job = await jobStore.getJob(jobId);
        const { jobRunId } = await stateMachine.start(job!);

        // Simulate some logs
        await jobStore.createJobLog({
          jobId: jobId,
          jobRunId,
          level: 'info',
          message: 'Starting job',
          timestamp: new Date(),
        });
        await jobStore.createJobLog({
          jobId: jobId,
          jobRunId,
          level: 'debug',
          message: 'Processing data',
          timestamp: new Date(),
        });
        await jobStore.createJobLog({
          jobId: jobId,
          jobRunId,
          level: 'info',
          message: 'Job completed',
          timestamp: new Date(),
        });

        const logs = await jobStore.listJobLogs(jobId, jobRunId);
        expect(logs).toHaveLength(3);
        expect(logs[0]).toMatchObject({
          jobRunId,
          level: 'info',
          message: 'Starting job',
          timestamp: expect.any(Date),
        });
        expect(logs[1]).toMatchObject({
          jobRunId,
          level: 'debug',
          message: 'Processing data',
        });
        expect(logs[2]).toMatchObject({
          jobRunId,
          level: 'info',
          message: 'Job completed',
        });
      });

      it('should maintain logs across multiple job runs', async () => {
        const jobId = await stateMachine.enqueue(defaultJobDefinition.type, defaultJob.data, {
          maxRetries: 3,
        });

        // First run
        let job = await jobStore.getJob(jobId);
        const { jobRunId: firstRunId } = await stateMachine.start(job!);
        await jobStore.createJobLog({
          jobId: jobId,
          jobRunId: firstRunId,
          level: 'error',
          message: 'First run failed',
          timestamp: new Date(),
        });
        await stateMachine.fail(job!, firstRunId, true, 'First error');

        // Second run
        job = await jobStore.getJob(jobId);
        const { jobRunId: secondRunId } = await stateMachine.start(job!);
        await jobStore.createJobLog({
          jobId: jobId,
          jobRunId: secondRunId,
          level: 'info',
          message: 'Second run started',
          timestamp: new Date(),
        });
        await stateMachine.complete(job!, secondRunId);

        const firstRunLogs = await jobStore.listJobLogs(jobId, firstRunId);

        expect(firstRunLogs).toHaveLength(2);
        expect(firstRunLogs[0]).toMatchObject({
          jobRunId: firstRunId,
          level: 'error',
          message: 'First run failed',
        });

        const secondRunLogs = await jobStore.listJobLogs(jobId, secondRunId);
        expect(secondRunLogs).toHaveLength(2);
        expect(secondRunLogs[0]).toMatchObject({
          jobRunId: secondRunId,
          level: 'info',
          message: 'Second run started',
        });
      });
    });

    describe('dead letter queue', () => {
      it('should move job to dead letter when max retries exceeded', async () => {
        const jobId = await stateMachine.enqueue(defaultJobDefinition.type, defaultJob.data, {
          maxRetries: 2,
        });

        // First attempt
        let job = await jobStore.getJob(jobId);
        let { jobRunId } = await stateMachine.start(job!);
        await stateMachine.fail(job!, jobRunId, true, 'First error');

        // Second attempt
        job = await jobStore.getJob(jobId);
        jobRunId = (await stateMachine.start(job!)).jobRunId;
        await stateMachine.fail(job!, jobRunId, true, 'Second error');

        // Third attempt (should go to dead letter)
        job = await jobStore.getJob(jobId);
        jobRunId = (await stateMachine.start(job!)).jobRunId;
        await stateMachine.fail(job!, jobRunId, true, 'Third error');

        job = await jobStore.getJob(jobId);
        expect(job).toMatchObject({
          status: JobState.Failed,
          failCount: 3,
          failReason: 'Third error',
        });

        const deadLetterJobs = await jobStore.listDeadLetterJobs();
        expect(deadLetterJobs).toHaveLength(1);
      });

      it('should move job to dead letter immediately when retry is disabled', async () => {
        const jobId = await stateMachine.enqueue(defaultJobDefinition.type, defaultJob.data, {});
        let job = await jobStore.getJob(jobId);
        const { jobRunId } = await stateMachine.start(job!);

        await stateMachine.fail(job!, jobRunId, false, 'Fatal error');

        job = await jobStore.getJob(jobId);
        expect(job).toMatchObject({
          status: JobState.Failed,
          failCount: 1,
          failReason: 'Fatal error',
        });

        const deadLetterJobs = await jobStore.listDeadLetterJobs();
        expect(deadLetterJobs).toHaveLength(1);
      });

      it('should maintain complete history in dead letter queue', async () => {
        const jobId = await stateMachine.enqueue(defaultJobDefinition.type, defaultJob.data, {
          maxRetries: 1,
        });

        // First attempt
        let job = await jobStore.getJob(jobId);
        let { jobRunId } = await stateMachine.start(job!);
        await jobStore.createJobLog({
          jobId,
          jobRunId,
          level: 'error',
          message: 'First attempt failed',
          timestamp: new Date(),
        });
        await stateMachine.fail(job!, jobRunId, true, 'First error');

        // Second attempt (will go to dead letter)
        job = await jobStore.getJob(jobId);
        jobRunId = (await stateMachine.start(job!)).jobRunId;
        await jobStore.createJobLog({
          jobId,
          jobRunId,
          level: 'error',
          message: 'Second attempt failed',
          timestamp: new Date(),
        });
        await stateMachine.fail(job!, jobRunId, true, 'Second error');

        // Verify final state
        job = await jobStore.getJob(jobId);
        expect(job).toMatchObject({
          status: JobState.Failed,
          failCount: 2,
        });

        // Verify runs are preserved
        const runs = await jobStore.listJobRuns(jobId);
        expect(runs).toHaveLength(2);
        expect(runs.map((r) => r.status)).toEqual([JobState.Failed, JobState.Failed]);

        // Verify logs are preserved
        const logs = await jobStore.listJobLogs(jobId);
        expect(logs).toHaveLength(4);
        expect(logs.map((l) => l.message)).toEqual([
          'First attempt failed',
          'Job failed, retrying in 10000 ms: First error',
          'Second attempt failed',
          'Job failed permanently after 2 attempts: Second error',
        ]);
      });

      it('should prevent starting a job in dead letter queue', async () => {
        const jobId = await stateMachine.enqueue(defaultJobDefinition.type, defaultJob.data);

        let job = await jobStore.getJob(jobId);
        const { jobRunId } = await stateMachine.start(job!);

        await stateMachine.fail(job!, jobRunId, false, 'Fatal error');

        job = await jobStore.getJob(jobId);
        await expect(stateMachine.start(job!)).rejects.toThrow(InvalidStateTransitionError);
      });
    });

    describe('field updates', () => {
      it('should set all fields correctly when creating a job', async () => {
        const jobId = await stateMachine.enqueue(defaultJobDefinition.type, defaultJob.data, {
          maxRetries: 3,
          priority: 5,
        });

        const job = await jobStore.getJob(jobId);
        expect(job).toMatchObject({
          id: jobId,
          type: defaultJobDefinition.type,
          data: defaultJob.data,
          status: JobState.Pending,
          priority: 5,
          maxRetries: 3,
          failCount: 0,
        });

        expect(job?.createdAt).toBeInstanceOf(Date);
        expect(job?.updatedAt).toBeInstanceOf(Date);
      });

      it('should update all relevant fields when starting a job', async () => {
        const jobId = await stateMachine.enqueue(defaultJobDefinition.type, defaultJob.data);
        let job = await jobStore.getJob(jobId);
        await stateMachine.start(job!);

        job = await jobStore.getJob(jobId);
        expect(job).toMatchObject({
          id: jobId,
          status: JobState.Running,
          startedAt: expect.any(Date),
        });
      });

      it('should update all relevant fields when failing a job', async () => {
        const jobId = await stateMachine.enqueue(defaultJobDefinition.type, defaultJob.data);
        let job = await jobStore.getJob(jobId);
        const { jobRunId } = await stateMachine.start(job!);

        await stateMachine.fail(job!, jobRunId, false, 'Test failure reason');

        job = await jobStore.getJob(jobId);
        expect(job).toMatchObject({
          id: jobId,
          status: JobState.Failed,
          startedAt: expect.any(Date),
          finishedAt: expect.any(Date),
          failReason: 'Test failure reason',
          failCount: 1,
        });
      });

      it('should update all relevant fields when completing a job', async () => {
        const jobId = await stateMachine.enqueue(defaultJobDefinition.type, defaultJob.data);
        let job = await jobStore.getJob(jobId);
        const { jobRunId } = await stateMachine.start(job!);

        await stateMachine.complete(job!, jobRunId);

        job = await jobStore.getJob(jobId);
        expect(job).toMatchObject({
          id: jobId,
          status: JobState.Completed,
          startedAt: expect.any(Date),
          finishedAt: expect.any(Date),
        });
      });

      it('should reset relevant fields when retrying a failed job', async () => {
        const jobId = await stateMachine.enqueue(defaultJobDefinition.type, defaultJob.data, {
          maxRetries: 3,
        });

        let job = await jobStore.getJob(jobId);
        const { jobRunId } = await stateMachine.start(job!);
        await stateMachine.fail(job!, jobRunId, true, 'Test error');

        job = await jobStore.getJob(jobId);
        expect(job).toMatchObject({
          id: jobId,
          status: JobState.Pending,
          failReason: null,
          failCount: 1,
        });
      });

      it('should maintain job options through transitions', async () => {
        const jobId = await stateMachine.enqueue(defaultJobDefinition.type, defaultJob.data, {
          maxRetries: 3,
          priority: 5,
        });

        let job = await jobStore.getJob(jobId);
        const { jobRunId } = await stateMachine.start(job!);
        await stateMachine.fail(job!, jobRunId, true, 'Test error');

        job = await jobStore.getJob(jobId);
        expect(job).toMatchObject({
          maxRetries: 3,
          priority: 5,
        });
      });

      it('should properly track multiple failure attempts', async () => {
        const jobId = await stateMachine.enqueue(defaultJobDefinition.type, defaultJob.data, {
          maxRetries: 3,
        });

        let job = await jobStore.getJob(jobId);

        // First attempt
        let { jobRunId } = await stateMachine.start(job!);
        await stateMachine.fail(job!, jobRunId, true, 'Error 1');

        // Second attempt
        job = await jobStore.getJob(jobId);
        jobRunId = (await stateMachine.start(job!)).jobRunId;
        await stateMachine.fail(job!, jobRunId, true, 'Error 2');

        const runs = await jobStore.listJobRuns(jobId);
        expect(runs.length).toBe(2);

        job = await jobStore.getJob(jobId);
        expect(job).toMatchObject({
          failCount: 2,
          status: JobState.Pending,
        });
      });
    });
  });
});
