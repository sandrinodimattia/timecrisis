import { z } from 'zod';
import { describe, it, expect, beforeEach } from 'vitest';

import { JobDefinition } from './types.js';
import { JobContextImpl } from './context.js';
import { JobStorage } from '../storage/types.js';
import { EmptyLogger } from '../logger/index.js';
import { MockJobStorage } from '../storage/mock/index.js';
import { defaultJob, defaultValues, now } from '../test-helpers/defaults.js';

describe('JobContextImpl', () => {
  let mockStorage: JobStorage;
  let jobContext: JobContextImpl;
  let jobId: string;
  let jobRunId: string;

  const testJobDefinition: JobDefinition = {
    type: 'test-job',
    schema: z.object({
      foo: z.string(),
      bar: z.number(),
    }),
    handle: async () => {},
    concurrency: 2,
  };

  beforeEach(async () => {
    mockStorage = new MockJobStorage();

    jobId = await mockStorage.createJob({ ...defaultJob, data: { foo: 'test', bar: 42 } });
    jobRunId = await mockStorage.createJobRun({
      jobId,
      status: 'running',
      startedAt: now,
      attempt: 1,
    });

    jobContext = new JobContextImpl(
      new EmptyLogger(),
      mockStorage,
      testJobDefinition,
      defaultValues.workerName,
      defaultValues.jobLockTTL,
      jobId,
      jobRunId,
      1,
      5,
      { foo: 'test', bar: 42 },
      new WeakRef({ isShuttingDown: false })
    );
  });

  describe('updateProgress', () => {
    it('should update progress in both job and job run', async () => {
      await jobContext.updateProgress(50);

      expect(mockStorage.updateJobRun).toHaveBeenCalledWith(jobRunId, {
        progress: 50,
      });
    });

    it('should throw error if progress is less than 0', async () => {
      await expect(jobContext.updateProgress(-1)).rejects.toThrow(
        'Progress value must be between 0 and 100'
      );
    });

    it('should throw error if progress is greater than 100', async () => {
      await expect(jobContext.updateProgress(101)).rejects.toThrow(
        'Progress value must be between 0 and 100'
      );
    });

    it('should accept progress at boundaries (0 and 100)', async () => {
      await jobContext.updateProgress(0);
      expect(mockStorage.updateJobRun).toHaveBeenCalledWith(jobRunId, {
        progress: 0,
      });

      await jobContext.updateProgress(100);
      expect(mockStorage.updateJobRun).toHaveBeenCalledWith(jobRunId, {
        progress: 100,
      });
    });
  });

  describe('updateData', () => {
    it('should update data in parent job only', async () => {
      const newData = { foo: 'updated', bar: 43 };
      await jobContext.updateData(newData);

      expect(mockStorage.updateJob).toHaveBeenCalledWith(jobId, {
        data: newData,
      });
      expect(mockStorage.updateJobRun).not.toHaveBeenCalled();
    });

    it('should validate data against job definition schema', async () => {
      const invalidData = { foo: 123, bar: 'invalid' };
      // eslint-disable-next-line @typescript-eslint/no-explicit-any
      await expect(jobContext.updateData(invalidData as any)).rejects.toThrow();
    });

    it('should accept valid data according to schema', async () => {
      const validData = { foo: 'valid', bar: 42 };
      await jobContext.updateData(validData);

      expect(mockStorage.updateJob).toHaveBeenCalledWith(jobId, {
        data: validData,
      });
    });
  });

  describe('touch', () => {
    it('should call the touch function', async () => {
      await jobContext.touch();
      expect(mockStorage.renewLock).toHaveBeenCalled();
    });
  });

  describe('logging', () => {
    it('should create log entries with correct job and run IDs', async () => {
      const message = 'Test log message';
      const metadata = { test: true };

      await jobContext.persistLog('info', message, metadata);

      expect(mockStorage.createJobLog).toHaveBeenCalledWith({
        jobId,
        jobRunId,
        level: 'info',
        message,
        metadata,
        timestamp: expect.any(Date),
      });
    });

    it('should support different log levels', async () => {
      await jobContext.persistLog('info', 'Info message');
      await jobContext.persistLog('warn', 'Warning message');
      await jobContext.persistLog('error', 'Error message');

      expect(mockStorage.createJobLog).toHaveBeenCalledTimes(3);
      expect(mockStorage.createJobLog).toHaveBeenCalledWith(
        expect.objectContaining({ level: 'info' })
      );
      expect(mockStorage.createJobLog).toHaveBeenCalledWith(
        expect.objectContaining({ level: 'warn' })
      );
      expect(mockStorage.createJobLog).toHaveBeenCalledWith(
        expect.objectContaining({ level: 'error' })
      );
    });
  });

  describe('getters', () => {
    it('should return correct jobId', () => {
      expect(jobContext.jobId).toBe(jobId);
    });

    it('should return correct payload', () => {
      expect(jobContext.payload).toEqual({ foo: 'test', bar: 42 });
    });
  });
});
