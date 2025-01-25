import { z } from 'zod';
import { describe, it, expect, beforeEach, vi } from 'vitest';

import { JobDefinition } from './types.js';
import { JobContextImpl } from './context.js';
import { JobStorage } from '../storage/types.js';

describe('JobContextImpl', () => {
  let mockStorage: JobStorage;
  let jobContext: JobContextImpl;
  let mockTouchFn: ReturnType<typeof vi.fn>;

  const testJobDefinition: JobDefinition = {
    type: 'test-job',
    schema: z.object({
      foo: z.string(),
      bar: z.number(),
    }),
    handle: async () => {},
  };

  beforeEach(() => {
    mockStorage = {
      updateJob: vi.fn(),
      updateJobRun: vi.fn(),
      createJobLog: vi.fn(),
    } as unknown as JobStorage;

    mockTouchFn = vi.fn();

    jobContext = new JobContextImpl(
      mockStorage,
      testJobDefinition,
      'job-123',
      'job-run-456',
      1,
      5,
      { foo: 'test', bar: 42 },
      mockTouchFn,
      new WeakRef({ isShuttingDown: false })
    );
  });

  describe('updateProgress', () => {
    it('should update progress in both job and job run', async () => {
      await jobContext.updateProgress(50);

      expect(mockStorage.updateJobRun).toHaveBeenCalledWith('job-run-456', {
        progress: 50,
      });
      expect(mockStorage.updateJob).toHaveBeenCalledWith('job-123', {
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
      expect(mockStorage.updateJobRun).toHaveBeenCalledWith('job-run-456', {
        progress: 0,
      });
      expect(mockStorage.updateJob).toHaveBeenCalledWith('job-123', {
        progress: 0,
      });

      await jobContext.updateProgress(100);
      expect(mockStorage.updateJobRun).toHaveBeenCalledWith('job-run-456', {
        progress: 100,
      });
      expect(mockStorage.updateJob).toHaveBeenCalledWith('job-123', {
        progress: 100,
      });
    });
  });

  describe('updateData', () => {
    it('should update data in parent job only', async () => {
      const newData = { foo: 'updated', bar: 43 };
      await jobContext.updateData(newData);

      expect(mockStorage.updateJob).toHaveBeenCalledWith('job-123', {
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

      expect(mockStorage.updateJob).toHaveBeenCalledWith('job-123', {
        data: validData,
      });
    });
  });

  describe('touch', () => {
    it('should call the touch function', async () => {
      await jobContext.touch();
      expect(mockTouchFn).toHaveBeenCalled();
    });
  });

  describe('logging', () => {
    it('should create log entries with correct job and run IDs', async () => {
      const message = 'Test log message';
      const metadata = { test: true };

      await jobContext.log('info', message, metadata);

      expect(mockStorage.createJobLog).toHaveBeenCalledWith({
        jobId: 'job-123',
        jobRunId: 'job-run-456',
        level: 'info',
        message,
        metadata,
        timestamp: expect.any(Date),
      });
    });

    it('should support different log levels', async () => {
      await jobContext.log('info', 'Info message');
      await jobContext.log('warn', 'Warning message');
      await jobContext.log('error', 'Error message');

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
      expect(jobContext.jobId).toBe('job-123');
    });

    it('should return correct payload', () => {
      expect(jobContext.payload).toEqual({ foo: 'test', bar: 42 });
    });
  });
});
