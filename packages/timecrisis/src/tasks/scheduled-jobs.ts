import * as cronParser from 'cron-parser';

import { JobStorage } from '../storage/types.js';
import { parseDuration } from '../lib/duration.js';
import { ScheduledJob } from '../storage/schemas/index.js';
import { Logger, logger as defaultLogger } from '../logger/index.js';

export interface ScheduledJobsConfig {
  /**
   * Maximum age of a stale nextRunAt value in milliseconds
   * If a job's nextRunAt is older than this, it will be recalculated
   * Default: 5 minutes
   */
  maxStaleAge?: number;
}

export class ScheduledJobsTask {
  private storage: JobStorage;
  private maxStaleAge: number;
  private isExecuting: boolean = false;
  private enqueueJob: (type: string, data: unknown) => Promise<void>;
  private logger: Logger;

  constructor(
    storage: JobStorage,
    enqueueJob: (type: string, data: unknown) => Promise<void>,
    logger: Logger = defaultLogger,
    config: ScheduledJobsConfig = {}
  ) {
    this.storage = storage;
    this.enqueueJob = enqueueJob;
    this.logger = logger.child('ScheduledJobsTask');
    this.maxStaleAge = config.maxStaleAge || 5 * 60 * 1000; // 5 minutes default
  }

  /**
   * Check and process scheduled jobs that are due to run
   */
  public async execute(): Promise<void> {
    // Skip if already running
    if (this.isExecuting) {
      return;
    } else {
      this.isExecuting = true;
    }

    try {
      const now = new Date();

      // Get all enabled jobs that are due to run
      const scheduledJobs = await this.storage.listScheduledJobs({
        enabled: true,
        nextRunBefore: now,
      });

      this.logger.debug('Checking for scheduled jobs which are due to run', {
        scheduledJobs: scheduledJobs.length,
      });

      for (const job of scheduledJobs) {
        try {
          if (!job.enabled) {
            this.logger.debug('Skipping disabled job', {
              jobId: job.id,
              type: job.type,
            });
            continue;
          }

          // Check if the nextRunAt is stale
          const isStale =
            job.nextRunAt && now.getTime() - job.nextRunAt.getTime() > this.maxStaleAge;

          // If the job is stale, skip this execution and just update the next run time
          if (isStale) {
            this.logger.warn('Skipping stale job', {
              jobId: job.id,
              type: job.type,
              nextRunAt: job.nextRunAt,
              maxStaleAge: this.maxStaleAge,
            });

            const nextRun = this.getNextRunDate(job, now);
            if (nextRun) {
              this.logger.info('Updating job to simply run next time', {
                jobId: job.id,
                type: job.type,
                nextRunAt: nextRun,
              });

              await this.storage.updateScheduledJob(job.id, {
                nextRunAt: nextRun,
              });
            }
            continue;
          }

          this.logger.info('Enqueing job', {
            jobId: job.id,
            type: job.type,
            scheduleType: job.scheduleType,
            scheduleValue: job.scheduleValue,
          });

          // Execute the job
          await this.enqueueJob(job.type, job.data);

          // Update the last run time and calculate next run
          const executionTime = new Date();
          const updates: Partial<ScheduledJob> = {
            lastScheduledAt: executionTime,
          };

          // For exact schedules, disable after running once
          if (job.scheduleType === 'exact') {
            updates.enabled = false;
            this.logger.debug('Disabling job after running once', {
              jobId: job.id,
              type: job.type,
              scheduleType: job.scheduleType,
              scheduleValue: job.scheduleValue,
            });
          } else {
            // For interval and cron, calculate next run
            const nextRun = this.getNextRunDate(job, executionTime);
            if (nextRun) {
              this.logger.debug('Updating job schedule', {
                jobId: job.id,
                type: job.type,
                scheduleType: job.scheduleType,
                scheduleValue: job.scheduleValue,
                nextRunAt: nextRun,
              });
              updates.nextRunAt = nextRun;
            }
          }

          await this.storage.updateScheduledJob(job.id, updates);
        } catch (err) {
          this.logger.error(`Error processing scheduled job ${job.id}:`, {
            error: err instanceof Error ? err.message : String(err),
            error_stack: err instanceof Error ? err.stack : undefined,
          });
        }
      }
    } finally {
      this.isExecuting = false;
    }
  }

  /**
   * Get the next run date for a scheduled job.
   */
  private getNextRunDate(job: ScheduledJob, fromDate: Date = new Date()): Date | null {
    switch (job.scheduleType) {
      case 'exact': {
        const date = new Date(job.scheduleValue);
        return date > fromDate ? date : null;
      }
      case 'interval': {
        const interval = parseDuration(job.scheduleValue);
        return new Date(fromDate.getTime() + interval);
      }
      case 'cron': {
        const interval = cronParser.parseExpression(job.scheduleValue, {
          currentDate: fromDate,
          tz: 'UTC',
        });

        return interval.next().toDate();
      }
      default:
        throw new Error(`Unknown schedule type: ${job.scheduleType}`);
    }
  }
}
