import cronParser from 'cron-parser';

import { Logger } from '../logger/index.js';
import { JobStorage } from '../storage/types.js';
import { parseDuration } from '../lib/duration.js';
import { ScheduledJob } from '../storage/schemas/index.js';
import { JobStateMachine } from '../state-machine/index.js';
import { LeaderElection } from '../concurrency/leader-election.js';

export interface ScheduledJobsConfig {
  /**
   * Logger.
   */
  logger: Logger;

  /**
   * Storage backend.
   */
  storage: JobStorage;

  /**
   * State machine.
   */
  stateMachine: JobStateMachine;

  /**
   * Leader election process.
   */
  leaderElection: LeaderElection;

  /**
   * Maximum age of a stale nextRunAt value in milliseconds
   * If a job's nextRunAt is older than this, it will be skipped
   */
  scheduledJobMaxStaleAge: number;

  /**
   * Interval in milliseconds at which to check for scheduled jobs.
   */
  pollInterval: number;
}

export class ScheduledJobsTask {
  private isExecuting: boolean = false;
  private timer: NodeJS.Timeout | null = null;
  private readonly cfg: ScheduledJobsConfig;
  private readonly logger: Logger;

  constructor(config: ScheduledJobsConfig) {
    this.cfg = config;
    this.logger = config.logger.child('scheduled-jobs');
  }

  /**
   * Start task to plan for scheduled jobs.
   */
  async start(): Promise<void> {
    // Start the check timer
    this.timer = setInterval(async () => {
      try {
        await this.execute();
      } catch (err) {
        this.cfg.logger.error(`Failed to execute scheduled jobs planning`, {
          error: err instanceof Error ? err.message : String(err),
          error_stack: err instanceof Error ? err.stack : undefined,
        });
      }
    }, this.cfg.pollInterval);
  }

  /**
   * Stop the scheduled jobs planning task.
   */
  stop(): void {
    if (this.timer) {
      clearInterval(this.timer);
      this.timer = null;
    }
  }

  /**
   * Check and process scheduled jobs that are due to run.
   */
  public async execute(): Promise<void> {
    // Only run this task if we are the leader
    if (!this.cfg.leaderElection.isCurrentLeader()) {
      return;
    }

    // Skip if already running
    if (this.isExecuting) {
      return;
    } else {
      this.isExecuting = true;
    }

    try {
      const now = new Date();

      // Get all enabled jobs that are due to run
      const scheduledJobs = await this.cfg.storage.listScheduledJobs({
        enabled: true,
        nextRunBefore: now,
      });

      this.logger.debug('Checking for scheduled jobs which are due to run', {
        scheduled_jobs: scheduledJobs.length,
      });

      for (const job of scheduledJobs) {
        try {
          if (!job.enabled) {
            this.logger.debug('Skipping disabled job', {
              job_id: job.id,
              type: job.type,
            });
            continue;
          }

          // Check if the nextRunAt is stale
          const isStale =
            job.nextRunAt &&
            now.getTime() - job.nextRunAt.getTime() > this.cfg.scheduledJobMaxStaleAge;

          // If the job is stale, skip this execution and just update the next run time
          if (isStale) {
            this.logger.warn('Skipping stale job', {
              job_id: job.id,
              type: job.type,
              next_run_at: job.nextRunAt,
              max_stale_age: this.cfg.scheduledJobMaxStaleAge,
            });

            const nextRun = this.getNextRunDate(job, now);
            if (nextRun) {
              this.logger.info('Updating job to simply run next time', {
                job_id: job.id,
                type: job.type,
                next_run_at: nextRun,
              });

              await this.cfg.storage.updateScheduledJob(job.id, {
                nextRunAt: nextRun,
              });
            }
            continue;
          }

          // For cron jobs, check if we should run based on lastScheduledAt
          if (job.scheduleType === 'cron' && job.lastScheduledAt && job.lastScheduledAt > now) {
            this.logger.debug('Skipping job because lastScheduledAt is in the future', {
              job_id: job.id,
              type: job.type,
              last_scheduled_at: job.lastScheduledAt,
              now,
            });
            continue;
          }

          this.logger.info('Enqueing job', {
            job_id: job.id,
            type: job.type,
            schedule_type: job.scheduleType,
            schedule_value: job.scheduleValue,
            time_zone: job.timeZone,
          });

          // Execute the job
          await this.cfg.stateMachine.enqueue(job.type, job.data, {
            scheduledJobId: job.id,
            referenceId: job.referenceId,
          });

          // Update the last run time and calculate next run
          const executionTime = new Date();
          const updates: Partial<ScheduledJob> = {
            lastScheduledAt: executionTime,
          };

          // For exact schedules, disable after running once
          if (job.scheduleType === 'exact') {
            updates.enabled = false;
            this.logger.debug('Disabling job after running once', {
              job_id: job.id,
              type: job.type,
              schedule_type: job.scheduleType,
              schedule_value: job.scheduleValue,
            });
          } else {
            // For interval and cron, calculate next run
            const nextRun = this.getNextRunDate(job, executionTime);
            if (nextRun) {
              this.logger.debug('Updating job schedule', {
                job_id: job.id,
                type: job.type,
                schedule_type: job.scheduleType,
                schedule_value: job.scheduleValue,
                time_zone: job.timeZone,
                next_run_at: nextRun,
              });
              updates.nextRunAt = nextRun;
            } else if (job.scheduleType === 'cron') {
              // If we couldn't calculate the next run time for a cron job, skip execution
              this.logger.warn('Skipping job due to invalid cron expression', {
                job_id: job.id,
                type: job.type,
                schedule_value: job.scheduleValue,
              });
              continue;
            }
          }

          await this.cfg.storage.updateScheduledJob(job.id, updates);
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
        try {
          const interval = cronParser.parseExpression(job.scheduleValue, {
            currentDate: fromDate,
            tz: job.timeZone || 'UTC',
            iterator: true,
          });

          const next = interval.next();
          if (next.done) {
            return null;
          }
          return next.value.toDate();
        } catch (error) {
          this.logger.error('Failed to parse cron expression', {
            error: error instanceof Error ? error.message : String(error),
            schedule_value: job.scheduleValue,
            time_zone: job.timeZone,
          });
          return null;
        }
      }
      default:
        throw new Error(`Unknown schedule type: ${job.scheduleType}`);
    }
  }
}
