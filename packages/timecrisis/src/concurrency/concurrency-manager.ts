import { Logger } from '../logger/index.js';

/**
 * Configuration for the concurrency manager.
 */
export interface ConcurrencyConfig {
  /**
   * Maximum number of concurrent jobs that can be running across the entire process.
   */
  maxConcurrentJobs: number;
}

/**
 * Manages concurrency limits for job execution across the entire process.
 * Uses a simple token bucket approach where each running job consumes one token.
 */
export class ConcurrencyManager {
  public readonly maxConcurrentJobs: number;

  private readonly logger: Logger;
  private readonly runningJobs = new Set<string>();

  constructor(logger: Logger, config: ConcurrencyConfig) {
    this.logger = logger.child('global-concurrency');
    this.maxConcurrentJobs = config.maxConcurrentJobs;
  }

  /**
   * Get the current number of running jobs.
   */
  public getRunningCount(): number {
    return this.runningJobs.size;
  }

  /**
   * Check if we can run more jobs based on current concurrency.
   */
  public canRunMore(): boolean {
    return this.runningJobs.size < this.maxConcurrentJobs;
  }

  /**
   * Try to acquire a slot to run a job. Returns true if successful,
   * false if we've hit the concurrency limit.
   */
  public acquire(jobId: string): boolean {
    if (this.runningJobs.size >= this.maxConcurrentJobs) {
      this.logger.debug('Concurrency limit reached, job cannot be run', {
        job_id: jobId,
        current: this.runningJobs.size,
        max: this.maxConcurrentJobs,
      });
      return false;
    }

    this.runningJobs.add(jobId);
    this.logger.debug('Concurrency slot acquired', {
      job_id: jobId,
      current: this.runningJobs.size,
    });
    return true;
  }

  /**
   * Release a job's concurrency slot.
   */
  public release(jobId: string): void {
    if (this.runningJobs.has(jobId)) {
      this.runningJobs.delete(jobId);
      this.logger.debug('Concurrency slot released', {
        job_id: jobId,
        current: this.runningJobs.size,
      });
    }
  }
}
