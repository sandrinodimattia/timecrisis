import { Logger } from '../logger/index.js';

/**
 * Configuration for the global concurrency manager.
 */
export interface GlobalConcurrencyConfig {
  /**
   * Maximum number of concurrent jobs that can be running across the entire process.
   */
  maxConcurrentJobs: number;
}

/**
 * Manages global concurrency limits for job execution across the entire process.
 * Uses a simple token bucket approach where each running job consumes one token.
 */
export class GlobalConcurrencyManager {
  private logger: Logger;
  private maxConcurrentJobs: number;
  private runningJobs = new Set<string>();

  constructor(logger: Logger, config: GlobalConcurrencyConfig) {
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
   * false if we've hit the global concurrency limit.
   */
  public acquire(jobId: string): boolean {
    if (this.runningJobs.size >= this.maxConcurrentJobs) {
      this.logger.debug('Hit global concurrency limit', {
        jobId,
        currentCount: this.runningJobs.size,
        maxConcurrent: this.maxConcurrentJobs,
      });
      return false;
    }

    this.runningJobs.add(jobId);
    this.logger.debug('Acquired concurrency slot', {
      jobId,
      currentCount: this.runningJobs.size,
    });
    return true;
  }

  /**
   * Release a job's concurrency slot.
   */
  public release(jobId: string): void {
    if (this.runningJobs.has(jobId)) {
      this.runningJobs.delete(jobId);
      this.logger.debug('Released concurrency slot', {
        jobId,
        currentCount: this.runningJobs.size,
      });
    }
  }
}
