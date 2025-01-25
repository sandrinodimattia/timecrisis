import { Logger } from '../logger/index.js';
import { JobStorage } from '../storage/types.js';

export interface StorageCleanupConfig {
  /**
   * Logger.
   */
  logger: Logger;

  /**
   * Storage backend.
   */
  storage: JobStorage;

  /**
   * Interval in milliseconds at which to run the cleanup.
   */
  pollInterval: number;

  /**
   * Number of days to retain completed jobs.
   */
  jobRetention: number;

  /**
   * Number of days to retain failed jobs.
   */
  failedJobRetention: number;

  /**
   * Number of days to retain dead letter jobs.
   */
  deadLetterRetention: number;
}

export class StorageCleanupTask {
  private isExecuting: boolean = false;
  private timer: NodeJS.Timeout | null = null;
  private readonly cfg: StorageCleanupConfig;
  private readonly logger: Logger;

  constructor(config: StorageCleanupConfig) {
    this.cfg = config;
    this.logger = config.logger.child('StorageCleanupTask');
  }

  /**
   * Start the storage cleanup task.
   */
  async start(): Promise<void> {
    // Start the check timer
    this.timer = setInterval(async () => {
      try {
        await this.execute();
      } catch (err) {
        this.cfg.logger.error(`Failed to execute storage cleanup`, {
          error: err instanceof Error ? err.message : String(err),
          error_stack: err instanceof Error ? err.stack : undefined,
        });
      }
    }, this.cfg.pollInterval);
  }

  /**
   * Stop the storage cleanup task.
   */
  stop(): void {
    if (this.timer) {
      clearInterval(this.timer);
      this.timer = null;
    }
  }

  /**
   * Execute the storage cleanup task.
   */
  public async execute(): Promise<void> {
    // Skip if already running
    if (this.isExecuting) {
      return;
    } else {
      this.isExecuting = true;
    }

    try {
      this.logger.debug('Starting storage cleanup');

      await this.cfg.storage.cleanup({
        jobRetention: this.cfg.jobRetention,
        failedJobRetention: this.cfg.failedJobRetention,
        deadLetterRetention: this.cfg.deadLetterRetention,
      });

      this.logger.debug('Storage cleanup completed');
    } catch (err) {
      this.logger.error('Error cleaning up jobs:', {
        error: err instanceof Error ? err.message : String(err),
        error_stack: err instanceof Error ? err.stack : undefined,
      });
    } finally {
      this.isExecuting = false;
    }
  }
}
