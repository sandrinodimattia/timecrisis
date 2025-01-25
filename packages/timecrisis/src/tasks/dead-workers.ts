import { Logger } from '../logger/index.js';
import { JobStorage } from '../storage/types.js';
import { LeaderElection } from '../leader/index.js';

interface DeadWorkersTaskConfig {
  /**
   * Logger.
   */
  logger: Logger;

  /**
   * Storage backend.
   */
  storage: JobStorage;

  /**
   * Leader election mechanism to determine the current leader.
   */
  leaderElection: LeaderElection;

  /**
   * The time in milliseconds after which a worker is considered dead if no heartbeat is received.
   */
  deadWorkerTimeout: number;

  /**
   * The interval in milliseconds at which to check for inactive workers.
   */
  cleanupInterval: number;
}

/**
 * Task which checks for and removes dead workers (workers that haven't sent a heartbeat).
 */
export class DeadWorkersTask {
  private timer: NodeJS.Timeout | null = null;

  private readonly cfg: DeadWorkersTaskConfig;

  constructor(config: DeadWorkersTaskConfig) {
    this.cfg = config;
  }

  /**
   * Start the dead workers task
   * This will begin checking for and removing inactive workers at the specified interval
   */
  async start(): Promise<void> {
    // Execute immediately on start
    await this.execute();

    // Start the check timer
    this.timer = setInterval(async () => {
      try {
        await this.execute();
      } catch (err) {
        this.cfg.logger.error(`Failed to execute dead workers check`, {
          error: err instanceof Error ? err.message : String(err),
          error_stack: err instanceof Error ? err.stack : undefined,
        });
      }
    }, this.cfg.cleanupInterval);
  }

  /**
   * Stop the dead workers task
   * This will stop checking for inactive workers
   */
  stop(): void {
    if (this.timer) {
      clearInterval(this.timer);
      this.timer = null;
    }
  }

  /**
   * Execute the task to check for and remove inactive workers.
   */
  async execute(): Promise<void> {
    // Only run this task if we are the leader
    if (!this.cfg.leaderElection.isCurrentLeader()) {
      return;
    }

    const now = new Date();
    const cutoff = new Date(now.getTime() - this.cfg.deadWorkerTimeout);

    try {
      // Get all workers that haven't sent a heartbeat since the cutoff
      const inactiveWorkers = await this.cfg.storage.getInactiveWorkers(cutoff);
      if (inactiveWorkers.length === 0) {
        return;
      }

      this.cfg.logger.info(`Found ${inactiveWorkers.length} inactive workers to remove`, {
        cutoff_time: cutoff.toISOString(),
        worker_count: inactiveWorkers.length,
      });

      // Remove each inactive worker
      for (const worker of inactiveWorkers) {
        try {
          await this.cfg.storage.deleteWorker(worker.id);
          this.cfg.logger.info(`Removed inactive worker`, {
            worker_id: worker.id,
            worker_name: worker.name,
            last_heartbeat: worker.last_heartbeat?.toISOString(),
          });
        } catch (err) {
          this.cfg.logger.error(`Failed to remove inactive worker`, {
            worker_id: worker.id,
            error: err instanceof Error ? err.message : String(err),
            error_stack: err instanceof Error ? err.stack : undefined,
          });
        }
      }
    } catch (err) {
      this.cfg.logger.error(`Failed to check for inactive workers`, {
        error: err instanceof Error ? err.message : String(err),
        error_stack: err instanceof Error ? err.stack : undefined,
      });
    }
  }
}
