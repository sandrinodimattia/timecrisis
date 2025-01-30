import { Logger } from '../logger/index.js';
import { JobStorage } from '../storage/types.js';

interface WorkerAliveTaskConfig {
  /**
   * Logger.
   */
  logger: Logger;

  /**
   * Storage backend.
   */
  storage: JobStorage;

  /**
   * The name of the worker instance.
   */
  worker: string;

  /**
   * The interval in milliseconds at which to send heartbeats.
   */
  heartbeatInterval: number;
}

export class WorkerAliveTask {
  private worker: string | null = null;
  private timer: NodeJS.Timeout | null = null;

  private readonly cfg: WorkerAliveTaskConfig;

  constructor(config: WorkerAliveTaskConfig) {
    this.cfg = config;
  }

  /**
   * Execute the worker alive task
   * This will send a heartbeat for the current worker
   */
  async execute(): Promise<void> {
    try {
      if (!this.worker) {
        // Register the worker if not already registered
        this.worker = await this.cfg.storage.registerWorker({
          name: this.cfg.worker,
        });
        return;
      }

      await this.cfg.storage.updateWorkerHeartbeat(this.worker, {
        last_heartbeat: new Date(),
      });
    } catch (err) {
      this.cfg.logger.error(`Failed to update worker heartbeat`, {
        error: err instanceof Error ? err.message : String(err),
        error_stack: err instanceof Error ? err.stack : undefined,
      });
    }
  }

  /**
   * Start the worker alive task
   * This will register the worker and begin sending heartbeats
   */
  async start(): Promise<void> {
    // Execute immediately to register the worker
    await this.execute();

    // Start the heartbeat timer
    this.timer = setInterval(async () => {
      await this.execute();
    }, this.cfg.heartbeatInterval);
  }

  /**
   * Stop the worker alive task
   * This will stop sending heartbeats
   */
  stop(): void {
    if (this.timer) {
      clearInterval(this.timer);
      this.timer = null;
    }
  }

  /**
   * Get the ID of the registered worker
   * @returns The worker ID if registered, null otherwise
   */
  getWorkerName(): string | null {
    return this.worker;
  }
}
