import { Logger } from '../logger/index.js';
import { JobStorage } from '../storage/types.js';
import { WorkerTerminatedError } from '../scheduler/types.js';
import { LeaderElection } from '../concurrency/leader-election.js';
import { JobState, JobStateMachine } from '../state-machine/index.js';
import { formatLockName, getJobId, isJobLock } from '../concurrency/job-lock.js';

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
   * State machine.
   */
  stateMachine: JobStateMachine;

  /**
   * Leader election mechanism to determine the current leader.
   */
  leaderElection: LeaderElection;

  /**
   * The time in milliseconds after which a worker is considered dead if no heartbeat is received.
   */
  workerDeadTimeout: number;

  /**
   * The interval in milliseconds at which to check for inactive workers.
   */
  pollInterval: number;
}

/**
 * Task which checks for and removes dead workers (workers that haven't sent a heartbeat).
 */
export class DeadWorkersTask {
  private isExecuting: boolean = false;
  private timer: NodeJS.Timeout | null = null;

  private readonly logger: Logger;
  private readonly cfg: DeadWorkersTaskConfig;

  constructor(config: DeadWorkersTaskConfig) {
    this.cfg = config;
    this.logger = this.cfg.logger.child('dead-workers');
  }

  /**
   * Start the dead workers task
   * This will begin checking for and removing inactive workers at the specified interval
   */
  async start(): Promise<void> {
    // Execute immediately on start
    try {
      await this.execute();
    } catch (err) {
      this.logger.error(`Failed to execute dead workers check`, {
        error: err instanceof Error ? err.message : String(err),
        error_stack: err instanceof Error ? err.stack : undefined,
      });
    }

    // Start the check timer
    this.timer = setInterval(async () => {
      try {
        await this.execute();
      } catch (err) {
        this.logger.error(`Failed to execute dead workers check`, {
          error: err instanceof Error ? err.message : String(err),
          error_stack: err instanceof Error ? err.stack : undefined,
        });
      }
    }, this.cfg.pollInterval);
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

    // Skip if already running
    if (this.isExecuting) {
      return;
    } else {
      this.isExecuting = true;
    }

    const now = new Date();
    const cutoff = new Date(now.getTime() - this.cfg.workerDeadTimeout);

    try {
      // Get all workers that haven't sent a heartbeat since the cutoff
      const inactiveWorkers = await this.cfg.storage.getInactiveWorkers(cutoff);
      if (inactiveWorkers.length === 0) {
        return;
      }

      this.logger.info(`Found ${inactiveWorkers.length} inactive workers to remove`, {
        cutoff_time: cutoff.toISOString(),
        worker_count: inactiveWorkers.length,
      });

      // Remove each inactive worker
      for (const worker of inactiveWorkers) {
        try {
          // Find all jobs locked by this worker
          const locks = await this.cfg.storage.listLocks({ worker: worker.name });

          // Update locked jobs to be unlocked
          for (const jobId of locks
            .filter((l) => isJobLock(l.lockId))
            .map((l) => getJobId(l.lockId) as string)) {
            const job = await this.cfg.storage.getJob(jobId);

            // We only need to fail running jobs, pending jobs will be handled by the expired jobs task.
            if (job && job.status === JobState.Running) {
              // Get the current job run
              const runs = await this.cfg.storage.listJobRuns(job.id);
              const currentRun = runs.find((r) => r.status === JobState.Running);

              // Fail.
              const err = new WorkerTerminatedError(
                `Worker which was running the task is no longer active`
              );
              await this.cfg.stateMachine.fail(
                job,
                currentRun?.id,
                true,
                err,
                err.message,
                err.stack
              );
            }

            // And finally release the lock.
            await this.cfg.storage.releaseLock(formatLockName(jobId), worker.name);
          }

          // Remove concurrency locks
          await this.cfg.storage.releaseAllTypeSlots(worker.name);
          this.logger.info(`Released all job type slots for worker`, {
            worker_name: worker.name,
          });

          // Now remove the worker
          await this.cfg.storage.deleteWorker(worker.name);
          this.logger.info(`Removed inactive worker`, {
            worker_name: worker.name,
          });
        } catch (err) {
          this.logger.error(`Failed to remove inactive worker`, {
            error: err instanceof Error ? err.message : String(err),
            error_stack: err instanceof Error ? err.stack : undefined,
          });
        }
      }
    } catch (err) {
      this.logger.error(`Failed to check for inactive workers`, {
        error: err instanceof Error ? err.message : String(err),
        error_stack: err instanceof Error ? err.stack : undefined,
      });
    } finally {
      this.isExecuting = false;
    }
  }
}
