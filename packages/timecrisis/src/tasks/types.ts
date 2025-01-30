import { Logger } from '../logger/index.js';
import { JobStorage } from '../storage/types.js';
import { JobDefinition } from '../scheduler/types.js';
import { JobStateMachine } from '../state-machine/index.js';
import { LeaderElection } from '../concurrency/leader-election.js';
import { DistributedLock } from '../concurrency/distributed-lock.js';
import { ConcurrencyManager } from '../concurrency/concurrency-manager.js';

export interface TaskContext {
  /**
   * Name of the worker that is performing the job.
   */
  worker: string;

  /**
   * Logger.
   */
  logger: Logger;

  /**
   * Storage backend.
   */
  storage: JobStorage;

  /**
   * Leader election process.
   */
  leaderElection: LeaderElection;

  /**
   * Job state machine.
   */
  stateMachine: JobStateMachine;

  /**
   * Concurrency manager.
   */
  concurrency: ConcurrencyManager;

  /**
   * Distributed lock.
   */
  lock: DistributedLock;

  /**
   * Job definitions.
   */
  jobs: Map<string, JobDefinition>;
}
