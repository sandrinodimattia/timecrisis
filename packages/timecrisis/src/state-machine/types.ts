import { Logger } from '../logger/index.js';
import { JobEvent, JobState } from './index.js';
import { JobStorage } from '../storage/types.js';
import { JobDefinition } from '../scheduler/types.js';

export interface StateMachineConfig {
  /**
   * Logger.
   */
  logger: Logger;

  /**
   * Storage backend.
   */
  storage: JobStorage;

  /**
   * List of job definitions.
   */
  jobs: Map<string, JobDefinition>;
}

/**
 * Error thrown when an invalid state transition is attempted.
 */
export class InvalidStateTransitionError extends Error {
  constructor(currentState: JobState, event: JobEvent) {
    super(
      `Invalid state transition: cannot transition job in state "${currentState}" to "${event}"`
    );
    this.name = 'InvalidStateTransitionError';
  }
}
