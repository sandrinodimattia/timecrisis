/**
 * Format a job ID into a lock name.
 * @param jobId The ID of the job.
 * @returns The formatted lock name.
 */
export function formatLockName(jobId: string): string {
  return `timecrisis/job/${jobId}`;
}

/**
 * Checks if a lock name is a job lock.
 * @param name The name of the lock.
 * @returns `true` if the name is a job lock, `false` otherwise.
 */
export function isJobLock(name: string): boolean {
  return name.startsWith('timecrisis/job/');
}

/**
 * Extracts the job ID from a lock name.
 * @param name The name of the lock.
 * @returns The job ID, or `undefined` if the name is not a job lock.
 */
export function getJobId(name: string): string | undefined {
  if (!isJobLock(name)) {
    return undefined;
  }
  return name.split('/').pop();
}
