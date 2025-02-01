import { z } from 'zod';
import Database from 'better-sqlite3';

import { SQLiteJobStorage } from '../adapter.js';
import { ConsoleLogger, JobScheduler } from '@timecrisis/timecrisis';

// Get database path from args
const dbPath = process.argv[2];
if (!dbPath) {
  throw new Error('Database path not provided');
}

// Initialize storage
const db = new Database(dbPath);
db.pragma('busy_timeout = 5000');
db.pragma('journal_mode = WAL');
const storage = new SQLiteJobStorage(db);
await storage.init({
  runMigrations: false,
});

const logger = new ConsoleLogger().child('leader-worker');

// Create the leader scheduler
const scheduler = new JobScheduler({
  storage,
  logger,
  worker: 'leader-worker',
  maxConcurrentJobs: 5,
  jobLockTTL: 1000,
  leaderLockTTL: 250,
  jobProcessingInterval: 100,
  jobSchedulingInterval: 100,
  scheduledJobMaxStaleAge: 60000,
  expiredJobCheckInterval: 1000,
  shutdownTimeout: 15000,
  workerHeartbeatInterval: 50,
  // Set worker inactive check interval to 100ms, allowing us to quickly spot when the leader dies.
  workerInactiveCheckInterval: 100,
});

// Register job type
scheduler.registerJob({
  type: 'concurrent-job',
  schema: z.object({ id: z.string() }),
  handle: async (): Promise<void> => {
    // Process for 1000ms to ensure jobs don't complete before leader dies
    await new Promise((resolve) => setTimeout(resolve, 1000));
  },
  concurrency: 2,
});

// Start the scheduler
await scheduler.start();

// Handle shutdown signals
process.on('SIGTERM', async () => {
  logger.error('Received SIGTERM, shutting down');
  process.exit(-1);
});

process.on('SIGINT', async () => {
  logger.error('Received SIGINT, shutting down');
  process.exit(-1);
});
