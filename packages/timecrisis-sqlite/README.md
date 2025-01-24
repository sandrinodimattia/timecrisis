# TimeCrisis SQLite

[![npm version](https://badge.fury.io/js/timecrisis-sqlite.svg)](https://badge.fury.io/js/timecrisis-sqlite)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)

SQLite storage adapter for TimeCrisis job scheduler. Provides a production-ready, durable storage backend using SQLite.

## Installation

```bash
npm install timecrisis timecrisis-sqlite better-sqlite3
```

## Features

- 💾 Production-ready SQLite storage implementation
- 🔄 Automatic schema migrations
- 🔒 Transaction support
- 🚀 Efficient job querying and locking
- 🧹 Built-in cleanup for old jobs

## Usage

### Basic Setup

```typescript
import Database from 'better-sqlite3';
import { JobScheduler } from 'timecrisis';
import { SQLiteJobStorage } from 'timecrisis-sqlite';

// Create SQLite connection
const db = new Database('jobs.sqlite');

// Enable WAL mode for better concurrency
db.pragma('journal_mode = WAL');
db.pragma('busy_timeout = 5000');

// Create storage instance
const storage = new SQLiteJobStorage(db);
await storage.init(); // This runs migrations

// Create scheduler
const scheduler = new JobScheduler({
  storage,
  node: 'worker-1',
  pollInterval: 1000,
  maxConcurrentJobs: 5,
});

// Start scheduler
await scheduler.start();
```

### Production Configuration

For production use, configure SQLite for reliability:

```typescript
const db = new Database('jobs.sqlite', {
  // Verbose error messages
  verbose: process.env.NODE_ENV === 'development',

  // File system synchronization level
  fileMustExist: false,
});

// Performance and reliability settings
db.pragma('journal_mode = WAL');
db.pragma('synchronous = NORMAL');
db.pragma('busy_timeout = 5000');
db.pragma('foreign_keys = ON');
db.pragma('temp_store = MEMORY');
```

### Cleanup Configuration

Configure job retention periods:

```typescript
// Cleanup old jobs periodically
await storage.cleanup({
  // Keep completed jobs for 7 days
  jobRetention: 7,

  // Keep failed jobs for 14 days
  failedJobRetention: 14,

  // Keep dead letter jobs for 30 days
  deadLetterRetention: 30,
});
```

### Metrics

Get storage-specific metrics:

```typescript
const metrics = await storage.getMetrics();
console.log(metrics);
// {
//   jobs: {
//     total: 1000,
//     pending: 5,
//     completed: 900,
//     failed: 95,
//     deadLetter: 10,
//     scheduled: 50
//   },
//   averageDurationByType: {
//     'send-email': 245,
//     'process-video': 15000
//   },
//   failureRateByType: {
//     'send-email': 0.02,
//     'process-video': 0.15
//   }
// }
```

### Multiple Workers

The storage adapter supports multiple workers accessing the same database through SQLite's WAL mode and distributed locking:

```typescript
// Worker 1
const worker1 = new JobScheduler({
  storage: new SQLiteJobStorage(db1),
  node: 'worker-1',
  // Keep lock TTL short for faster failover
  jobLockTTL: 30000,
  leaderLockTTL: 30000,
});

// Worker 2
const worker2 = new JobScheduler({
  storage: new SQLiteJobStorage(db2),
  node: 'worker-2',
  jobLockTTL: 30000,
  leaderLockTTL: 30000,
});
```

### Database Maintenance

Regular maintenance tasks:

```typescript
// Vacuum database to reclaim space
db.pragma('vacuum');

// Analyze tables for query optimization
db.pragma('analyze');

// Optimize WAL file
db.pragma('wal_checkpoint(TRUNCATE)');

// Backup database
const backup = new Database('backup.sqlite');
db.backup(backup).then(() => {
  backup.close();
});
```

### Error Handling

The adapter provides detailed error types:

```typescript
try {
  await storage.createJob(job);
} catch (error) {
  if (error instanceof JobNotFoundError) {
    // Handle missing job
  } else if (error instanceof JobRunNotFoundError) {
    // Handle missing job run
  } else if (error instanceof ScheduledJobNotFoundError) {
    // Handle missing scheduled job
  }
}
```

### Transactions

All operations use transactions for consistency:

```typescript
await storage.transaction(async (trx) => {
  const jobId = await storage.createJob(job);
  await storage.createJobLog({
    jobId,
    level: 'info',
    message: 'Job created',
  });
});
```

## Performance Tips

WAL Configuration:

```typescript
// Larger WAL file for better write performance
db.pragma('page_size = 4096');
db.pragma('wal_autocheckpoint = 1000');
```

## Contributing

See the [main repository](https://github.com/sandrinodimattia/timecrisis) for contribution guidelines.
