# 🕰️ TimeCrisis Job Scheduler

[![Build Status](https://github.com/sandrinodimattia/timecrisis/workflows/CI/badge.svg)](https://github.com/sandrinodimattia/timecrisis/actions)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)
[![npm version](https://badge.fury.io/js/timecrisis.svg)](https://badge.fury.io/js/timecrisis)
[![TypeScript](https://img.shields.io/badge/%3C%2F%3E-TypeScript-%230074c1.svg)](https://www.typescriptlang.org/)

A robust, type-safe job scheduling system for Node.js applications. Built for reliability, scalability, and developer experience.

![TimeCrisis](timecrisis.jpg)

> [TimeCrisis](https://en.wikipedia.org/wiki/Time_Crisis_II) is also a popular arcade game made by Namco in 1997.

## 📦 Packages

| Package                                             | Description                                       | Version                                                                                                      |
| --------------------------------------------------- | ------------------------------------------------- | ------------------------------------------------------------------------------------------------------------ |
| [`timecrisis`](./packages/timecrisis)               | Core job scheduling system with in-memory storage | [![npm version](https://badge.fury.io/js/timecrisis.svg)](https://badge.fury.io/js/timecrisis)               |
| [`timecrisis-sqlite`](./packages/timecrisis-sqlite) | SQLite storage adapter for TimeCrisis             | [![npm version](https://badge.fury.io/js/timecrisis-sqlite.svg)](https://badge.fury.io/js/timecrisis-sqlite) |

## ✨ Features

- 🎯 **Type-safe**: Built with TypeScript for robust job definitions and payload validation
- 🔄 **Flexible Scheduling**: Support for cron, intervals, and one-time jobs
- 🔒 **Distributed Locking**: Safe execution in multi-node environments
- 🔄 **Automatic Retries**: Configurable retry strategies with exponential/linear backoff
- 📊 **Comprehensive Monitoring**: Built-in metrics, logging, and job history
- 🚀 **Scalable**: Handles high throughput with concurrent job execution
- 💾 **Storage Agnostic**: Pluggable storage backend (memory, SQLite included)
- 🔌 **Extensible**: Easy to add custom storage adapters and job types

## 🚀 Quick Start

```bash
# Install core package
npm install timecrisis

# Optional: Install SQLite adapter
npm install timecrisis-sqlite
```

Basic usage example:

```typescript
import { JobScheduler, InMemoryJobStorage } from 'timecrisis';
import { z } from 'zod';

// Create scheduler instance
const scheduler = new JobScheduler({
  storage: new InMemoryJobStorage(),
  node: 'worker-1',
  pollInterval: 1000,
  maxConcurrentJobs: 5,
});

// Define job type
const emailJob = {
  type: 'send-email',
  schema: z.object({
    to: z.string().email(),
    subject: z.string(),
    body: z.string(),
  }),
  async handle(data, ctx) {
    await ctx.log('info', `Sending email to ${data.to}`);
    // ... email sending logic
  },
};

// Register and start
scheduler.registerJob(emailJob);
await scheduler.start();

// Schedule a job
await scheduler.enqueue('send-email', {
  to: 'user@example.com',
  subject: 'Welcome!',
  body: 'Hello world',
});
```

### Using SQLite Storage

```typescript
import { JobScheduler } from 'timecrisis';
import { SQLiteJobStorage } from 'timecrisis-sqlite';
import Database from 'better-sqlite3';
import { z } from 'zod';

// Create SQLite database with WAL mode
const db = new Database('jobs.sqlite');
db.pragma('journal_mode = WAL');
db.pragma('busy_timeout = 5000');

// Initialize storage with migrations
const storage = new SQLiteJobStorage(db);
await storage.init();

// Create scheduler with SQLite storage
const scheduler = new JobScheduler({
  storage,
  node: 'worker-1',
  pollInterval: 1000,
  maxConcurrentJobs: 5,
  jobLockTTL: 30000,
  leaderLockTTL: 30000,
});

// Define recurring job
const reportJob = {
  type: 'generate-report',
  schema: z.object({
    reportType: z.enum(['daily', 'weekly']),
    format: z.enum(['pdf', 'csv']),
    recipients: z.array(z.string().email()),
  }),
  async handle(data, ctx) {
    await ctx.log('info', `Generating ${data.reportType} report`);
    // ... report generation logic
  },
};

// Register job
scheduler.registerJob(reportJob);

// Schedule recurring daily report
await scheduler.schedule(
  'daily-report',
  'generate-report',
  {
    reportType: 'daily',
    format: 'pdf',
    recipients: ['team@company.com'],
  },
  {
    scheduleType: 'cron',
    scheduleValue: '0 0 * * *', // Every day at midnight
    enabled: true,
  }
);

// Schedule weekly report
await scheduler.schedule(
  'weekly-report',
  'generate-report',
  {
    reportType: 'weekly',
    format: 'csv',
    recipients: ['management@company.com'],
  },
  {
    scheduleType: 'cron',
    scheduleValue: '0 0 * * 1', // Every Monday at midnight
    enabled: true,
  }
);

// Start the scheduler
await scheduler.start();

// Get metrics
const metrics = await storage.getMetrics();
console.log('Current job stats:', metrics.jobs);
```

## 📚 Documentation

See individual package READMEs for detailed documentation:

- [TimeCrisis Core Documentation](https://github.com/sandrinodimattia/timecrisis/tree/main/packages/timecrisis/README.md)
- [TimeCrisis SQLite Documentation](https://github.com/sandrinodimattia/timecrisis/tree/main/packages/timecrisis-sqlite/README.md)

## 🛠️ Development

```bash
# Install dependencies
pnpm install

# Build all packages
pnpm build

# Run tests
pnpm test

# Run tests with coverage
pnpm test:coverage

# Lint code
pnpm lint
```
