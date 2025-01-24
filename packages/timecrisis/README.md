# Time Crisis Job Scheduler

[![npm version](https://badge.fury.io/js/timecrisis.svg)](https://badge.fury.io/js/timecrisis)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)

Core package for the Time Crisis job scheduler.

## Installation

```bash
npm install timecrisis
```

## Features

- 🔒 Type-safe job definitions with Zod schema validation
- ⏰ Flexible scheduling (cron, interval, one-time)
- 🔄 Automatic retries with configurable backoff
- 🔐 Distributed locking for multi-node safety
- 📊 Comprehensive job logging and metrics
- 🔌 Pluggable storage system
- 🔧 Fork mode for isolation
- ⌛ Job expiration and TTL support

## Usage

### Basic Setup

```typescript
import { JobScheduler, InMemoryJobStorage, PinoLogger } from 'timecrisis';
import { z } from 'zod';

const scheduler = new JobScheduler({
  storage: new InMemoryJobStorage(),
  logger: new PinoLogger(),
  node: 'worker-1',
  pollInterval: 1000,
  maxConcurrentJobs: 5,
  jobLockTTL: 5000,
  leaderLockTTL: 5000,
});
```

### Defining Jobs

```typescript
const emailJob = {
  type: 'send-email',
  schema: z.object({
    to: z.string().email(),
    subject: z.string(),
    body: z.string(),
  }),
  // Optional concurrency limit per job type
  concurrency: 5,
  // Optional job priority
  priority: 10,
  // Handler function
  async handle(data, ctx) {
    await ctx.log('info', `Sending email to ${data.to}`);
    // Send email...
    await ctx.log('info', 'Email sent successfully');
  },
};

scheduler.registerJob(emailJob);
```

### Running Jobs

```typescript
// One-time immediate job
await scheduler.enqueue(
  'send-email',
  {
    to: 'user@example.com',
    subject: 'Welcome!',
    body: 'Hello world',
  },
  {
    priority: 5,
    maxRetries: 3,
    expiresIn: '1h',
  }
);

// Scheduled job (cron)
await scheduler.schedule(
  'daily-report',
  'send-email',
  {
    to: 'team@company.com',
    subject: 'Daily Report',
    body: 'Here is your report...',
  },
  {
    scheduleType: 'cron',
    scheduleValue: '0 0 * * *', // Daily at midnight
    enabled: true,
  }
);

// Interval job
await scheduler.schedule(
  'heartbeat',
  'send-email',
  {
    to: 'monitoring@company.com',
    subject: 'System Heartbeat',
    body: 'System is alive',
  },
  {
    scheduleType: 'interval',
    scheduleValue: '5m', // Every 5 minutes
    enabled: true,
  }
);
```

### Job Context

Jobs receive a context object with useful utilities:

```typescript
interface JobContext {
  // Unique job ID
  jobId: string;

  // Job payload
  payload: unknown;

  // Log a message
  log(
    level: 'info' | 'warn' | 'error',
    message: string,
    metadata?: Record<string, unknown>
  ): Promise<void>;

  // Keep job lock alive (for long-running jobs)
  touch(): Promise<void>;
}
```

### Fork Mode

For CPU-intensive or risky jobs, run them in a separate process:

```typescript
const heavyJob = {
  type: 'process-video',
  schema: z.object({
    videoId: z.string(),
  }),
  forkMode: true,
  forkHelperPath: './workers/video-processor.js',
  async handle(data, ctx) {
    // This runs in a separate process
    await processVideo(data.videoId);
  },
};
```

### Metrics

Track job execution statistics:

```typescript
const metrics = await scheduler.getMetrics();
console.log(metrics);
// {
//   running: 2,
//   pending: 5,
//   completed: 100,
//   failed: 3,
//   averageDuration: 1500,
//   types: {
//     'send-email': {
//       running: 1,
//       pending: 2,
//       completed: 50,
//       failed: 1
//     }
//   }
// }
```

### Custom Storage

Implement the `JobStorage` interface for custom storage:

```typescript
class MyCustomStorage implements JobStorage {
  async init(): Promise<void> {
    // Initialize storage
  }

  async createJob(job: CreateJob): Promise<string> {
    // Create job
  }

  async getJob(id: string): Promise<Job | null> {
    // Get job by ID
  }

  // ... implement other methods
}
```

## API Reference

### JobScheduler Options

```typescript
interface SchedulerOptions {
  // Storage backend
  storage: JobStorage;

  // Optional logger
  logger?: Logger;

  // Unique node identifier
  node?: string;

  // Maximum concurrent jobs
  maxConcurrentJobs?: number;

  // How often to check for new jobs (ms)
  pollInterval?: number;

  // How long a job can run before being considered stuck
  jobLockTTL?: number;

  // How long a leader lock is valid
  leaderLockTTL?: number;
}
```

### Job Definition

```typescript
interface JobDefinition<T extends z.ZodType> {
  // Unique job type
  type: string;

  // Zod schema for payload validation
  schema: T;

  // Job handler function
  handle: (data: z.infer<T>, ctx: JobContext) => Promise<void>;

  // Optional settings
  concurrency?: number;
  priority?: number;
  forkMode?: boolean;
  forkHelperPath?: string;
}
```

### Enqueue Options

```typescript
interface EnqueueOptions {
  // Maximum retry attempts
  maxRetries?: number;

  // Job priority (-20 to 20)
  priority?: number;

  // Custom reference ID for grouping jobs (eg: the user ID)
  referenceId?: string;

  // When the job expires
  expiresAt?: Date;

  // How long until expiration
  expiresIn?: string;

  // Retry backoff strategy
  backoffStrategy?: 'exponential' | 'linear';
}
```

## Error Handling

The library provides specific error classes:

```typescript
// Job type not found
throw new JobDefinitionNotFoundError('unknown-job');

// Duplicate job registration
throw new JobAlreadyRegisteredError('send-email');

// Lock acquisition failed
throw new DistributedLockError('Failed to acquire lock');
```

## Contributing

See the [main repository](https://github.com/sandrinodimattia/timecrisis) for contribution guidelines.
