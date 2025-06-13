# Time Crisis Job Scheduler

[![npm version](https://badge.fury.io/js/timecrisis.svg)](https://badge.fury.io/js/timecrisis)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)

Core package for the Time Crisis job scheduler.

## Installation

```bash
npm i @timecrisis/timecrisis
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
    timeZone: 'Europe/Paris',
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

### Writing Long-Running & Interruptible Jobs

For jobs that might take a long time, you can check the `ctx.isShuttingDown` property to allow for graceful termination when the scheduler is stopping.

```typescript
async function videoProcessingHandler(data, ctx) {
  for (let i = 0; i <= 100; i++) {
    // Check for shutdown signal before starting a new chunk of work
    if (ctx.isShuttingDown) {
      await ctx.log('warn', 'Shutdown detected, stopping job early.');
      // Perform any necessary cleanup
      return;
    }

    await processChunk(i);
    await ctx.updateProgress(i);
  }
}
```

### High Availability & Clustering

Time Crisis is designed for distributed environments. You can run multiple instances of `JobScheduler` connected to the same database to achieve high availability. The schedulers will automatically perform leader election to ensure that singleton tasks (like cron scheduling and cleanup) are only executed by one "leader" instance at a time.

If the leader instance goes down, another instance will automatically take over leadership after the `leaderLockTTL` expires. You can hook into these events:

```typescript
const scheduler = new JobScheduler({
  // ...
  leaderLockTTL: 30000, // 30 seconds
  onLeadershipAcquired: () => {
    console.log('This instance is now the leader!');
  },
  onLeadershipLost: () => {
    console.log('This instance lost leadership.');
  },
});
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

### `JobScheduler`

When creating a new `JobScheduler` instance, you can pass a configuration object with the following options to fine-tune its behavior.

| Option                        | Type                               | Description                                                                                                                                                                | Default                       |
| ----------------------------- | ---------------------------------- | -------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | ----------------------------- |
| **Core Configuration**        |                                    |                                                                                                                                                                            |                               |
| `storage`                     | `JobStorage`                       | **Required.** The storage adapter instance for persisting all job data, locks, and worker information.                                                                     | _(none)_                      |
| `worker`                      | `string`                           | A unique identifier for this scheduler instance. Crucial for multi-node deployments.                                                                                       | `` `${hostname()}-${uuid}` `` |
| `logger`                      | `Logger`                           | An instance of a logger that conforms to the `Logger` interface (e.g., `PinoLogger`).                                                                                      | `new EmptyLogger()`           |
| **Concurrency & Locking**     |                                    |                                                                                                                                                                            |                               |
| `maxConcurrentJobs`           | `number`                           | The global maximum number of jobs this worker instance can process concurrently.                                                                                           | `20`                          |
| `jobLockTTL`                  | `number`                           | The time (in ms) a job is locked for when it's being processed. For long-running jobs, this lock must be renewed within this TTL using `ctx.touch()`.                      | `60000` (60 seconds)          |
| `leaderLockTTL`               | `number`                           | The time (in ms) the leader lock is valid. In a cluster, if the leader fails to renew its lock within this TTL, another instance will take over leadership.                | `30000` (30 seconds)          |
| **Task Intervals & Timing**   |                                    |                                                                                                                                                                            |                               |
| `jobProcessingInterval`       | `number`                           | How often (in ms) this worker checks the database for pending jobs to execute.                                                                                             | `5000` (5 seconds)            |
| `jobSchedulingInterval`       | `number`                           | How often (in ms) the **leader** instance checks for scheduled (cron/interval) jobs that are due to be enqueued.                                                           | `60000` (60 seconds)          |
| `expiredJobCheckInterval`     | `number`                           | How often (in ms) the **leader** instance checks for jobs with expired locks or `expiresAt` dates.                                                                         | `60000` (60 seconds)          |
| `workerHeartbeatInterval`     | `number`                           | How often (in ms) this worker sends a heartbeat to the database to signal that it is still alive.                                                                          | `15000` (15 seconds)          |
| `workerInactiveCheckInterval` | `number`                           | How often (in ms) the **leader** instance checks for and cleans up dead workers that have missed their heartbeats.                                                         | `60000` (60 seconds)          |
| **Behavior & Lifecycle**      |                                    |                                                                                                                                                                            |                               |
| `shutdownTimeout`             | `number`                           | On graceful shutdown (`stop(false)`), the maximum time (in ms) to wait for running jobs to complete before forcing an exit.                                                | `15000` (15 seconds)          |
| `scheduledJobMaxStaleAge`     | `number`                           | The maximum age (in ms) of a scheduled job's `nextRunAt` time. If a scheduler comes online and finds a job older than this, it will be skipped to prevent a backlog storm. | `3600000` (1 hour)            |
| **Event Hooks**               |                                    |                                                                                                                                                                            |                               |
| `onJobStarted`                | `(job: Job) => void`               | An optional callback invoked when a job enters the 'running' state.                                                                                                        | `undefined`                   |
| `onJobCompleted`              | `(job: Job) => void`               | An optional callback invoked when a job enters the 'completed' state.                                                                                                      | `undefined`                   |
| `onJobFailed`                 | `(job: Job, error: Error) => void` | An optional callback invoked when a job fails (either for a retry or permanently).                                                                                         | `undefined`                   |
| `onLeadershipAcquired`        | `() => void`                       | An optional callback invoked when this scheduler instance becomes the cluster leader.                                                                                      | `undefined`                   |
| `onLeadershipLost`            | `() => void`                       | An optional callback invoked when this scheduler instance loses cluster leadership.                                                                                        | `undefined`                   |

### `JobDefinition`

When you register a job with the scheduler, you provide a `JobDefinition` object. This object defines the job's type, its data schema, its execution logic, and its behavior.

| Option           | Type                                          | Description                                                                                                                                                                                         | Required / Default               |
| ---------------- | --------------------------------------------- | --------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | -------------------------------- |
| `type`           | `string`                                      | **Required.** A unique string identifier for this job type. This is used to enqueue jobs and find the correct handler.                                                                              | Required                         |
| `schema`         | `z.ZodObject`                                 | **Required.** A [Zod](https://zod.dev/) schema that defines and validates the data payload for this job type. This ensures the `data` argument in your `handle` function is fully type-safe.        | Required                         |
| `handle`         | `(data: T, ctx: JobContext) => Promise<void>` | **Required.** The `async` function containing your business logic. It receives the validated `data` payload and a `JobContext` object with utilities like logging (`ctx.log`) and progress updates. | Required                         |
| `concurrency`    | `number`                                      | **Required.** The maximum number of jobs of **this specific type** that can run concurrently across the _entire cluster_. This provides fine-grained control over resource-intensive tasks.         | Required                         |
| `priority`       | `number`                                      | The default priority for jobs of this type, from `-20` (lowest) to `20` (highest). Jobs with a higher priority are picked from the queue first. This can be overridden in the `enqueue` options.    | `1`                              |
| `forkMode`       | `boolean`                                     | If `true`, the `handle` function will be executed in a separate child process. Ideal for CPU-intensive, long-running, or risky tasks that could block the main event loop or crash the worker.      | `false`                          |
| `forkHelperPath` | `string`                                      | The file path to the worker script that will be forked if `forkMode` is enabled. This path must be resolvable by the running Node.js process.                                                       | Required if `forkMode` is `true` |

#### Example Job Definition

```typescript
import { z } from 'zod';
import { JobDefinition, JobContext } from 'timecrisis';

// Define the schema for the job's data payload
const videoProcessingSchema = z.object({
  videoId: z.string().uuid(),
  resolution: z.enum(['1080p', '720p', '480p']),
});

// Define the job handler function
async function handleVideoProcessing(data: z.infer<typeof videoProcessingSchema>, ctx: JobContext) {
  await ctx.log('info', `Starting video processing for ${data.videoId} at ${data.resolution}.`);

  // Simulate long-running work
  for (let i = 0; i <= 100; i += 10) {
    // Check if the scheduler is shutting down
    if (ctx.isShuttingDown) {
      await ctx.log('warn', 'Shutdown detected, stopping early.');
      return;
    }
    await new Promise((res) => setTimeout(res, 1000));
    await ctx.updateProgress(i);
  }

  await ctx.log('info', `Finished processing ${data.videoId}.`);
}

// Create the complete job definition object
export const videoProcessingJob: JobDefinition<typeof videoProcessingSchema> = {
  // --- Required ---
  type: 'video.process',
  schema: videoProcessingSchema,
  handle: handleVideoProcessing,
  concurrency: 2, // Only allow 2 video processing jobs to run at once across the entire cluster

  // --- Optional ---
  priority: -5, // Lower priority than other jobs
  // forkMode: true, // Uncomment to run in a separate process
  // forkHelperPath: './path/to/video-worker.js',
};
```

### `EnqueueOptions`

When you enqueue a job using `scheduler.enqueue()`, you can provide an optional third argument, an `EnqueueOptions` object, to customize the job's behavior and lifecycle.

| Option            | Type                          | Description                                                                                                                                                                                                    | Default                                                  |
| ----------------- | ----------------------------- | -------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | -------------------------------------------------------- |
| `priority`        | `number`                      | A priority level for this specific job instance, from `-20` (lowest) to `20` (highest). This overrides the default `priority` set in the `JobDefinition`. Jobs with higher priority are processed first.       | `JobDefinition`'s `priority` (or `1`)                    |
| `maxRetries`      | `number`                      | The maximum number of times this job will be retried if it fails. After exhausting all retries, the job is moved to the dead-letter queue.                                                                     | `JobDefinition`'s `maxRetries` (or `0`)                  |
| `backoffStrategy` | `'exponential'` \| `'linear'` | The strategy for calculating the delay between retries. `'exponential'` doubles the delay with each attempt, while `'linear'` uses a constant delay. This overrides the default `backoffStrategy`.             | `JobDefinition`'s `backoffStrategy` (or `'exponential'`) |
| `entityId`        | `string`                      | An optional identifier to associate this job with a specific entity (e.g., a user ID, a document ID). This can be useful for querying or grouping jobs related to the same entity.                             | `undefined`                                              |
| `expiresAt`       | `Date`                        | A specific `Date` object after which the job should be considered expired. If the job has not started by this time, it will be automatically failed and will not run. This is useful for time-sensitive tasks. | `undefined`                                              |
| `expiresIn`       | `string`                      | A duration string (e.g., `'5m'`, `'2h'`, `'10s'`) specifying how long from now until the job expires. This is a convenient alternative to `expiresAt`. If both are provided, `expiresIn` takes precedence.     | `undefined`                                              |

#### Example: Enqueueing a Job with Options

This example demonstrates how to enqueue a high-priority, time-sensitive job with a custom retry strategy.

```typescript
import { scheduler } from './your-scheduler-setup';

async function sendPasswordResetEmail(userId: string, email: string) {
  try {
    const jobId = await scheduler.enqueue(
      'send-email', // Job type
      {
        to: email,
        subject: 'Your Password Reset Link',
        body: 'Here is your link...',
      },
      {
        // --- Enqueue Options ---
        priority: 15, // This is an important email, process it quickly.
        maxRetries: 2, // Only try a couple of times.
        backoffStrategy: 'linear', // Use a simple 10-second delay between retries.
        entityId: userId, // Associate this job with the user's ID.
        expiresIn: '5m', // The password reset link is only valid for a short time.
      }
    );

    console.log(`Successfully enqueued password reset email with Job ID: ${jobId}`);
  } catch (error) {
    console.error('Failed to enqueue password reset email:', error);
  }
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
