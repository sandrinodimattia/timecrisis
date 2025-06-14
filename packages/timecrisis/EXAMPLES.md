# Job Definition Examples

This document provides a collection of `JobDefinition` examples to illustrate how to configure jobs for various use cases with Time Crisis.

## 1. Basic Job

This is the simplest form of a job definition. It includes the required properties: `type`, `schema`, `handle`, and `concurrency`. This job is for sending a welcome email to a new user. It runs with default settings for priority and retries.

```typescript
import { z } from 'zod';
import { JobDefinition, JobContext } from 'timecrisis';

const sendWelcomeEmailSchema = z.object({
  userId: z.string().uuid(),
  email: z.string().email(),
});

async function handleSendWelcomeEmail(
  data: z.infer<typeof sendWelcomeEmailSchema>,
  ctx: JobContext
) {
  await ctx.persistLog('info', `Sending welcome email to ${data.email}`);
  // Your email sending logic here
  await new Promise((res) => setTimeout(res, 500)); // Simulate network delay
  await ctx.persistLog('info', 'Welcome email sent successfully.');
}

export const sendWelcomeEmailJob: JobDefinition<typeof sendWelcomeEmailSchema> = {
  type: 'email.send-welcome',
  schema: sendWelcomeEmailSchema,
  handle: handleSendWelcomeEmail,
  concurrency: 10, // Allow 10 welcome emails to be sent at once.
};
```

## 2. High-Priority Job with Retries

This example defines a job for processing a payment, which is a high-priority task. It's configured with:

- A high `priority` of `1` to ensure it's picked up before other jobs.
- `maxRetries` set to `5` because payment processing can sometimes fail due to transient network issues.
- A `'linear'` `backoffStrategy` for consistent, predictable delays between retries.

```typescript
import { z } from 'zod';
import { JobDefinition, JobContext } from 'timecrisis';

const processPaymentSchema = z.object({
  orderId: z.string().uuid(),
  amount: z.number().positive(),
});

async function handleProcessPayment(data: z.infer<typeof processPaymentSchema>, ctx: JobContext) {
  await ctx.persistLog('info', `Processing payment for order ${data.orderId}`);
  try {
    // Your payment processing logic here
    // This might fail, triggering a retry
    await somePaymentGateway.charge(data.amount);
    await ctx.persistLog('info', 'Payment successful.');
  } catch (error) {
    await ctx.persistLog('error', 'Payment processing failed.', { error });
    throw error; // Re-throw to signal failure to the scheduler
  }
}

export const processPaymentJob: JobDefinition<typeof processPaymentSchema> = {
  type: 'payment.process',
  schema: processPaymentSchema,
  handle: handleProcessPayment,
  concurrency: 5, // Process up to 5 payments concurrently.
  priority: 1, // High priority
  maxRetries: 5,
  backoffStrategy: 'linear',
};
```

## 3. Long-Running, Interruptible Job

This job definition is for a task that takes a long time, such as generating a large report. Key features include:

- A `lockTTL` of `'10m'` (10 minutes) to prevent the job from being marked as "stuck" prematurely. The handler must call `ctx.touch()` periodically if it runs longer than this.
- The `handle` function checks `ctx.isShuttingDown` to allow for graceful termination.
- `ctx.updateProgress` is used to provide real-time feedback on the job's progress.

```typescript
import { z } from 'zod';
import { JobDefinition, JobContext } from 'timecrisis';

const generateReportSchema = z.object({
  reportId: z.string().uuid(),
  filters: z.record(z.any()),
});

async function handleGenerateReport(data: z.infer<typeof generateReportSchema>, ctx: JobContext) {
  await ctx.persistLog('info', `Generating report ${data.reportId}`);
  const totalSteps = 100;

  for (let step = 1; step <= totalSteps; step++) {
    if (ctx.isShuttingDown) {
      await ctx.persistLog('warn', 'Shutdown signal received, aborting report generation.');
      return; // Exit gracefully
    }

    // Simulate one piece of work
    await new Promise((res) => setTimeout(res, 1000));
    await ctx.updateProgress((step / totalSteps) * 100);

    // Every 5 minutes, touch the job to keep the lock alive
    if (step % 300 === 0) {
      await ctx.touch();
    }
  }

  await ctx.persistLog('info', `Report ${data.reportId} generated successfully.`);
}

export const generateReportJob: JobDefinition<typeof generateReportSchema> = {
  type: 'report.generate',
  schema: generateReportSchema,
  handle: handleGenerateReport,
  concurrency: 2, // Generating reports is resource-intensive.
  lockTTL: '10m', // Allow up to 10 minutes for this job to run.
};
```

## 4. Forked Job for CPU-Intensive Tasks

For tasks that are CPU-bound or carry a risk of crashing the process, you can run them in a separate process using `forkMode`.

- `forkMode: true` tells the scheduler to execute this job in a child process.
- `forkHelperPath` must point to a file that can handle the job. This file will be executed by Node.js.

**`jobs/video-processing.ts`**

```typescript
import { z } from 'zod';
import { JobDefinition, JobContext } from 'timecrisis';

const videoProcessingSchema = z.object({
  videoId: z.string().uuid(),
});

// Note: The 'handle' function for a forked job is often defined
// in the fork helper file itself, but it can be defined here.
// The code in the fork helper will be what's actually executed.
async function handleProcessVideo(data: z.infer<typeof videoProcessingSchema>, ctx: JobContext) {
  // This logic runs in the main process and is mainly for definition.
  // The actual heavy lifting is in the fork helper.
  await ctx.log('info', `Queueing video processing for ${data.videoId}.`);
}

export const videoProcessingJob: JobDefinition<typeof videoProcessingSchema> = {
  type: 'video.process',
  schema: videoProcessingSchema,
  handle: handleProcessVideo,
  concurrency: 2,
  priority: 100, // Low priority
  forkMode: true,
  forkHelperPath: './workers/video-processor.js', // Path to the worker script
};
```

**`workers/video-processor.js`**

This is the script that will be executed in a child process. It needs to set up a minimal handler to receive the job data and execute the logic.

```javascript
// This is a simplified example.
// In a real application, you would import your actual video processing logic.
const { parentPort } = require('worker_threads');

async function processVideo(data) {
  console.log(`[Worker] Processing video ${data.videoId}...`);
  // Simulate heavy CPU work
  await new Promise((res) => setTimeout(res, 10000));
  console.log(`[Worker] Finished processing video ${data.videoId}.`);
}

parentPort.on('message', async (job) => {
  try {
    await processVideo(job.data);
    parentPort.postMessage({ status: 'completed' });
  } catch (error) {
    parentPort.postMessage({ status: 'failed', error: error.message });
  } finally {
    process.exit(0);
  }
});
```

## 5. Time-Sensitive Job with Expiration

This job is for sending a one-time password (OTP) that is only valid for a short period.

- `expiresAfter: '5m'` ensures that if the job doesn't start running within 5 minutes, it will be discarded, preventing a stale OTP from being sent.

```typescript
import { z } from 'zod';
import { JobDefinition, JobContext } from 'timecrisis';

const sendOtpSchema = z.object({
  phone: z.string(),
});

async function handleSendOtp(data: z.infer<typeof sendOtpSchema>, ctx: JobContext) {
  await ctx.log('info', `Sending OTP to ${data.phone}`);
  // Your SMS sending logic here
  await ctx.log('info', 'OTP sent successfully.');
}

export const sendOtpJob: JobDefinition<typeof sendOtpSchema> = {
  type: 'auth.send-otp',
  schema: sendOtpSchema,
  handle: handleSendOtp,
  concurrency: 20,
  priority: 1, // Highest priority
  expiresAfter: '5m', // OTP is time-sensitive
};
```
