import { pino } from 'pino';
import { JobScheduler, InMemoryJobStorage, PinoLogger } from '@timecrisis/timecrisis';

import { sendEmailJob } from './jobs/send-email';

const runScheduler = async () => {
  const logger = pino({ name: 'basic-example' });

  // Instantiate the storage adapter.
  const storage = new InMemoryJobStorage();
  await storage.init();

  // Instantiate the scheduler
  const scheduler = new JobScheduler({
    storage,
    logger: new PinoLogger({
      options: {
        level: 'debug',
      },
    }),
    maxConcurrentJobs: 10,
    // Check for new jobs every 500 ms
    jobProcessingInterval: 500,
    // This will lock jobs for 5 minutes when they get picked up
    jobLockTTL: 300000,
    // This process will acquire the leader lock for 30 seconds
    leaderLockTTL: 30000,
    // Unique identifier for this scheduler instance
    worker: 'node-1',
  });

  // Register the sendEmail job
  scheduler.registerJob(sendEmailJob);

  // Start the scheduler
  await scheduler.start();
  logger.info('Job scheduler started');

  // Enqueue a sample sendEmail job
  const jobData = {
    to: 'recipient@example.com',
    subject: 'Test Email',
    body: 'This is a test email sent from the job scheduler.',
  };

  const jobId = await scheduler.enqueue('sendEmail', jobData, {
    priority: 15,
    maxRetries: 5,
    referenceId: '123',
    expiresIn: '20s',
  });

  logger.info(`Enqueued sendEmail job with ID: ${jobId}`);
};

runScheduler().catch((error) => {
  console.error('Error running scheduler:', error);
});
