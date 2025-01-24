import { pino } from 'pino';
import Database from 'better-sqlite3';

import { SQLiteJobStorage } from 'timecrisis-sqlite';
import { JobScheduler, PinoLogger } from 'timecrisis';

import { sendEmailJob } from './jobs/send-email';

const runScheduler = async () => {
  const logger = pino({ name: 'basic-example' });

  const db = new Database('db.sqlite');
  const storage = new SQLiteJobStorage(db);
  await storage.init();

  // Instantiate the scheduler
  const scheduler = new JobScheduler({
    storage,
    logger: new PinoLogger({
      options: {
        level: 'info',
      },
    }),
    maxConcurrentJobs: 10,
    // Check for new jobs every 500 ms
    pollInterval: 500,
    // This will lock jobs for 5 minutes when they get picked up
    jobLockTTL: 300000,
    // This process will acquire the leader lock for 30 seconds
    leaderLockTTL: 30000, // 30 seconds
    // Unique identifier for this scheduler instance
    node: 'node-1',
  });

  // Register the sendEmail job
  scheduler.registerJob(sendEmailJob);
  logger.info(`Registered job type: ${sendEmailJob.type}`);

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
    expiresIn: '120s',
  });

  logger.info(`Enqueued sendEmail job with ID: ${jobId}`);

  // Optionally, you can schedule recurring jobs or handle graceful shutdown
};

runScheduler().catch((error) => {
  console.error('Error running scheduler:', error);
});
