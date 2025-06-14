import { pino } from 'pino';
import Database from 'better-sqlite3';

import { SQLiteJobStorage } from '@timecrisis/timecrisis-sqlite';
import { JobScheduler, PinoLogger } from '@timecrisis/timecrisis';

import { sendEmailJob } from './jobs/send-email';

const runScheduler = async () => {
  // Use 'debug' for more verbose logs.
  const logger = pino({ name: 'basic-example', level: 'info' });

  const db = new Database('db.sqlite');
  db.pragma('journal_mode = WAL');
  db.pragma('busy_timeout = 5000');

  const storage = new SQLiteJobStorage(db);
  await storage.init();

  // Instantiate the scheduler
  const scheduler = new JobScheduler({
    storage,
    logger: new PinoLogger({
      instance: logger,
    }),
    maxConcurrentJobs: 10,
    // Check for new jobs every 500 ms
    jobProcessingInterval: 500,
    // This will lock jobs for 5 minutes when they get picked up
    jobLockTTL: 300000,
    // This process will acquire the leader lock for 30 seconds
    leaderLockTTL: 30000, // 30 seconds
    // Look for scheduled jobs which need to be planned every 1 second
    jobSchedulingInterval: 1000,
    // Unique identifier for this scheduler instance
    worker: 'node-1',
  });

  // Register the sendEmail job
  scheduler.registerJob(sendEmailJob);
  logger.info(`Registered job type: ${sendEmailJob.type}`);

  // Start the scheduler
  await scheduler.start();
  logger.info('Job scheduler started');

  const jobId = await scheduler.schedule(
    'newsletter-cron',
    'send-email',
    {
      to: 'recipient@example.com',
      subject: 'Test Email',
      body: 'This is a test email sent from the job scheduler.',
    },
    {
      referenceId: '123',
      scheduleValue: '51 09 * * *',
      scheduleType: 'cron',
      timeZone: 'Europe/Paris',
    }
  );

  logger.info(`Scheduled send-email job with ID: ${jobId}`);
};

runScheduler().catch((error) => {
  console.error('Error running scheduler:', error);
});
