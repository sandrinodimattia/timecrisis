import { z } from 'zod';
import { JobContext, JobDefinition } from 'timecrisis';

import { pino } from 'pino';

const logger = pino({ name: 'send-email' });

const sendEmailSchema = z.object({
  to: z.string().email(),
  subject: z.string(),
  body: z.string(),
});

async function sendEmailHandler(data: z.infer<typeof sendEmailSchema>, ctx: JobContext) {
  logger.info({ data, ctx }, 'Sending email');

  // Persist the log.
  await ctx.log('info', `Email sent: ${data.subject}`);

  throw new Error('Test error');
}

export const sendEmailJob: JobDefinition<typeof sendEmailSchema> = {
  type: 'sendEmail',
  concurrency: 5,
  priority: 10,
  schema: sendEmailSchema,
  handle: sendEmailHandler,
};
