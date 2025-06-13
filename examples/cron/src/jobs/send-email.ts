import { z } from 'zod';
import { JobContext, JobDefinition } from '@timecrisis/timecrisis';

import { pino } from 'pino';

const logger = pino({ name: 'send-email' });

const schema = z.object({
  to: z.string().email(),
  subject: z.string(),
  body: z.string(),
});

export const sendEmailJob: JobDefinition<typeof schema> = {
  type: 'sendEmail',
  concurrency: 5,
  priority: 10,
  schema,
  handle: async (data: z.infer<typeof schema>, ctx: JobContext) => {
    logger.info({ data }, 'Sending email');

    // Persist the log.
    await ctx.persistLog('info', `Email sent: ${data.subject}`);
  },
};
