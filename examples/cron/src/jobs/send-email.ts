import { z } from 'zod';
import { JobContext, JobDefinition } from '@timecrisis/timecrisis';

const schema = z.object({
  to: z.string().email(),
  subject: z.string(),
  body: z.string(),
  status: z.enum(['pending', 'sent', 'failed']).default('pending'),
  sentAt: z.string().optional(),
});

export const sendEmailJob: JobDefinition<typeof schema> = {
  type: 'send-email',
  concurrency: 5,
  priority: 10,
  schema,
  handle: async (data: z.infer<typeof schema>, ctx: JobContext) => {
    ctx.logger.info('Sending email', { data });

    // Persist the log.
    await ctx.persistLog('info', `Email sent: ${data.subject}`);

    // Update the job data.
    await ctx.updateData({
      ...data,
      status: 'sent',
      sentAt: new Date().toISOString(),
    });

    ctx.logger.info('Email sent', { data, sentAt: new Date().toISOString() });
  },
};
