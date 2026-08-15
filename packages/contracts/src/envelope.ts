import { z } from 'zod';

export const rfc3339Schema: z.ZodString = z
  .string()
  .datetime({ offset: true }) as z.ZodString;

export const envelopeSchema: z.ZodObject<
  {
    event_id: z.ZodString;
    event_type: z.ZodString;
    event_version: z.ZodString;
    occurred_at: z.ZodString;
    trace_id: z.ZodOptional<z.ZodString>;
    source: z.ZodString;
    payload: z.ZodRecord<z.ZodString, z.ZodUnknown>;
  },
  'strict'
> = z
  .object({
    event_id: z.string().min(1),
    event_type: z.string().min(1),
    event_version: z.string().min(1),
    occurred_at: rfc3339Schema,
    trace_id: z.string().min(1).optional(),
    source: z.string().min(1),
    payload: z.record(z.unknown()),
  })
  .strict();

export type Envelope = z.infer<typeof envelopeSchema>;
