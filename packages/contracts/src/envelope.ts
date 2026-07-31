import { z } from 'zod';

export const rfc3339Schema = z.string().datetime({ offset: true });

export const envelopeSchema = z
  .object({
    event_id: z.string().min(1),
    event_type: z.string().min(1),
    event_version: z.string().min(1),
    occurred_at: rfc3339Schema,
    trace_id: z.string().min(1).optional(),
    source: z.string().min(1),
    payload: z.record(z.unknown())
  })
  .passthrough();

export type Envelope = z.infer<typeof envelopeSchema>;
