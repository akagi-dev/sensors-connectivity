import rateLimit from '@fastify/rate-limit';
import Fastify from 'fastify';
import type { FastifyInstance } from 'fastify';
import { fileURLToPath } from 'node:url';
import pino from 'pino';
import { z } from 'zod';
import { createRegistryReaderFromEnv, type RegistryReader } from '@scp/registry-sync';
import type { TelemetryRejectedPayload } from '@scp/contracts';
import { createEndpointEventProducer, type EndpointEventProducer } from './kafka-producer.js';
import { loadEndpointConfig } from './config.js';
import { verifyTelemetrySignature } from './signature.js';

const telemetryRequestSchema = z
  .object({
    measurements: z.record(z.unknown()),
    sensor_id: z.string().min(1),
    timestamp: z.string().datetime({ offset: true }),
    nonce: z.string().min(1),
    signature: z.string().min(1)
  })
  .passthrough();

const telemetryRequestJsonSchema = {
  type: 'object',
  required: ['measurements', 'sensor_id', 'timestamp', 'nonce', 'signature'],
  properties: {
    measurements: { type: 'object' },
    sensor_id: { type: 'string' },
    timestamp: { type: 'string', format: 'date-time' },
    nonce: { type: 'string' },
    signature: { type: 'string' }
  },
  additionalProperties: true
} as const;

const logger = pino({
  name: 'endpoint',
  level: process.env.ENDPOINT_LOG_LEVEL ?? process.env.LOG_LEVEL ?? 'info'
});

export function logInfo(message: string, context?: Record<string, unknown>): void {
  logger.info(context ?? {}, message);
}

export function logDebug(message: string, context?: Record<string, unknown>): void {
  logger.debug(context ?? {}, message);
}

export function logWarn(message: string, context?: Record<string, unknown>): void {
  logger.warn(context ?? {}, message);
}

export function logError(message: string, error: unknown, context?: Record<string, unknown>): void {
  const normalizedError = error instanceof Error
    ? { name: error.name, message: error.message, stack: error.stack }
    : { message: String(error) };
  logger.error(
    {
      ...(context ?? {}),
      error: normalizedError
    },
    message
  );
}

export interface EndpointDeps {
  registryReader: RegistryReader;
  producer: EndpointEventProducer;
}

export interface EndpointAppOptions {
  timestampSkewSeconds?: number;
}

export function createEndpointApp(
  deps: EndpointDeps,
  options: EndpointAppOptions = {}
): FastifyInstance {
  const app = Fastify({ logger: false });
  const timestampSkewSeconds = options.timestampSkewSeconds ?? 300;
  const metrics = {
    accepted: 0,
    rejected: 0,
    kafkaErrors: 0
  };

  app.register(rateLimit, {
    max: 100,
    timeWindow: '1 minute'
  });

  app.get('/health', async () => ({ status: 'ok' }));
  app.get('/metrics', async () => metrics);

  app.post(
    '/v1/telemetry',
    {
      schema: {
        body: telemetryRequestJsonSchema
      },
      config: {
        rateLimit: {
          max: 100,
          timeWindow: '1 minute'
        }
      }
    },
    async (request, reply) => {
      const body = telemetryRequestSchema.parse(request.body);
      const traceId = request.headers['x-request-id']?.toString() ?? request.id;
      logInfo('telemetry request received', {
        trace_id: traceId,
        sensor_id: body.sensor_id,
        timestamp: body.timestamp,
        nonce: body.nonce
      });
      const publishRejectedEvent = (payload: TelemetryRejectedPayload) =>
        void deps.producer
          .publishRejected(payload, traceId)
          .then((eventId) => {
            logInfo('telemetry rejected event published', {
              event_id: eventId,
              trace_id: traceId,
              sensor_id: payload.sensor_id,
              reason_code: payload.reason_code
            });
          })
          .catch((error) => {
            logError('failed to publish rejected event', error, {
              trace_id: traceId,
              sensor_id: payload.sensor_id,
              reason_code: payload.reason_code
            });
          });

      logDebug('validating telemetry timestamp', {
        trace_id: traceId,
        sensor_id: body.sensor_id
      });
      const timestampMs = Date.parse(body.timestamp);
      const skewMs = Math.abs(Date.now() - timestampMs);
      if (Number.isNaN(timestampMs) || skewMs > timestampSkewSeconds * 1000) {
        metrics.rejected += 1;
        logWarn('telemetry rejected due to stale timestamp', {
          trace_id: traceId,
          sensor_id: body.sensor_id,
          timestamp: body.timestamp,
          skew_ms: Number.isNaN(timestampMs) ? undefined : skewMs,
          allowed_skew_seconds: timestampSkewSeconds
        });
        publishRejectedEvent({
          sensor_id: body.sensor_id,
          reason_code: 'stale_timestamp',
          reason_message: 'Timestamp outside allowed skew window'
        });
        return reply.code(401).send({ status: 'rejected', error_code: 'stale_timestamp' });
      }
      logDebug('telemetry timestamp accepted', {
        trace_id: traceId,
        sensor_id: body.sensor_id,
        skew_ms: skewMs,
        allowed_skew_seconds: timestampSkewSeconds
      });

      logDebug('loading sensor record', {
        trace_id: traceId,
        sensor_id: body.sensor_id
      });
      const record = await deps.registryReader.getSensorRecord(body.sensor_id);
      if (!record || !record.enabled) {
        metrics.rejected += 1;
        logWarn('telemetry rejected due to unknown or disabled sensor', {
          trace_id: traceId,
          sensor_id: body.sensor_id,
          sensor_record_found: Boolean(record),
          sensor_enabled: record?.enabled
        });
        publishRejectedEvent({
          sensor_id: body.sensor_id,
          reason_code: 'sensor_forbidden',
          reason_message: 'Sensor is unknown or disabled'
        });
        return reply.code(403).send({ status: 'rejected', error_code: 'sensor_forbidden' });
      }
      logDebug('sensor record accepted', {
        trace_id: traceId,
        sensor_id: body.sensor_id,
        sensor_enabled: record.enabled
      });

      logDebug('checking nonce replay protection', {
        trace_id: traceId,
        sensor_id: body.sensor_id,
        nonce: body.nonce
      });
      const nonceSeen = await deps.registryReader.isNonceSeen(body.sensor_id, body.nonce);
      if (nonceSeen) {
        metrics.rejected += 1;
        logWarn('telemetry rejected due to duplicate nonce', {
          trace_id: traceId,
          sensor_id: body.sensor_id,
          nonce: body.nonce
        });
        publishRejectedEvent({
          sensor_id: body.sensor_id,
          reason_code: 'duplicate_nonce',
          reason_message: 'Nonce already used for this sensor'
        });
        return reply.code(409).send({ status: 'rejected', error_code: 'duplicate_nonce' });
      }
      logDebug('nonce accepted', {
        trace_id: traceId,
        sensor_id: body.sensor_id,
        nonce: body.nonce
      });

      logDebug('verifying telemetry signature', {
        trace_id: traceId,
        sensor_id: body.sensor_id,
        nonce: body.nonce
      });
      const signatureValid = await verifyTelemetrySignature({
        measurements: body.measurements,
        timestamp: body.timestamp,
        nonce: body.nonce,
        sensorId: body.sensor_id,
        signature: body.signature,
        signerAddress: record.sensorId
      });

      if (!signatureValid) {
        metrics.rejected += 1;
        logWarn('telemetry rejected due to invalid signature', {
          trace_id: traceId,
          sensor_id: body.sensor_id,
          nonce: body.nonce
        });
        publishRejectedEvent({
          sensor_id: body.sensor_id,
          reason_code: 'invalid_signature',
          reason_message: 'Signature verification failed'
        });
        return reply.code(401).send({ status: 'rejected', error_code: 'invalid_signature' });
      }
      logDebug('signature accepted', {
        trace_id: traceId,
        sensor_id: body.sensor_id,
        nonce: body.nonce
      });

      try {
        logDebug('publishing authorized telemetry event', {
          trace_id: traceId,
          sensor_id: body.sensor_id,
          nonce: body.nonce
        });
        const eventId = await deps.producer.publishAuthorized({
          sensor_id: body.sensor_id,
          timestamp: body.timestamp,
          nonce: body.nonce,
          measurements: body.measurements,
          signature: body.signature
        }, traceId);
        logInfo('authorized telemetry event published', {
          event_id: eventId,
          trace_id: traceId,
          sensor_id: body.sensor_id,
          nonce: body.nonce
        });
      } catch (error) {
        metrics.kafkaErrors += 1;
        metrics.rejected += 1;
        logError('failed to publish authorized telemetry event', error, {
          trace_id: traceId,
          sensor_id: body.sensor_id,
          nonce: body.nonce
        });
        return reply.code(503).send({ status: 'rejected', error_code: 'kafka_unavailable' });
      }

      try {
        logDebug('remembering accepted nonce', {
          trace_id: traceId,
          sensor_id: body.sensor_id,
          nonce: body.nonce
        });
        await deps.registryReader.rememberNonce(body.sensor_id, body.nonce);
      } catch (error) {
        metrics.rejected += 1;
        logError('failed to remember sensor nonce', error, {
          trace_id: traceId,
          sensor_id: body.sensor_id,
          nonce: body.nonce
        });
        return reply.code(503).send({ status: 'rejected', error_code: 'kafka_unavailable' });
      }

      metrics.accepted += 1;
      logInfo('telemetry accepted', {
        trace_id: traceId,
        sensor_id: body.sensor_id,
        nonce: body.nonce
      });
      return reply.code(202).send({ status: 'accepted' });
    }
  );

  return app;
}

export async function startEndpoint() {
  const config = loadEndpointConfig();
  logInfo('starting endpoint service', {
    port: config.port,
    source: config.source,
    kafka_broker_count: config.kafkaBrokers.length,
    timestamp_skew_seconds: config.timestampSkewSeconds,
    producer_max_attempts: config.producerMaxAttempts,
    producer_retry_backoff_ms: config.producerRetryBackoffMs
  });
  const app = createEndpointApp({
    registryReader: createRegistryReaderFromEnv(),
    producer: createEndpointEventProducer(
      config.kafkaBrokers,
      config.source,
      config.producerMaxAttempts,
      config.producerRetryBackoffMs
    )
  }, {
    timestampSkewSeconds: config.timestampSkewSeconds
  });

  await app.listen({ host: '0.0.0.0', port: config.port });
  logInfo('listening', { port: config.port });
  return app;
}

const isDirectRun = process.argv[1] === fileURLToPath(import.meta.url);
if (isDirectRun) {
  startEndpoint().catch((error: unknown) => {
    logError('failed to start', error);
    process.exitCode = 1;
  });
}
