import rateLimit from '@fastify/rate-limit';
import Fastify from 'fastify';
import type { FastifyInstance } from 'fastify';
import { fileURLToPath } from 'node:url';
import pino from 'pino';
import { z } from 'zod';
import { createRegistryReaderFromEnv, type RegistryReader } from '@scp/registry-sync';
import type { TelemetryRejectedPayload } from '@scp/contracts';
import { createAuthorizerEventProducer, type AuthorizerEventProducer } from './kafka-producer.js';
import { loadAuthorizerConfig } from './config.js';
import { verifyTelemetrySignature } from './signature.js';

const telemetryRequestSchema = z
  .object({
    measurements: z.record(z.unknown()),
    sensor_address: z.string().min(1),
    timestamp: z.string().datetime({ offset: true }),
    nonce: z.string().min(1),
    signature: z.string().min(1)
  })
  .passthrough();

const telemetryRequestJsonSchema = {
  type: 'object',
  required: ['measurements', 'sensor_address', 'timestamp', 'nonce', 'signature'],
  properties: {
    measurements: { type: 'object' },
    sensor_address: { type: 'string' },
    timestamp: { type: 'string', format: 'date-time' },
    nonce: { type: 'string' },
    signature: { type: 'string' }
  },
  additionalProperties: true
} as const;

const logger = pino({
  name: 'authorizer',
  level: process.env.AUTHORIZER_LOG_LEVEL ?? process.env.LOG_LEVEL ?? 'info'
});

export function logInfo(message: string, context?: Record<string, unknown>): void {
  logger.info(context ?? {}, message);
}

export function logWarn(message: string, context?: Record<string, unknown>): void {
  logger.warn(context ?? {}, message);
}

export function logError(message: string, error: unknown, context?: Record<string, unknown>): void {
  logger.error(
    {
      ...(context ?? {}),
      error: error instanceof Error ? error.message : String(error)
    },
    message
  );
}

export interface AuthorizerDeps {
  registryReader: RegistryReader;
  producer: AuthorizerEventProducer;
}

export interface AuthorizerAppOptions {
  timestampSkewSeconds?: number;
}

export function createAuthorizerApp(
  deps: AuthorizerDeps,
  options: AuthorizerAppOptions = {}
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
      const publishRejectedEvent = (payload: TelemetryRejectedPayload) =>
        void deps.producer
          .publishRejected(payload, traceId)
          .then((eventId) => {
            request.log.info({
              event_id: eventId,
              trace_id: traceId,
              sensor_address: payload.sensor_address,
              reason_code: payload.reason_code
            });
          })
          .catch((error) => {
            request.log.error(error);
          });

      const timestampMs = Date.parse(body.timestamp);
      const skewMs = Math.abs(Date.now() - timestampMs);
      if (Number.isNaN(timestampMs) || skewMs > timestampSkewSeconds * 1000) {
        metrics.rejected += 1;
        publishRejectedEvent({
          sensor_address: body.sensor_address,
          reason_code: 'stale_timestamp',
          reason_message: 'Timestamp outside allowed skew window'
        });
        return reply.code(401).send({ status: 'rejected', error_code: 'stale_timestamp' });
      }

      const record = await deps.registryReader.getSensorRecord(body.sensor_address);
      if (!record || !record.enabled) {
        metrics.rejected += 1;
        publishRejectedEvent({
          sensor_address: body.sensor_address,
          reason_code: 'sensor_forbidden',
          reason_message: 'Sensor is unknown or disabled'
        });
        return reply.code(403).send({ status: 'rejected', error_code: 'sensor_forbidden' });
      }

      const nonceSeen = await deps.registryReader.isNonceSeen(body.sensor_address, body.nonce);
      if (nonceSeen) {
        metrics.rejected += 1;
        publishRejectedEvent({
          sensor_address: body.sensor_address,
          reason_code: 'duplicate_nonce',
          reason_message: 'Nonce already used for this sensor'
        });
        return reply.code(409).send({ status: 'rejected', error_code: 'duplicate_nonce' });
      }

      const signatureValid = await verifyTelemetrySignature({
        measurements: body.measurements,
        timestamp: body.timestamp,
        nonce: body.nonce,
        sensorAddress: body.sensor_address,
        signature: body.signature,
        signerAddress: record.sensorAddress
      });

      if (!signatureValid) {
        metrics.rejected += 1;
        publishRejectedEvent({
          sensor_address: body.sensor_address,
          reason_code: 'invalid_signature',
          reason_message: 'Signature verification failed'
        });
        return reply.code(401).send({ status: 'rejected', error_code: 'invalid_signature' });
      }

      try {
        const eventId = await deps.producer.publishAuthorized({
          sensor_address: body.sensor_address,
          timestamp: body.timestamp,
          nonce: body.nonce,
          measurements: body.measurements,
          signature: body.signature
        }, traceId);
        request.log.info({
          event_id: eventId,
          trace_id: traceId,
          sensor_address: body.sensor_address,
          nonce: body.nonce
        });
      } catch (error) {
        metrics.kafkaErrors += 1;
        metrics.rejected += 1;
        request.log.error(error);
        return reply.code(503).send({ status: 'rejected', error_code: 'kafka_unavailable' });
      }

      try {
        await deps.registryReader.rememberNonce(body.sensor_address, body.nonce);
      } catch (error) {
        metrics.rejected += 1;
        request.log.error(error);
        return reply.code(503).send({ status: 'rejected', error_code: 'kafka_unavailable' });
      }

      metrics.accepted += 1;
      return reply.code(202).send({ status: 'accepted' });
    }
  );

  return app;
}

export async function startAuthorizer() {
  const config = loadAuthorizerConfig();
  const app = createAuthorizerApp({
    registryReader: createRegistryReaderFromEnv(),
    producer: createAuthorizerEventProducer(
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
  startAuthorizer().catch((error: unknown) => {
    logError('failed to start', error);
    process.exitCode = 1;
  });
}
