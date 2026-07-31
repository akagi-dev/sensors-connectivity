import rateLimit from '@fastify/rate-limit';
import Fastify from 'fastify';
import type { FastifyInstance } from 'fastify';
import { fileURLToPath } from 'node:url';
import { z } from 'zod';
import { createRegistryReaderFromEnv, type RegistryReader } from '@scp/registry-sync';
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

export interface AuthorizerDeps {
  registryReader: RegistryReader;
  producer: AuthorizerEventProducer;
}

export function createAuthorizerApp(deps: AuthorizerDeps): FastifyInstance {
  const app = Fastify({ logger: false });
  app.register(rateLimit, {
    max: 100,
    timeWindow: '1 minute'
  });

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

      const record = await deps.registryReader.getSensorRecord(body.sensor_address);
      if (!record || !record.enabled) {
        return reply.code(403).send({ status: 'rejected', error_code: 'sensor_forbidden' });
      }

      const nonceSeen = await deps.registryReader.isNonceSeen(body.sensor_address, body.nonce);
      if (nonceSeen) {
        return reply.code(409).send({ status: 'rejected', error_code: 'duplicate_nonce' });
      }

      const signatureValid = await verifyTelemetrySignature({
        measurements: body.measurements,
        nonce: body.nonce,
        sensorAddress: body.sensor_address,
        signature: body.signature,
        signerAddress: record.sensorAddress
      });

      if (!signatureValid) {
        return reply.code(401).send({ status: 'rejected', error_code: 'invalid_signature' });
      }

      try {
        await deps.producer.publishAuthorized({
          sensor_address: body.sensor_address,
          timestamp: body.timestamp,
          nonce: body.nonce,
          measurements: body.measurements,
          signature: body.signature
        });
        await deps.registryReader.rememberNonce(body.sensor_address, body.nonce);
      } catch (error) {
        request.log.error(error);
        return reply.code(503).send({ status: 'rejected', error_code: 'kafka_unavailable' });
      }

      return reply.code(202).send({ status: 'accepted' });
    }
  );

  return app;
}

export async function startAuthorizer() {
  const config = loadAuthorizerConfig();
  const app = createAuthorizerApp({
    registryReader: createRegistryReaderFromEnv(),
    producer: createAuthorizerEventProducer(config.kafkaBrokers)
  });

  await app.listen({ host: '0.0.0.0', port: config.port });
  console.log(`[authorizer] listening on ${config.port}`);
  return app;
}

const isDirectRun = process.argv[1] === fileURLToPath(import.meta.url);
if (isDirectRun) {
  startAuthorizer().catch((error: unknown) => {
    console.error('[authorizer] failed to start', error);
    process.exitCode = 1;
  });
}
