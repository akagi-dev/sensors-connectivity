import rateLimit from '@fastify/rate-limit';
import Fastify from 'fastify';
import type { FastifyInstance, FastifyRequest } from 'fastify';
import { fileURLToPath } from 'node:url';
import type { TelemetryRejectedPayload, SignedEnvelope } from '@scp/contracts';
import {
  encodeBase64,
  extractSensorId,
  validateSignedEnvelope
} from '@scp/contracts';
import { createRegistryReaderFromEnv, type RegistryReader } from '@scp/registry-sync';
import { createEndpointEventProducer, type EndpointEventProducer } from './kafka-producer.js';
import { loadEndpointConfig } from './config.js';
import { verifyTelemetrySignature } from './signature.js';
import { createSensorAuthProvider, loadSensorAuthConfig } from './sensor-auth-factory.js';
import { logDebug, logError, logInfo, logWarn } from './logger.js';

export interface EndpointDeps {
  registryReader: RegistryReader;
  producer: EndpointEventProducer;
}

export interface EndpointAppOptions {
  timestampSkewSeconds?: number;
}

function toHex(bytes: Uint8Array): string {
  return Buffer.from(bytes).toString('hex');
}

function parseSignedEnvelope(request: FastifyRequest): { envelope: SignedEnvelope; rawBytes: Uint8Array } {
  const contentType = request.headers['content-type']?.toLowerCase() ?? '';
  if (!contentType.includes('application/protobuf') && !contentType.includes('application/x-protobuf')) {
    throw new Error('Expected Content-Type application/protobuf');
  }
  if (!(request.body instanceof Uint8Array || Buffer.isBuffer(request.body))) {
    throw new Error('Expected binary protobuf request body');
  }
  const rawBytes = Uint8Array.from(request.body);
  return { envelope: validateSignedEnvelope(rawBytes), rawBytes };
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

  app.addContentTypeParser(
    ['application/protobuf', 'application/x-protobuf'],
    { parseAs: 'buffer' },
    (_request, body, done) => done(null, body)
  );

  app.register(rateLimit, {
    max: 100,
    timeWindow: '1 minute'
  });

  app.get('/health', async () => ({ status: 'ok' }));
  app.get('/metrics', async () => metrics);

  app.post('/v1/telemetry', {
    config: {
      rateLimit: {
        max: 100,
        timeWindow: '1 minute'
      }
    }
  }, async (request, reply) => {
    const traceId = request.headers['x-request-id']?.toString() ?? request.id;
    let parsedEnvelope: SignedEnvelope;
    let rawEnvelopeBytes: Uint8Array;
    let sensorAddress = 'unknown';
    let nonceHex = 'unknown';
    let sensorHex = 'unknown';

    try {
      const parsed = parseSignedEnvelope(request);
      parsedEnvelope = parsed.envelope;
      rawEnvelopeBytes = parsed.rawBytes;
      sensorAddress = extractSensorId(parsedEnvelope);
      nonceHex = toHex(parsedEnvelope.nonce);
      sensorHex = toHex(parsedEnvelope.sensorId);
    } catch (error) {
      metrics.rejected += 1;
      logWarn('telemetry rejected due to invalid envelope', { trace_id: traceId });
      return reply.code(400).send({ status: 'rejected', error_code: 'invalid_envelope' });
    }

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

    const timestampMs = Number(parsedEnvelope.timestamp);
    const skewMs = Math.abs(Date.now() - timestampMs);
    if (!Number.isSafeInteger(timestampMs) || skewMs > timestampSkewSeconds * 1000) {
      metrics.rejected += 1;
      publishRejectedEvent({
        sensor_id: sensorAddress,
        reason_code: 'stale_timestamp',
        reason_message: 'Timestamp outside allowed skew window'
      });
      return reply.code(401).send({ status: 'rejected', error_code: 'stale_timestamp' });
    }

    const record = await deps.registryReader.getSensorRecord(sensorAddress);
    if (!record || !record.enabled) {
      metrics.rejected += 1;
      publishRejectedEvent({
        sensor_id: sensorAddress,
        reason_code: 'sensor_forbidden',
        reason_message: 'Sensor is unknown or disabled'
      });
      return reply.code(403).send({ status: 'rejected', error_code: 'sensor_forbidden' });
    }

    const nonceSeen = await deps.registryReader.isNonceSeen(sensorHex, nonceHex);
    if (nonceSeen) {
      metrics.rejected += 1;
      publishRejectedEvent({
        sensor_id: sensorAddress,
        reason_code: 'duplicate_nonce',
        reason_message: 'Nonce already used for this sensor'
      });
      return reply.code(409).send({ status: 'rejected', error_code: 'duplicate_nonce' });
    }

    const signatureValid = await verifyTelemetrySignature({
      sensorId: parsedEnvelope.sensorId,
      timestamp: parsedEnvelope.timestamp,
      nonce: parsedEnvelope.nonce,
      message: parsedEnvelope.message,
      signature: parsedEnvelope.signature,
      signerAddress: record.sensorId
    });

    if (!signatureValid) {
      metrics.rejected += 1;
      publishRejectedEvent({
        sensor_id: sensorAddress,
        reason_code: 'invalid_signature',
        reason_message: 'Signature verification failed'
      });
      return reply.code(401).send({ status: 'rejected', error_code: 'invalid_signature' });
    }

    try {
      await deps.producer.publishAuthorized({
        sensor_id: encodeBase64(parsedEnvelope.sensorId),
        timestamp: timestampMs,
        nonce: encodeBase64(parsedEnvelope.nonce),
        message: encodeBase64(parsedEnvelope.message),
        signature: encodeBase64(parsedEnvelope.signature),
        envelope: encodeBase64(rawEnvelopeBytes)
      }, traceId);
    } catch (error) {
      metrics.kafkaErrors += 1;
      metrics.rejected += 1;
      logError('failed to publish authorized telemetry event', error, { trace_id: traceId, sensor_id: sensorAddress });
      return reply.code(503).send({ status: 'rejected', error_code: 'kafka_unavailable' });
    }

    try {
      await deps.registryReader.rememberNonce(sensorHex, nonceHex);
    } catch (error) {
      metrics.rejected += 1;
      logError('failed to remember sensor nonce', error, { trace_id: traceId, sensor_id: sensorAddress });
      return reply.code(503).send({ status: 'rejected', error_code: 'kafka_unavailable' });
    }

    metrics.accepted += 1;
    return reply.code(202).send({ status: 'accepted' });
  });

  return app;
}

export async function startEndpoint() {
  const config = loadEndpointConfig();
  const authConfig = loadSensorAuthConfig();
  
  logInfo('starting endpoint service', {
    port: config.port,
    source: config.source,
    auth_strategy: authConfig.strategy,
    kafka_broker_count: config.kafkaBrokers.length,
    timestamp_skew_seconds: config.timestampSkewSeconds,
    producer_max_attempts: config.producerMaxAttempts,
    producer_retry_backoff_ms: config.producerRetryBackoffMs
  });

  const registryReader = createSensorAuthProvider(
    authConfig.strategy,
    createRegistryReaderFromEnv
  );

  const app = createEndpointApp({
    registryReader,
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
