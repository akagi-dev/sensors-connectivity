/**
 * Copyright 2026 Robonomics Network
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
import {
  TELEMETRY_TOPICS,
  EnvelopeSchema,
  TelemetryAuthorizedPayloadSchema,
  formatSensorId,
  type TelemetryAuthorizedPayload,
} from '@scp/core';
import { fromBinary } from '@bufbuild/protobuf';
import { Consumer } from '@platformatic/kafka';
import { createServer, type Server } from 'node:http';
import { fileURLToPath } from 'node:url';
import {
  create as createKuboClient,
  type KuboRPCClient,
} from 'kubo-rpc-client';
import pino from 'pino';
import {
  loadPubsubBroadcasterConfig,
  type PubsubBroadcasterConfig,
} from './config.js';

interface PubsubBroadcasterMetrics {
  consumed: number;
  publishSuccess: number;
  publishFailure: number;
}

export interface PubsubBroadcasterService {
  start(): Promise<void>;
  stop(): Promise<void>;
  getMetrics(): Readonly<PubsubBroadcasterMetrics>;
}

interface PubsubClient {
  start(): Promise<void>;
  stop(): Promise<void>;
  publish(topic: string, data: Uint8Array): Promise<void>;
}

interface PubsubBroadcasterDeps {
  createPubsubClient?: () => Promise<PubsubClient>;
  createConsumer?: () => Consumer;
  createHealthServer?: (
    getMetrics: () => PubsubBroadcasterMetrics,
    port: number
  ) => Server;
}

const logger = pino({
  name: 'pubsub-broadcaster',
  level:
    process.env.PUBSUB_BROADCASTER_LOG_LEVEL ?? process.env.LOG_LEVEL ?? 'info',
});

function logInfo(message: string, context?: Record<string, unknown>): void {
  logger.info(context ?? {}, message);
}

function logWarn(message: string, context?: Record<string, unknown>): void {
  logger.warn(context ?? {}, message);
}

function logDebug(message: string, context?: Record<string, unknown>): void {
  logger.debug(context ?? {}, message);
}

function logError(
  message: string,
  error: unknown,
  context?: Record<string, unknown>
): void {
  logger.error(
    {
      ...(context ?? {}),
      error: error instanceof Error ? error.message : String(error),
    },
    message
  );
}

/**
 * Process and publish authorized telemetry envelope to PubSub
 * Simple handler without retry logic or result events (observability-only)
 */
export function handleTelemetryMessage(
  raw: Buffer,
  pubsub: PubsubClient,
  config: PubsubBroadcasterConfig,
  metrics: PubsubBroadcasterMetrics
): Promise<void> {
  try {
    const envelope = fromBinary(EnvelopeSchema, new Uint8Array(raw));

    if (envelope.eventType !== TELEMETRY_TOPICS.AUTHORIZED) {
      logDebug('non-authorized envelope ignored', {
        eventType: envelope.eventType,
      });
      return Promise.resolve();
    }

    const payload = fromBinary(
      TelemetryAuthorizedPayloadSchema,
      envelope.payload
    ) as TelemetryAuthorizedPayload;

    const sensorIdFormatted = formatSensorId(payload.sensorId);

    logDebug('authorized envelope received', {
      eventId: envelope.eventId,
      eventType: envelope.eventType,
      sensor_id: sensorIdFormatted,
    });

    metrics.consumed += 1;

    // Forward the original SignedEnvelope to PubSub
    const signedEnvelopeBytes = payload.signedEnvelope;

    return pubsub
      .publish(config.pubsubTopic, signedEnvelopeBytes)
      .then(() => {
        metrics.publishSuccess += 1;
        logInfo('telemetry published to PubSub', {
          sensor_id: sensorIdFormatted,
          pubsub_topic: config.pubsubTopic,
        });
      })
      .catch((error) => {
        metrics.publishFailure += 1;
        logWarn('PubSub publish failed (will not retry)', {
          sensor_id: sensorIdFormatted,
          error: error instanceof Error ? error.message : String(error),
        });
      });
  } catch (error) {
    logWarn('envelope parse error', {
      reason: error instanceof Error ? error.message : String(error),
    });
    return Promise.resolve();
  }
}

export function createPubsubBroadcasterService(
  config: PubsubBroadcasterConfig = loadPubsubBroadcasterConfig(),
  deps: PubsubBroadcasterDeps = {}
): PubsubBroadcasterService {
  const consumer =
    deps.createConsumer?.() ??
    new Consumer({
      groupId: config.consumerGroupId,
      clientId: 'pubsub-broadcaster',
      bootstrapBrokers: config.kafkaBrokers,
    });
  const createPubsubClient =
    deps.createPubsubClient ??
    (() => createIpfsPubsubClient(config.ipfsApiUrl));
  const createHealthServer =
    deps.createHealthServer ?? startHealthAndMetricsServer;

  let pubsubClient: PubsubClient | null = null;
  let started = false;
  let runPromise: Promise<void> | null = null;
  let healthServer: Server | null = null;
  let consumerStream: AsyncIterable<{
    topic: string;
    partition: number;
    offset: bigint;
    value: Buffer | null;
  }> | null = null;
  const metrics: PubsubBroadcasterMetrics = {
    consumed: 0,
    publishSuccess: 0,
    publishFailure: 0,
  };

  const getMetrics = (): PubsubBroadcasterMetrics => metrics;

  return {
    async start(): Promise<void> {
      if (started) {
        logInfo('start skipped; service already started');
        return;
      }

      started = true;
      logInfo('starting service', {
        consumerGroupId: config.consumerGroupId,
        kafkaBrokers: config.kafkaBrokers,
        pubsubTopic: config.pubsubTopic,
        ipfsApiUrl: config.ipfsApiUrl,
        healthPort: config.healthPort,
      });

      try {
        pubsubClient = await createPubsubClient();
        await pubsubClient.start();

        consumerStream = await consumer.consume({
          topics: [TELEMETRY_TOPICS.AUTHORIZED],
          autocommit: true,
        });

        healthServer = createHealthServer(getMetrics, config.healthPort);

        runPromise = (async () => {
          for await (const message of consumerStream!) {
            if (!message.value) {
              logWarn('received null message value; skipping');
              continue;
            }
            await handleTelemetryMessage(
              message.value,
              pubsubClient!,
              config,
              metrics
            );
            logDebug('kafka message processed', {
              topic: message.topic,
              partition: message.partition,
              offset: message.offset,
              consumed: metrics.consumed,
            });
          }
        })();

        logInfo('service started');
      } catch (error) {
        logError('service failed to start', error);
        started = false;
        runPromise = null;

        if (healthServer) {
          await new Promise<void>((resolve) => {
            healthServer?.close(() => {
              resolve();
            });
          });
          healthServer = null;
        }

        await consumer.close().catch(() => undefined);
        await pubsubClient?.stop().catch(() => undefined);
        pubsubClient = null;
        consumerStream = null;
        logInfo('startup rollback complete');
        throw error;
      }
    },
    async stop(): Promise<void> {
      if (!started) {
        logInfo('stop skipped; service not started');
        return;
      }

      started = false;
      logInfo('stopping service');
      await consumer.close();
      await pubsubClient?.stop();
      pubsubClient = null;
      consumerStream = null;
      if (healthServer) {
        await new Promise<void>((resolve, reject) => {
          healthServer?.close((error) => {
            if (error) {
              reject(error);
              return;
            }
            resolve();
          });
        });
        healthServer = null;
      }
      await runPromise?.catch(() => undefined);
      runPromise = null;
      logInfo('service stopped');
    },
    getMetrics(): Readonly<PubsubBroadcasterMetrics> {
      return metrics;
    },
  };
}

function startHealthAndMetricsServer(
  getMetrics: () => PubsubBroadcasterMetrics,
  port: number
): Server {
  const server = createServer((request, response) => {
    if (request.url === '/health') {
      logDebug('health check requested');
      response.statusCode = 200;
      response.setHeader('content-type', 'application/json; charset=utf-8');
      response.end(JSON.stringify({ status: 'ok' }));
      return;
    }

    if (request.url === '/metrics') {
      logDebug('metrics endpoint requested');
      const metrics = getMetrics();
      logInfo('metrics served', {
        consumed: metrics.consumed,
        publishSuccess: metrics.publishSuccess,
        publishFailure: metrics.publishFailure,
      });
      response.statusCode = 200;
      response.setHeader('content-type', 'application/json; charset=utf-8');
      response.end(JSON.stringify(metrics));
      return;
    }

    response.statusCode = 404;
    response.end('not found');
  });
  server.listen({ host: '0.0.0.0', port });
  logInfo('HTTP server listening', { port, host: '0.0.0.0' });
  return server;
}

/**
 * Create an IPFS Kubo RPC client for PubSub messaging
 */
async function createIpfsPubsubClient(apiUrl: string): Promise<PubsubClient> {
  const client: KuboRPCClient = createKuboClient({ url: apiUrl });
  let started = false;

  return {
    async start() {
      // Verify connection to Kubo
      try {
        const version = await client.version();
        logInfo('connected to IPFS node', {
          version: version.version,
          apiUrl,
        });
        started = true;
      } catch (error) {
        const errorMessage =
          error instanceof Error ? error.message : String(error);
        const wrappedError = new Error(
          `Failed to connect to IPFS at ${apiUrl}: ${errorMessage}`,
          { cause: error }
        );
        throw wrappedError;
      }
    },
    async stop() {
      if (!started) {
        return;
      }
      started = false;
      logInfo('IPFS pubsub client stopped');
    },
    async publish(topic: string, data: Uint8Array) {
      if (!started) {
        throw new Error('IPFS pubsub client not started');
      }
      await client.pubsub.publish(topic, data);
    },
  };
}

export async function startPubsubBroadcaster(): Promise<PubsubBroadcasterService> {
  const service = createPubsubBroadcasterService();
  await service.start();
  logInfo('service started (direct run)');
  return service;
}

const isDirectRun = process.argv[1] === fileURLToPath(import.meta.url);
if (isDirectRun) {
  startPubsubBroadcaster().catch((error: unknown) => {
    logError('failed to start (direct run)', error);
    process.exitCode = 1;
  });
}
