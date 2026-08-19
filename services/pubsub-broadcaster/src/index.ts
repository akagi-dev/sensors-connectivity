import {
  InMemoryRetryCounterStore,
  TELEMETRY_TOPICS,
  runConsumerProcessingRule,
  parseEnvelope,
  parsePayloadForEventType,
  formatSensorId,
  createEnvelope,
  serializePayloadForEventType,
  TelemetryPubsubResultPayloadSchema,
  TelemetryPubsubResultPayload_Status,
  type Envelope,
  type RetryDlqPublisher,
  type TelemetryAuthorizedPayload,
} from '@scp/contracts';
import { create } from '@bufbuild/protobuf';
import { Consumer, Producer } from '@platformatic/kafka';
import { createServer, type Server } from 'node:http';
import { randomUUID } from 'node:crypto';
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
  processed: number;
  publishSuccess: number;
  publishFailure: number;
  retryCount: number;
  dlqCount: number;
  consumerLag: number;
}

export interface PubsubBroadcasterService {
  start(): Promise<void>;
  stop(): Promise<void>;
  getMetrics(): Readonly<PubsubBroadcasterMetrics>;
}

export type AuthorizedTelemetryMessage = {
  eventId: string;
  eventType: typeof TELEMETRY_TOPICS.AUTHORIZED;
  traceId?: string | undefined;
  payload: TelemetryAuthorizedPayload;
};

interface PubsubClient {
  start(): Promise<void>;
  stop(): Promise<void>;
  publish(topic: string, data: Uint8Array): Promise<void>;
}

interface EnvelopePublisher {
  connect(): Promise<void>;
  disconnect(): Promise<void>;
  publish(topic: string, key: string, envelope: Envelope): Promise<void>;
}

interface EventIdStore {
  has(eventId: string): Promise<boolean>;
  mark(eventId: string): Promise<void>;
}

interface PubsubBroadcasterDeps {
  createPubsubClient?: () => Promise<PubsubClient>;
  createPublisher?: () => EnvelopePublisher;
  createConsumer?: () => Consumer<Buffer, Buffer, Buffer, Buffer>;
  idempotencyStore?: EventIdStore;
  createHealthServer?: (
    metrics: PubsubBroadcasterMetrics,
    port: number
  ) => Server;
  sleep?: (ms: number) => Promise<void>;
}

class InMemoryEventIdStore implements EventIdStore {
  private readonly seen = new Set<string>();

  async has(eventId: string): Promise<boolean> {
    return this.seen.has(eventId);
  }

  async mark(eventId: string): Promise<void> {
    this.seen.add(eventId);
  }
}

interface ProcessingContext {
  config: PubsubBroadcasterConfig;
  pubsub: PubsubClient;
  publisher: EnvelopePublisher;
  idempotencyStore: EventIdStore;
  retryStore: InMemoryRetryCounterStore;
  metrics: PubsubBroadcasterMetrics;
}

interface MessageData {
  topic: string;
  partition: number;
  offset: bigint;
  value: Buffer;
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

export async function processAuthorizedEnvelope(
  message: AuthorizedTelemetryMessage,
  commitOffset: () => Promise<void>,
  context: ProcessingContext
): Promise<'processed' | 'duplicate' | 'dlq'> {
  let pending = true;
  let finalStatus: 'processed' | 'duplicate' | 'dlq' = 'processed';

  while (pending) {
    let publishStatus: 'submitted' | 'failed' = 'submitted';
    let publishError: Error | undefined;
    const sensorIdKey = formatSensorId(message.payload.sensorId);

    const status = await runConsumerProcessingRule(message, {
      retryPolicy: {
        maxAttempts: context.config.maxRetries,
        getEventId: (current) => current.eventId,
        store: context.retryStore,
      },
      idempotency: {
        getEventId: (current) => current.eventId,
        hasProcessed: (eventId) => context.idempotencyStore.has(eventId),
        markProcessed: (eventId) => context.idempotencyStore.mark(eventId),
      },
      performExternalAction: async (current) => {
        try {
          // Forward the original SignedEnvelope to PubSub
          const signedEnvelopeBytes = current.payload.signedEnvelope;

          const pubsubTopic = context.config.pubsubTopic;
          await context.pubsub.publish(pubsubTopic, signedEnvelopeBytes);

          publishStatus = 'submitted';
          context.metrics.publishSuccess += 1;

          logInfo('telemetry published to PubSub', {
            sensor_id: formatSensorId(current.payload.sensorId),
            pubsub_topic: pubsubTopic,
          });
        } catch (error) {
          publishStatus = 'failed';
          publishError =
            error instanceof Error ? error : new Error(String(error));
          context.metrics.publishFailure += 1;

          // Check if error is transient and should be retried
          if (!isTransientPublishError(error)) {
            // Non-transient errors should not retry, but we should emit a result event
            // before proceeding to the final commit
            return;
          }

          // Transient errors will be re-thrown to trigger retry logic
          throw error;
        }
      },
      waitForConfirmation: async () => {},
      emitResultEvent: async (current) => {
        const resultPayload = create(TelemetryPubsubResultPayloadSchema, {
          status:
            publishStatus === 'submitted'
              ? TelemetryPubsubResultPayload_Status.SUBMITTED
              : TelemetryPubsubResultPayload_Status.FAILED,
          sensorId: current.payload.sensorId,
          ...(publishError
            ? {
                errorCode: 1, // Generic error code
                errorMessage: publishError.message,
              }
            : {}),
        });

        return createEnvelope({
          eventId: randomUUID(),
          eventType: TELEMETRY_TOPICS.PUBSUB_RESULT,
          eventVersion: 'v1',
          occurredAt: new Date().toISOString(),
          source: context.config.source,
          traceId: current.traceId,
          payload: serializePayloadForEventType(
            TELEMETRY_TOPICS.PUBSUB_RESULT,
            resultPayload
          ),
        });
      },
      publishResultEvent: async (result) => {
        await context.publisher.publish(
          TELEMETRY_TOPICS.PUBSUB_RESULT,
          sensorIdKey,
          result
        );
      },
      commitOffset,
      retryDlqPublisher: createRetryDlqPublisher(context),
    });

    if (status === 'retried') {
      context.metrics.retryCount += 1;
      await sleep(context.config.retryBackoffMs);
      continue;
    }

    if (status === 'dlq') {
      context.metrics.dlqCount += 1;
      finalStatus = 'dlq';
      pending = false;
      continue;
    }

    if (status === 'duplicate') {
      finalStatus = 'duplicate';
      pending = false;
      continue;
    }

    context.metrics.processed += 1;
    finalStatus = 'processed';
    pending = false;
  }

  return finalStatus;
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
  const producer =
    deps.createPublisher?.() ??
    createKafkaEnvelopePublisher(config.kafkaBrokers);
  const idempotencyStore = deps.idempotencyStore ?? new InMemoryEventIdStore();
  const retryStore = new InMemoryRetryCounterStore();
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
    value: Buffer;
  }> | null = null;
  const metrics: PubsubBroadcasterMetrics = {
    consumed: 0,
    processed: 0,
    publishSuccess: 0,
    publishFailure: 0,
    retryCount: 0,
    dlqCount: 0,
    consumerLag: 0,
  };

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
        await producer.connect();

        consumerStream = await consumer.consume({
          topics: [TELEMETRY_TOPICS.AUTHORIZED],
          autocommit: false,
        });

        healthServer = createHealthServer(metrics, config.healthPort);

        runPromise = (async () => {
          for await (const message of consumerStream!) {
            await processMessage(message, consumer, {
              config,
              pubsub: pubsubClient!,
              publisher: producer,
              idempotencyStore,
              retryStore,
              metrics,
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

        await consumer.disconnect().catch(() => undefined);
        await producer.disconnect().catch(() => undefined);
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
      await consumer.disconnect();
      await producer.disconnect();
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

async function processMessage(
  message: MessageData,
  consumer: Consumer<Buffer, Buffer, Buffer, Buffer>,
  context: ProcessingContext
): Promise<void> {
  context.metrics.consumed += 1;

  const commitOffset = async () => {
    await consumer.commit({
      offsets: [
        {
          topic: message.topic,
          partition: message.partition,
          offset: message.offset + 1n,
          leaderEpoch: 0,
        },
      ],
    });
  };

  const msgContext = {
    topic: message.topic,
    partition: message.partition,
    offset: message.offset.toString(),
  };

  // Only parse the envelope header, not the payload
  const raw = message.value ?? Buffer.from([]);
  let envelope;
  try {
    envelope = parseEnvelope(new Uint8Array(raw));
  } catch (error) {
    const reason =
      error instanceof Error ? error.message : 'Invalid protobuf envelope';
    logWarn('invalid envelope routed to DLQ', {
      ...msgContext,
      reason,
    });
    await publishInvalidMessageDlq(raw, reason, context, commitOffset);
    return;
  }

  if (envelope.eventType !== TELEMETRY_TOPICS.AUTHORIZED) {
    const reason = `Unsupported event type: ${envelope.eventType}`;
    logWarn('invalid envelope routed to DLQ', {
      ...msgContext,
      reason,
    });
    await publishInvalidMessageDlq(raw, reason, context, commitOffset);
    return;
  }

  // Parse only the payload to get sensorId and signedEnvelope
  let payload: TelemetryAuthorizedPayload;
  try {
    payload = parsePayloadForEventType(
      TELEMETRY_TOPICS.AUTHORIZED,
      envelope.payload
    );
  } catch (error) {
    const reason = error instanceof Error ? error.message : 'Invalid payload';
    logWarn('invalid payload routed to DLQ', {
      ...msgContext,
      reason,
    });
    await publishInvalidMessageDlq(raw, reason, context, commitOffset);
    return;
  }

  const authorizedMessage: AuthorizedTelemetryMessage = {
    eventId: envelope.eventId,
    eventType: TELEMETRY_TOPICS.AUTHORIZED,
    traceId: envelope.traceId,
    payload,
  };

  const status = await processAuthorizedEnvelope(
    authorizedMessage,
    commitOffset,
    context
  );
  logInfo('authorized envelope handled', {
    eventId: envelope.eventId,
    traceId: envelope.traceId,
    status,
    ...msgContext,
  });
}

async function publishInvalidMessageDlq(
  failedPayload: unknown,
  reason: string,
  context: ProcessingContext,
  commitOffset: () => Promise<void>
): Promise<void> {
  context.metrics.dlqCount += 1;
  await context.publisher.publish(TELEMETRY_TOPICS.DLQ, 'invalid-envelope', {
    event_id: randomUUID(),
    event_type: TELEMETRY_TOPICS.DLQ,
    event_version: 'v1',
    occurred_at: new Date().toISOString(),
    source: context.config.source,
    payload: {
      failed_topic: TELEMETRY_TOPICS.AUTHORIZED,
      reason_code: 'invalid_envelope',
      reason_message: reason,
      failed_payload: failedPayload,
    },
  });
  await commitOffset();
}

function createRetryDlqPublisher(
  context: ProcessingContext
): RetryDlqPublisher<Envelope> {
  return {
    publishRetry: async (event, reason, failureContext) => {
      await context.publisher.publish(TELEMETRY_TOPICS.RETRY, event.event_id, {
        event_id: randomUUID(),
        event_type: TELEMETRY_TOPICS.RETRY,
        event_version: 'v1',
        occurred_at: new Date().toISOString(),
        trace_id: event.trace_id,
        source: context.config.source,
        payload: {
          failed_topic: TELEMETRY_TOPICS.AUTHORIZED,
          reason_code: 'transient_publish_error',
          reason_message: reason,
          failed_event: event,
          context: failureContext,
        },
      });
    },
    publishDlq: async (event, reason, failureContext) => {
      await context.publisher.publish(TELEMETRY_TOPICS.DLQ, event.event_id, {
        event_id: randomUUID(),
        event_type: TELEMETRY_TOPICS.DLQ,
        event_version: 'v1',
        occurred_at: new Date().toISOString(),
        trace_id: event.trace_id,
        source: context.config.source,
        payload: {
          failed_topic: TELEMETRY_TOPICS.AUTHORIZED,
          reason_code: 'publish_failed',
          reason_message: reason,
          failed_event: event,
          context: failureContext,
        },
      });
    },
  };
}

function startHealthAndMetricsServer(
  metrics: PubsubBroadcasterMetrics,
  port: number
): Server {
  const server = createServer((request, response) => {
    if (request.url === '/health') {
      response.statusCode = 200;
      response.setHeader('content-type', 'application/json; charset=utf-8');
      response.end(
        JSON.stringify({
          status: 'ok',
          consumer_lag: metrics.consumerLag,
        })
      );
      return;
    }

    if (request.url === '/metrics') {
      response.statusCode = 200;
      response.setHeader('content-type', 'application/json; charset=utf-8');
      response.end(JSON.stringify(metrics));
      return;
    }

    response.statusCode = 404;
    response.end('not found');
  });
  server.listen({ host: '0.0.0.0', port });
  return server;
}

function createKafkaEnvelopePublisher(brokers: string[]): EnvelopePublisher {
  const producer = new Producer({
    clientId: 'pubsub-broadcaster-producer',
    bootstrapBrokers: brokers,
  });
  let connected = false;

  return {
    async connect() {
      if (!connected) {
        connected = true;
      }
    },
    async disconnect() {
      if (connected) {
        connected = false;
      }
    },
    async publish(topic: string, key: string, envelope: Envelope) {
      await producer.send({
        messages: [
          {
            topic,
            key,
            value: JSON.stringify(envelope),
          },
        ],
      });
    },
  };
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

function isTransientPublishError(error: unknown): boolean {
  if (error && typeof error === 'object' && 'terminal' in error) {
    return false;
  }

  if (error && typeof error === 'object' && 'retriable' in error) {
    return Boolean((error as { retriable?: unknown }).retriable);
  }

  const message = error instanceof Error ? error.message.toLowerCase() : '';
  return (
    message.includes('timeout') ||
    message.includes('temporar') ||
    message.includes('network') ||
    message.includes('econn') ||
    message.includes('unavailable')
  );
}

async function sleep(ms: number): Promise<void> {
  await new Promise<void>((resolve) => {
    setTimeout(resolve, ms);
  });
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
