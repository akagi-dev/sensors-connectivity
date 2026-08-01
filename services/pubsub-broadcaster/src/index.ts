import { gossipsub } from '@chainsafe/libp2p-gossipsub';
import {
  InMemoryRetryCounterStore,
  TELEMETRY_TOPICS,
  runConsumerProcessingRule,
  validateEnvelopeWithKnownPayload,
  type Envelope,
  type RetryDlqPublisher,
  type TelemetryAuthorizedPayload
} from '@scp/contracts';
import { Kafka, type Consumer, type EachBatchPayload, type Producer } from 'kafkajs';
import { createServer, type Server } from 'node:http';
import { randomUUID } from 'node:crypto';
import { fileURLToPath } from 'node:url';
import { createLibp2p } from 'libp2p';
import { loadPubsubBroadcasterConfig, type PubsubBroadcasterConfig } from './config.js';

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

export type AuthorizedTelemetryEnvelope = Envelope & {
  event_type: typeof TELEMETRY_TOPICS.AUTHORIZED;
  payload: TelemetryAuthorizedPayload;
};

interface PubsubClient {
  start(): Promise<void>;
  stop(): Promise<void>;
  connectReservedPeers(peers: string[]): Promise<void>;
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
  createConsumer?: () => Consumer;
  idempotencyStore?: EventIdStore;
  createHealthServer?: (metrics: PubsubBroadcasterMetrics, port: number) => Server;
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

export async function processAuthorizedEnvelope(
  envelope: AuthorizedTelemetryEnvelope,
  commitOffset: () => Promise<void>,
  context: ProcessingContext
): Promise<'processed' | 'duplicate' | 'dlq'> {
  let pending = true;
  let finalStatus: 'processed' | 'duplicate' | 'dlq' = 'processed';

  while (pending) {
    let publishStatus: 'submitted' | 'failed' = 'submitted';
    let publishError: Error | undefined;
    const status = await runConsumerProcessingRule(envelope, {
      retryPolicy: {
        maxAttempts: context.config.maxRetries,
        getEventId: (current) => current.event_id,
        store: context.retryStore
      },
      idempotency: {
        getEventId: (current) => current.event_id,
        hasProcessed: (eventId) => context.idempotencyStore.has(eventId),
        markProcessed: (eventId) => context.idempotencyStore.mark(eventId)
      },
      performExternalAction: async (current) => {
        const encodedPayload = Buffer.from(JSON.stringify(current.payload));

        try {
          await context.pubsub.publish(context.config.pubsubTopic, encodedPayload);
          context.metrics.publishSuccess += 1;
        } catch (error) {
          context.metrics.publishFailure += 1;
          if (isTransientPublishError(error)) {
            throw error;
          }

          publishStatus = 'failed';
          publishError = error instanceof Error ? error : new Error('Unknown publish error');
        }
      },
      waitForConfirmation: async () => {},
      emitResultEvent: async (current) => ({
        event_id: randomUUID(),
        event_type: TELEMETRY_TOPICS.PUBSUB_RESULT,
        event_version: 'v1',
        occurred_at: new Date().toISOString(),
        trace_id: current.trace_id,
        source: context.config.source,
        payload: {
          status: publishStatus,
          pubsub_topic: context.config.pubsubTopic,
          sensor_address: current.payload.sensor_address,
          nonce: current.payload.nonce,
          ...(publishError
            ? {
                error_code: 'publish_failed',
                error_message: publishError.message
              }
            : {})
        }
      }),
      publishResultEvent: async (result) => {
        await context.publisher.publish(
          TELEMETRY_TOPICS.PUBSUB_RESULT,
          `${result.payload.sensor_address}:${result.payload.nonce}`,
          result as Envelope
        );
      },
      commitOffset,
      retryDlqPublisher: createRetryDlqPublisher(context)
    });

    if (status === 'retried') {
      context.metrics.retryCount += 1;
      await sleep(context.config.retryBackoffMs);
      continue;
    }

    if (status === 'dlq') {
      context.metrics.dlqCount += 1;
      await commitOffset();
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
  const kafka = new Kafka({ clientId: 'pubsub-broadcaster', brokers: config.kafkaBrokers });
  const consumer = deps.createConsumer?.() ?? kafka.consumer({ groupId: config.consumerGroupId });
  const publisher = deps.createPublisher?.() ?? createKafkaEnvelopePublisher(kafka.producer());
  const idempotencyStore = deps.idempotencyStore ?? new InMemoryEventIdStore();
  const retryStore = new InMemoryRetryCounterStore();
  const createPubsubClient = deps.createPubsubClient ?? (() => createLibp2pPubsubClient());
  const createHealthServer = deps.createHealthServer ?? startHealthAndMetricsServer;
  const sleepFn = deps.sleep ?? sleep;

  let pubsubClient: PubsubClient | null = null;
  let started = false;
  let runPromise: Promise<void> | null = null;
  let healthServer: Server | null = null;
  const metrics: PubsubBroadcasterMetrics = {
    consumed: 0,
    processed: 0,
    publishSuccess: 0,
    publishFailure: 0,
    retryCount: 0,
    dlqCount: 0,
    consumerLag: 0
  };

  return {
    async start(): Promise<void> {
      if (started) {
        return;
      }

      started = true;
      pubsubClient = await createPubsubClient();
      await pubsubClient.start();
      await pubsubClient.connectReservedPeers(config.reservedPeers);
      await publisher.connect();
      await consumer.connect();
      await consumer.subscribe({ topic: TELEMETRY_TOPICS.AUTHORIZED, fromBeginning: false });
      healthServer = createHealthServer(metrics, config.healthPort);

      runPromise = consumer.run({
        autoCommit: false,
        eachBatchAutoResolve: false,
        eachBatch: async (batchPayload) => {
          await processBatch(batchPayload, {
            config,
            consumer,
            pubsub: pubsubClient!,
            publisher,
            idempotencyStore,
            retryStore,
            metrics,
            sleep: sleepFn
          });
        }
      });
    },
    async stop(): Promise<void> {
      if (!started) {
        return;
      }

      started = false;
      await consumer.stop();
      await consumer.disconnect();
      await publisher.disconnect();
      await pubsubClient?.stop();
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
    },
    getMetrics(): Readonly<PubsubBroadcasterMetrics> {
      return metrics;
    }
  };
}

interface BatchProcessingDeps {
  config: PubsubBroadcasterConfig;
  consumer: Consumer;
  pubsub: PubsubClient;
  publisher: EnvelopePublisher;
  idempotencyStore: EventIdStore;
  retryStore: InMemoryRetryCounterStore;
  metrics: PubsubBroadcasterMetrics;
  sleep: (ms: number) => Promise<void>;
}

async function processBatch(payload: EachBatchPayload, deps: BatchProcessingDeps): Promise<void> {
  for (const message of payload.batch.messages) {
    if (!payload.isRunning() || payload.isStale()) {
      return;
    }

    deps.metrics.consumed += 1;
    const lag = BigInt(payload.batch.highWatermark) - BigInt(message.offset) - 1n;
    const clampedLag = lag < 0n ? 0n : lag;
    deps.metrics.consumerLag =
      clampedLag > BigInt(Number.MAX_SAFE_INTEGER) ? Number.MAX_SAFE_INTEGER : Number(clampedLag);
    const commitOffset = async () => {
      await deps.consumer.commitOffsets([
        {
          topic: payload.batch.topic,
          partition: payload.batch.partition,
          offset: incrementOffset(message.offset)
        }
      ]);
      payload.resolveOffset(message.offset);
    };

    const raw = message.value?.toString('utf8') ?? '';
    const parsed = safeJsonParse(raw);
    if (!parsed.success) {
      await publishInvalidMessageDlq(raw, parsed.error, deps, commitOffset);
      await payload.heartbeat();
      continue;
    }

    const envelopeResult = validateEnvelopeWithKnownPayload(parsed.data);
    if (!envelopeResult.success || envelopeResult.data.event_type !== TELEMETRY_TOPICS.AUTHORIZED) {
      const reason = envelopeResult.success
        ? `Unsupported event type: ${envelopeResult.data.event_type}`
        : envelopeResult.error.message;
      await publishInvalidMessageDlq(parsed.data, reason, deps, commitOffset);
      await payload.heartbeat();
      continue;
    }

    await processAuthorizedEnvelope(envelopeResult.data as AuthorizedTelemetryEnvelope, commitOffset, {
      config: deps.config,
      pubsub: deps.pubsub,
      publisher: deps.publisher,
      idempotencyStore: deps.idempotencyStore,
      retryStore: deps.retryStore,
      metrics: deps.metrics
    });
    await payload.heartbeat();
  }
}

async function publishInvalidMessageDlq(
  failedPayload: unknown,
  reason: string,
  deps: BatchProcessingDeps,
  commitOffset: () => Promise<void>
): Promise<void> {
  deps.metrics.dlqCount += 1;
  await deps.publisher.publish(TELEMETRY_TOPICS.DLQ, 'invalid-envelope', {
    event_id: randomUUID(),
    event_type: TELEMETRY_TOPICS.DLQ,
    event_version: 'v1',
    occurred_at: new Date().toISOString(),
    source: deps.config.source,
    payload: {
      failed_topic: TELEMETRY_TOPICS.AUTHORIZED,
      reason_code: 'invalid_envelope',
      reason_message: reason,
      failed_payload: failedPayload
    }
  });
  await commitOffset();
}

function createRetryDlqPublisher(context: ProcessingContext): RetryDlqPublisher<Envelope> {
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
          context: failureContext
        }
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
          context: failureContext
        }
      });
    }
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
          consumer_lag: metrics.consumerLag
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

function createKafkaEnvelopePublisher(producer: Producer): EnvelopePublisher {
  let connected = false;
  let connectPromise: Promise<void> | undefined;

  async function ensureConnected() {
    if (!connected) {
      if (!connectPromise) {
        connectPromise = producer.connect().then(() => {
          connected = true;
        });
      }

      try {
        await connectPromise;
      } catch (error) {
        connectPromise = undefined;
        throw error;
      }
    }
  }

  return {
    connect: () => ensureConnected(),
    async disconnect() {
      if (connectPromise) {
        try {
          await connectPromise;
        } catch {
          // ignore connect failures; producer isn't connected
        }
      }

      if (!connected) {
        connectPromise = undefined;
        return;
      }

      await producer.disconnect();
      connected = false;
      connectPromise = undefined;
    }
    async publish(topic: string, key: string, envelope: Envelope) {
      await ensureConnected();
      await producer.send({
        topic,
        messages: [
          {
            key,
            value: JSON.stringify(envelope)
          }
        ]
      });
    }
  };
}

async function createLibp2pPubsubClient(): Promise<PubsubClient> {
  const node = await createLibp2p({
    services: {
      pubsub: gossipsub() as never
    } as never
  });

  const reservedPeers = new Set<string>();
  let redialTimer: ReturnType<typeof setInterval> | null = null;

  return {
    async start() {
      await node.start();
    },
    async stop() {
      if (redialTimer) {
        clearInterval(redialTimer);
        redialTimer = null;
      }
      await node.stop();
    },
    async connectReservedPeers(peers: string[]) {
      for (const peer of peers) {
        reservedPeers.add(peer);
        await safeDial(node, peer);
      }

      if (redialTimer) {
        clearInterval(redialTimer);
      }

      redialTimer = setInterval(() => {
        for (const peer of reservedPeers) {
          void safeDial(node, peer);
        }
      }, 30_000);
    },
    async publish(topic: string, data: Uint8Array) {
      await (node.services.pubsub as { publish: (currentTopic: string, currentData: Uint8Array) => Promise<void> }).publish(topic, data);
    }
  };
}

async function safeDial(node: Awaited<ReturnType<typeof createLibp2p>>, multiaddr: string) {
  try {
    await (node as unknown as { dial: (target: unknown) => Promise<unknown> }).dial(multiaddr);
  } catch (error) {
    const message = error instanceof Error ? error.message : 'Unknown dial error';
    console.warn('[pubsub-broadcaster] reserved peer dial failed', {
      peer: multiaddr,
      error: message
    });
  }
}

function safeJsonParse(raw: string): { success: true; data: unknown } | { success: false; error: string } {
  try {
    return { success: true, data: JSON.parse(raw) };
  } catch (error) {
    return {
      success: false,
      error: error instanceof Error ? error.message : 'Invalid JSON payload'
    };
  }
}

function incrementOffset(offset: string): string {
  return (BigInt(offset) + 1n).toString();
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
  console.log('[pubsub-broadcaster] started');
  return service;
}

const isDirectRun = process.argv[1] === fileURLToPath(import.meta.url);
if (isDirectRun) {
  startPubsubBroadcaster().catch((error: unknown) => {
    console.error('[pubsub-broadcaster] failed to start', error);
    process.exitCode = 1;
  });
}
