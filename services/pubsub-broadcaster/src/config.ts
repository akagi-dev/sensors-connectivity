export interface PubsubBroadcasterConfig {
  kafkaBrokers: string[];
  consumerGroupId: string;
  source: string;
  healthPort: number;
  maxRetries: number;
  retryBackoffMs: number;
  pubsubTopic: string;
  reservedPeers: string[];
}

function parsePositiveInt(value: string | undefined, fallback: number): number {
  const parsed = Number.parseInt(value ?? '', 10);
  return Number.isFinite(parsed) && parsed > 0 ? parsed : fallback;
}

function parseCsvEnv(value: string | undefined): string[] {
  if (!value) {
    return [];
  }

  return value
    .split(',')
    .map((item) => item.trim())
    .filter((item) => item.length > 0);
}

export function loadPubsubBroadcasterConfig(env: NodeJS.ProcessEnv = process.env): PubsubBroadcasterConfig {
  return {
    kafkaBrokers: (env.KAFKA_BROKERS ?? 'localhost:9092')
      .split(',')
      .map((broker) => broker.trim())
      .filter((broker) => broker.length > 0),
    consumerGroupId: env.PUBSUB_BROADCASTER_GROUP_ID ?? 'pubsub-broadcaster-v1',
    source: env.PUBSUB_BROADCASTER_SOURCE ?? 'pubsub-broadcaster',
    healthPort: parsePositiveInt(env.PUBSUB_BROADCASTER_HEALTH_PORT, 3020),
    maxRetries: parsePositiveInt(env.PUBSUB_BROADCASTER_MAX_RETRIES, 3),
    retryBackoffMs: parsePositiveInt(env.PUBSUB_BROADCASTER_RETRY_BACKOFF_MS, 250),
    pubsubTopic: env.PUBSUB_TOPIC ?? 'telemetry/authorized/v1',
    reservedPeers: parseCsvEnv(env.PUBSUB_RESERVED_PEERS)
  };
}
