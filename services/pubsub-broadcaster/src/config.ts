export interface PubsubBroadcasterConfig {
  kafkaBrokers: string[];
  consumerGroupId: string;
  source: string;
  healthPort: number;
  maxRetries: number;
  retryBackoffMs: number;
  pubsubTopic: string;
  ipfsApiUrl: string;
}

function parsePositiveInt(value: string | undefined, fallback: number): number {
  const parsed = Number.parseInt(value ?? '', 10);
  return Number.isFinite(parsed) && parsed >= 0 ? parsed : fallback;
}

export function loadPubsubBroadcasterConfig(
  env: NodeJS.ProcessEnv = process.env
): PubsubBroadcasterConfig {
  return {
    kafkaBrokers: (env.KAFKA_BROKERS ?? 'localhost:9092')
      .split(',')
      .map((broker) => broker.trim())
      .filter((broker) => broker.length > 0),
    consumerGroupId: env.PUBSUB_BROADCASTER_GROUP_ID ?? 'pubsub-broadcaster-v1',
    source: env.PUBSUB_BROADCASTER_SOURCE ?? 'pubsub-broadcaster',
    healthPort: parsePositiveInt(env.PUBSUB_BROADCASTER_HEALTH_PORT, 3020),
    maxRetries: parsePositiveInt(env.PUBSUB_BROADCASTER_MAX_RETRIES, 3),
    retryBackoffMs: parsePositiveInt(
      env.PUBSUB_BROADCASTER_RETRY_BACKOFF_MS,
      250
    ),
    pubsubTopic: env.PUBSUB_TOPIC ?? 'sensors.social/telemetry/v1',
    ipfsApiUrl: env.IPFS_API_URL ?? 'http://localhost:5001',
  };
}
