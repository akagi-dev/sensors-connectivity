export interface IpfsPublisherConfig {
  kafkaBrokers: string[];
  consumerGroupId: string;
  batchMaxEvents: number;
  batchMaxWaitMs: number;
  maxRetries: number;
  retryBackoffMs: number;
  ipfsApiUrl: string;
  healthPort: number;
}

/** Reads a positive integer from an optional environment variable. */
function parsePositiveInteger(
  value: string | undefined,
  fallback: number,
  name: string
): number {
  const parsed = value === undefined ? fallback : Number(value);
  if (!Number.isInteger(parsed) || parsed < 1) {
    throw new Error(`${name} must be a positive integer`);
  }
  return parsed;
}

/** Reads a non-negative integer from an optional environment variable. */
function parseNonNegativeInteger(
  value: string | undefined,
  fallback: number,
  name: string
): number {
  const parsed = value === undefined ? fallback : Number(value);
  if (!Number.isInteger(parsed) || parsed < 0) {
    throw new Error(`${name} must be a non-negative integer`);
  }
  return parsed;
}

export function loadIpfsPublisherConfig(
  env: NodeJS.ProcessEnv = process.env
): IpfsPublisherConfig {
  return {
    kafkaBrokers: (env.KAFKA_BROKERS ?? 'localhost:9092')
      .split(',')
      .map((broker) => broker.trim())
      .filter((broker) => broker.length > 0),
    consumerGroupId: env.IPFS_PUBLISHER_GROUP_ID ?? 'ipfs-publisher-v1',
    batchMaxEvents: parsePositiveInteger(
      env.IPFS_PUBLISHER_BATCH_MAX_EVENTS,
      100,
      'IPFS_PUBLISHER_BATCH_MAX_EVENTS'
    ),
    batchMaxWaitMs: parsePositiveInteger(
      env.IPFS_PUBLISHER_BATCH_MAX_WAIT_MS,
      1_000,
      'IPFS_PUBLISHER_BATCH_MAX_WAIT_MS'
    ),
    maxRetries: parsePositiveInteger(
      env.IPFS_PUBLISHER_MAX_RETRIES,
      3,
      'IPFS_PUBLISHER_MAX_RETRIES'
    ),
    retryBackoffMs: parseNonNegativeInteger(
      env.IPFS_PUBLISHER_RETRY_BACKOFF_MS,
      250,
      'IPFS_PUBLISHER_RETRY_BACKOFF_MS'
    ),
    ipfsApiUrl: env.IPFS_API_URL ?? 'http://localhost:5001',
    healthPort: parsePositiveInteger(
      env.IPFS_PUBLISHER_HEALTH_PORT,
      3_040,
      'IPFS_PUBLISHER_HEALTH_PORT'
    )
  };
}
