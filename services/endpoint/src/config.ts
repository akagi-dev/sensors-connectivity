export interface EndpointConfig {
  port: number;
  source: string;
  kafkaBrokers: string[];
  timestampSkewSeconds: number;
  producerMaxAttempts: number;
  producerRetryBackoffMs: number;
}

function parsePositiveInt(value: string | undefined, fallback: number): number {
  const parsed = Number.parseInt(value ?? '', 10);
  return Number.isFinite(parsed) && parsed > 0 ? parsed : fallback;
}

function parseCsv(value: string | undefined, fallback: string): string[] {
  return (value ?? fallback)
    .split(',')
    .map((v) => v.trim())
    .filter(Boolean);
}

export function loadEndpointConfig(
  env: NodeJS.ProcessEnv = process.env
): EndpointConfig {
  return {
    port: parsePositiveInt(env.ENDPOINT_PORT, 3000),
    source: env.ENDPOINT_SOURCE ?? 'endpoint',
    kafkaBrokers: parseCsv(env.KAFKA_BROKERS, 'localhost:9092'),
    timestampSkewSeconds: parsePositiveInt(
      env.ENDPOINT_TIMESTAMP_SKEW_SECONDS,
      300
    ),
    producerMaxAttempts: parsePositiveInt(
      env.ENDPOINT_PRODUCER_MAX_ATTEMPTS,
      3
    ),
    producerRetryBackoffMs: parsePositiveInt(
      env.ENDPOINT_PRODUCER_RETRY_BACKOFF_MS,
      100
    ),
  };
}
