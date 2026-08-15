export interface EndpointConfig {
  port: number;
  source: string;
  kafkaBrokers: string[];
  timestampSkewSeconds: number;
  producerMaxAttempts: number;
  producerRetryBackoffMs: number;
}

export function loadEndpointConfig(env: NodeJS.ProcessEnv = process.env): EndpointConfig {
  return {
    port: Number(env.ENDPOINT_PORT ?? 3000),
    source: env.ENDPOINT_SOURCE ?? 'endpoint',
    kafkaBrokers: (env.KAFKA_BROKERS ?? 'localhost:9092').split(','),
    timestampSkewSeconds: Number(env.ENDPOINT_TIMESTAMP_SKEW_SECONDS ?? 300),
    producerMaxAttempts: Number(env.ENDPOINT_PRODUCER_MAX_ATTEMPTS ?? 3),
    producerRetryBackoffMs: Number(env.ENDPOINT_PRODUCER_RETRY_BACKOFF_MS ?? 100)
  };
}
