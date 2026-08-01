export interface AuthorizerConfig {
  port: number;
  source: string;
  kafkaBrokers: string[];
  timestampSkewSeconds: number;
  producerMaxAttempts: number;
  producerRetryBackoffMs: number;
}

export function loadAuthorizerConfig(env: NodeJS.ProcessEnv = process.env): AuthorizerConfig {
  return {
    port: Number(env.AUTHORIZER_PORT ?? 3000),
    source: env.AUTHORIZER_SOURCE ?? 'authorizer',
    kafkaBrokers: (env.KAFKA_BROKERS ?? 'localhost:9092').split(','),
    timestampSkewSeconds: Number(env.AUTHORIZER_TIMESTAMP_SKEW_SECONDS ?? 300),
    producerMaxAttempts: Number(env.AUTHORIZER_PRODUCER_MAX_ATTEMPTS ?? 3),
    producerRetryBackoffMs: Number(env.AUTHORIZER_PRODUCER_RETRY_BACKOFF_MS ?? 100)
  };
}
