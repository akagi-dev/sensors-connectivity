export interface AuthorizerConfig {
  port: number;
  source: string;
  kafkaBrokers: string[];
}

export function loadAuthorizerConfig(env: NodeJS.ProcessEnv = process.env): AuthorizerConfig {
  return {
    port: Number(env.AUTHORIZER_PORT ?? 3000),
    source: env.AUTHORIZER_SOURCE ?? 'authorizer',
    kafkaBrokers: (env.KAFKA_BROKERS ?? 'localhost:9092').split(',')
  };
}
