export interface RegistrySyncConfig {
  substrateWsUrl: string;
  redisUrl: string;
  logLevel: string;
}

export function loadRegistrySyncConfig(env: NodeJS.ProcessEnv = process.env): RegistrySyncConfig {
  return {
    substrateWsUrl: env.SUBSTRATE_WS_URL ?? 'ws://localhost:9944',
    redisUrl: env.REDIS_URL ?? 'redis://localhost:6379',
    logLevel: env.LOG_LEVEL ?? 'info'
  };
}
