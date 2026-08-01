export interface RegistrySyncConfig {
  substrateWsUrl: string;
  redisUrl: string;
  logLevel: string;
  redisKeyPrefix: string;
  healthPort: number;
  maxRetries: number;
  retryBackoffMs: number;
  nonceTtlSeconds: number;
}

function parsePositiveInt(value: string | undefined, fallback: number): number {
  const parsed = Number.parseInt(value ?? '', 10);
  return Number.isFinite(parsed) && parsed > 0 ? parsed : fallback;
}

export function loadRegistrySyncConfig(env: NodeJS.ProcessEnv = process.env): RegistrySyncConfig {
  return {
    substrateWsUrl: env.SUBSTRATE_WS_URL ?? 'ws://localhost:9944',
    redisUrl: env.REDIS_URL ?? 'redis://localhost:6379',
    logLevel: env.LOG_LEVEL ?? 'info',
    redisKeyPrefix: env.REGISTRY_REDIS_PREFIX ?? 'registry-sync:v1',
    healthPort: parsePositiveInt(env.REGISTRY_SYNC_HEALTH_PORT, 3011),
    maxRetries: parsePositiveInt(env.REGISTRY_SYNC_MAX_RETRIES, 3),
    retryBackoffMs: parsePositiveInt(env.REGISTRY_SYNC_RETRY_BACKOFF_MS, 250),
    nonceTtlSeconds: parsePositiveInt(env.NONCE_TTL_SECONDS, 900)
  };
}
