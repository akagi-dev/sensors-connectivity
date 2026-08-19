/**
 * Copyright 2026 Robonomics Network
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
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

export function loadRegistrySyncConfig(
  env: NodeJS.ProcessEnv = process.env
): RegistrySyncConfig {
  return {
    substrateWsUrl: env.SUBSTRATE_WS_URL ?? 'ws://localhost:9944',
    redisUrl: env.REDIS_URL ?? 'redis://localhost:6379',
    logLevel: env.LOG_LEVEL ?? 'info',
    redisKeyPrefix: env.REGISTRY_REDIS_PREFIX ?? 'registry-sync:v1',
    healthPort: parsePositiveInt(env.REGISTRY_SYNC_HEALTH_PORT, 3011),
    maxRetries: parsePositiveInt(env.REGISTRY_SYNC_MAX_RETRIES, 3),
    retryBackoffMs: parsePositiveInt(env.REGISTRY_SYNC_RETRY_BACKOFF_MS, 250),
    nonceTtlSeconds: parsePositiveInt(env.NONCE_TTL_SECONDS, 900),
  };
}
