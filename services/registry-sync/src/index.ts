import { ApiPromise, WsProvider } from '@polkadot/api';
import Redis from 'ioredis';
import { fileURLToPath } from 'node:url';
import { loadRegistrySyncConfig } from './config.js';
export * from './config.js';
export * from './reader.js';

export interface RegistrySyncService {
  start(): Promise<void>;
  stop(): Promise<void>;
}

export function createRegistrySyncService(): RegistrySyncService {
  const config = loadRegistrySyncConfig();

  return {
    async start(): Promise<void> {
      // TODO: connect to substrate via ApiPromise and consume finalized registry events.
      const _provider = new WsProvider(config.substrateWsUrl, 0);
      void _provider;

      // TODO: persist registry projection records to redis for authorizer reads.
      void Redis;

      console.log('[registry-sync] started in stub mode', {
        substrateWsUrl: config.substrateWsUrl,
        redisUrl: config.redisUrl
      });

      // Avoid eager network startup in skeleton mode.
      void ApiPromise;
    },
    async stop(): Promise<void> {
      console.log('[registry-sync] stopped');
    }
  };
}

const isDirectRun = process.argv[1] === fileURLToPath(import.meta.url);
if (isDirectRun) {
  const service = createRegistrySyncService();
  service.start().catch((error: unknown) => {
    console.error('[registry-sync] failed to start', error);
    process.exitCode = 1;
  });
}
