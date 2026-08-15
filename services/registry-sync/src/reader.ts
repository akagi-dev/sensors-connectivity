import Redis from 'ioredis';
import type { SensorAuth } from '@scp/contracts';
import { loadRegistrySyncConfig } from './config.js';
import { createRedisKeyspace } from './keyspace.js';
import { RedisProjectionStore, type RedisLike } from './projection-store.js';

export interface SensorRegistryRecord {
  sensorId: string;
  enabled: boolean;
}

export interface RegistryReader extends SensorAuth {
  getSensorRecord(sensorId: string): Promise<SensorRegistryRecord | null>;
  isNonceSeen(sensorId: string, nonce: string): Promise<boolean>;
  rememberNonce(sensorId: string, nonce: string): Promise<void>;
}

export class InMemoryRegistryReader implements RegistryReader {
  private readonly sensors = new Map<string, SensorRegistryRecord>();
  private readonly seenNonces = new Set<string>();

  constructor(seed: SensorRegistryRecord[] = []) {
    seed.forEach((record) => {
      this.sensors.set(record.sensorId, record);
    });
  }

  async authenticate(sensorId: string): Promise<boolean> {
    const record = await this.getSensorRecord(sensorId);
    return record !== null && record.enabled;
  }

  async getSensorRecord(sensorId: string): Promise<SensorRegistryRecord | null> {
    return this.sensors.get(sensorId) ?? null;
  }

  async isNonceSeen(sensorId: string, nonce: string): Promise<boolean> {
    return this.seenNonces.has(`${sensorId}:${nonce}`);
  }

  async rememberNonce(sensorId: string, nonce: string): Promise<void> {
    this.seenNonces.add(`${sensorId}:${nonce}`);
  }
}

export class RedisRegistryReader implements RegistryReader {
  private readonly projectionStore: RedisProjectionStore;

  constructor(
    private readonly redis: RedisLike,
    redisKeyPrefix: string,
    private readonly nonceTtlSeconds: number
  ) {
    this.projectionStore = new RedisProjectionStore(redis, createRedisKeyspace(redisKeyPrefix));
  }

  async authenticate(sensorId: string): Promise<boolean> {
    const record = await this.getSensorRecord(sensorId);
    return record !== null && record.enabled;
  }

  async getSensorRecord(sensorId: string): Promise<SensorRegistryRecord | null> {
    const record = await this.projectionStore.readSensor(sensorId);
    if (!record) {
      return null;
    }

    return {
      sensorId: record.sensorId,
      enabled: record.enabled
    };
  }

  async isNonceSeen(sensorId: string, nonce: string): Promise<boolean> {
    return this.projectionStore.isNonceSeen(sensorId, nonce);
  }

  async rememberNonce(sensorId: string, nonce: string): Promise<void> {
    await this.projectionStore.rememberNonce(sensorId, nonce, this.nonceTtlSeconds);
  }
}

let sharedRedisReader: RedisRegistryReader | null = null;
type RedisConstructor = new (url: string) => RedisLike;

export function createRegistryReaderFromEnv(): RegistryReader {
  if (sharedRedisReader) {
    return sharedRedisReader;
  }

  const config = loadRegistrySyncConfig();
  const RedisClient = Redis as unknown as RedisConstructor;
  const redis = new RedisClient(config.redisUrl);
  sharedRedisReader = new RedisRegistryReader(redis, config.redisKeyPrefix, config.nonceTtlSeconds);
  return sharedRedisReader;
}
