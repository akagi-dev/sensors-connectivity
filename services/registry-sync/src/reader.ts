import Redis from 'ioredis';
import { loadRegistrySyncConfig } from './config.js';
import { createRedisKeyspace } from './keyspace.js';
import { RedisProjectionStore, type RedisLike } from './projection-store.js';

export interface SensorRegistryRecord {
  sensorAddress: string;
  enabled: boolean;
}

export interface RegistryReader {
  getSensorRecord(sensorAddress: string): Promise<SensorRegistryRecord | null>;
  isNonceSeen(sensorAddress: string, nonce: string): Promise<boolean>;
  rememberNonce(sensorAddress: string, nonce: string): Promise<void>;
}

export class InMemoryRegistryReader implements RegistryReader {
  private readonly sensors = new Map<string, SensorRegistryRecord>();
  private readonly seenNonces = new Set<string>();

  constructor(seed: SensorRegistryRecord[] = []) {
    seed.forEach((record) => {
      this.sensors.set(record.sensorAddress, record);
    });
  }

  async getSensorRecord(sensorAddress: string): Promise<SensorRegistryRecord | null> {
    return this.sensors.get(sensorAddress) ?? null;
  }

  async isNonceSeen(sensorAddress: string, nonce: string): Promise<boolean> {
    return this.seenNonces.has(`${sensorAddress}:${nonce}`);
  }

  async rememberNonce(sensorAddress: string, nonce: string): Promise<void> {
    this.seenNonces.add(`${sensorAddress}:${nonce}`);
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

  async getSensorRecord(sensorAddress: string): Promise<SensorRegistryRecord | null> {
    const record = await this.projectionStore.readSensor(sensorAddress);
    if (!record) {
      return null;
    }

    return {
      sensorAddress: record.sensorAddress,
      enabled: record.enabled
    };
  }

  async isNonceSeen(sensorAddress: string, nonce: string): Promise<boolean> {
    return this.projectionStore.isNonceSeen(sensorAddress, nonce);
  }

  async rememberNonce(sensorAddress: string, nonce: string): Promise<void> {
    await this.projectionStore.rememberNonce(sensorAddress, nonce, this.nonceTtlSeconds);
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
