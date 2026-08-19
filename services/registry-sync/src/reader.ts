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
import Redis from 'ioredis';
import type { SensorAuth } from '@scp/core';
import { loadRegistrySyncConfig } from './config.js';
import { createRedisKeyspace } from './keyspace.js';
import { RedisProjectionStore, type RedisLike } from './projection-store.js';

export interface SensorRegistryRecord {
  sensorId: Uint8Array;
  enabled: boolean;
}

export interface RegistryReader extends SensorAuth {
  getSensorRecord(sensorId: Uint8Array): Promise<SensorRegistryRecord | null>;
  isNonceSeen(sensorId: Uint8Array, nonce: Uint8Array): Promise<boolean>;
  rememberNonce(sensorId: Uint8Array, nonce: Uint8Array): Promise<void>;
}

function toHex(bytes: Uint8Array): string {
  return Buffer.from(bytes).toString('hex');
}

export class InMemoryRegistryReader implements RegistryReader {
  private readonly sensors = new Map<string, SensorRegistryRecord>();
  private readonly seenNonces = new Set<string>();

  constructor(seed: SensorRegistryRecord[] = []) {
    seed.forEach((record) => {
      this.sensors.set(toHex(record.sensorId), record);
    });
  }

  async authenticate(sensorId: Uint8Array): Promise<boolean> {
    const record = await this.getSensorRecord(sensorId);
    return record !== null && record.enabled;
  }

  async getSensorRecord(
    sensorId: Uint8Array
  ): Promise<SensorRegistryRecord | null> {
    return this.sensors.get(toHex(sensorId)) ?? null;
  }

  async isNonceSeen(sensorId: Uint8Array, nonce: Uint8Array): Promise<boolean> {
    return this.seenNonces.has(`${toHex(sensorId)}:${toHex(nonce)}`);
  }

  async rememberNonce(sensorId: Uint8Array, nonce: Uint8Array): Promise<void> {
    this.seenNonces.add(`${toHex(sensorId)}:${toHex(nonce)}`);
  }
}

export class RedisRegistryReader implements RegistryReader {
  private readonly projectionStore: RedisProjectionStore;

  constructor(
    private readonly redis: RedisLike,
    redisKeyPrefix: string,
    private readonly nonceTtlSeconds: number
  ) {
    this.projectionStore = new RedisProjectionStore(
      redis,
      createRedisKeyspace(redisKeyPrefix)
    );
  }

  async authenticate(sensorId: Uint8Array): Promise<boolean> {
    const record = await this.getSensorRecord(sensorId);
    return record !== null && record.enabled;
  }

  async getSensorRecord(
    sensorId: Uint8Array
  ): Promise<SensorRegistryRecord | null> {
    const sensorIdHex = toHex(sensorId);
    const record = await this.projectionStore.readSensor(sensorIdHex);
    if (!record) {
      return null;
    }

    return {
      sensorId,
      enabled: record.enabled,
    };
  }

  async isNonceSeen(sensorId: Uint8Array, nonce: Uint8Array): Promise<boolean> {
    return this.projectionStore.isNonceSeen(toHex(sensorId), toHex(nonce));
  }

  async rememberNonce(sensorId: Uint8Array, nonce: Uint8Array): Promise<void> {
    await this.projectionStore.rememberNonce(
      toHex(sensorId),
      toHex(nonce),
      this.nonceTtlSeconds
    );
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
  sharedRedisReader = new RedisRegistryReader(
    redis,
    config.redisKeyPrefix,
    config.nonceTtlSeconds
  );
  return sharedRedisReader;
}
