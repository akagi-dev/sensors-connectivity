import type { RetryCounterStore } from '@scp/contracts';
import type { RegistryProjectionRecord, ProjectionUpdate, RedisKeyspace } from './keyspace.js';

export interface RedisLike {
  get(key: string): Promise<string | null>;
  set(key: string, value: string, mode?: string, duration?: number, flag?: string): Promise<unknown>;
  exists(key: string): Promise<number>;
  sadd(key: string, member: string): Promise<number>;
  sismember(key: string, member: string): Promise<number>;
  hset(key: string, map: Record<string, string>): Promise<number>;
  hgetall(key: string): Promise<Record<string, string>>;
  rpush(key: string, value: string): Promise<number>;
  incr(key: string): Promise<number>;
  del(key: string): Promise<number>;
  quit?(): Promise<unknown>;
  disconnect?(): void;
}

export class RedisProjectionStore {
  constructor(
    private readonly redis: RedisLike,
    private readonly keyspace: RedisKeyspace
  ) {}

  async loadCursorHeight(): Promise<number> {
    const raw = await this.redis.get(this.keyspace.cursorHeight);
    if (!raw) {
      return 0;
    }

    const parsed = Number.parseInt(raw, 10);
    return Number.isFinite(parsed) && parsed >= 0 ? parsed : 0;
  }

  async commitCursorHeight(height: number): Promise<void> {
    const current = await this.loadCursorHeight();
    if (height > current) {
      await this.redis.set(this.keyspace.cursorHeight, String(height));
    }
  }

  async hasProcessed(eventId: string): Promise<boolean> {
    return (await this.redis.sismember(this.keyspace.processedEvents, eventId)) === 1;
  }

  async markProcessed(eventId: string): Promise<void> {
    await this.redis.sadd(this.keyspace.processedEvents, eventId);
  }

  async applyProjection(
    update: ProjectionUpdate,
    context: Pick<RegistryProjectionRecord, 'updatedAtBlock' | 'updatedAtEvent'>
  ): Promise<void> {
    await this.redis.hset(this.keyspace.sensorState(update.sensorAddress), {
      sensor_address: update.sensorAddress,
      public_key: update.publicKey,
      enabled: String(update.enabled),
      updated_at_block: String(context.updatedAtBlock),
      updated_at_event: context.updatedAtEvent
    });

    await this.redis.hset(this.keyspace.keyState(update.publicKey), {
      public_key: update.publicKey,
      sensor_address: update.sensorAddress,
      enabled: String(update.enabled),
      updated_at_block: String(context.updatedAtBlock),
      updated_at_event: context.updatedAtEvent
    });
  }

  async readSensor(sensorAddress: string): Promise<RegistryProjectionRecord | null> {
    const entry = await this.redis.hgetall(this.keyspace.sensorState(sensorAddress));
    if (!entry.sensor_address || !entry.public_key || !entry.enabled) {
      return null;
    }

    return {
      sensorAddress: entry.sensor_address,
      publicKey: entry.public_key,
      enabled: entry.enabled === 'true',
      updatedAtBlock: Number.parseInt(entry.updated_at_block ?? '0', 10) || 0,
      updatedAtEvent: entry.updated_at_event ?? ''
    };
  }

  async isNonceSeen(sensorAddress: string, nonce: string): Promise<boolean> {
    return (await this.redis.exists(this.keyspace.nonceState(sensorAddress, nonce))) === 1;
  }

  async rememberNonce(sensorAddress: string, nonce: string, ttlSeconds: number): Promise<void> {
    await this.redis.set(this.keyspace.nonceState(sensorAddress, nonce), '1', 'EX', ttlSeconds, 'NX');
  }

  async publishDlq(payload: object): Promise<void> {
    await this.redis.rpush(this.keyspace.dlqEvents, JSON.stringify(payload));
  }
}

export class RedisRetryCounterStore implements RetryCounterStore {
  constructor(
    private readonly redis: RedisLike,
    private readonly keyspace: RedisKeyspace
  ) {}

  async getAttempts(eventId: string): Promise<number> {
    const raw = await this.redis.get(this.keyspace.retryAttempts(eventId));
    return Number.parseInt(raw ?? '0', 10) || 0;
  }

  async setAttempts(eventId: string, attempts: number): Promise<void> {
    await this.redis.set(this.keyspace.retryAttempts(eventId), String(attempts));
  }

  async clearAttempts(eventId: string): Promise<void> {
    await this.redis.del(this.keyspace.retryAttempts(eventId));
  }
}
