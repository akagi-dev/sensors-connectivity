import type {
  RegistryProjectionRecord,
  ProjectionUpdate,
  RedisKeyspace,
} from './keyspace.js';

export interface RedisLike {
  get(key: string): Promise<string | null>;
  set(
    key: string,
    value: string,
    mode?: string,
    duration?: number,
    flag?: string
  ): Promise<unknown>;
  eval?(script: string, numkeys: number, ...args: string[]): Promise<unknown>;
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
    if (typeof this.redis.eval === 'function') {
      const script = `
local current = redis.call('GET', KEYS[1])
local next = tonumber(ARGV[1])
if (current == false) then
  redis.call('SET', KEYS[1], ARGV[1])
  return 1
end
local currentNumber = tonumber(current)
if (currentNumber == nil or next > currentNumber) then
  redis.call('SET', KEYS[1], ARGV[1])
  return 1
end
return 0
`;
      await this.redis.eval(
        script,
        1,
        this.keyspace.cursorHeight,
        String(height)
      );
      return;
    }

    const current = await this.loadCursorHeight();
    if (height > current) {
      await this.redis.set(this.keyspace.cursorHeight, String(height));
    }
  }

  async hasProcessed(eventId: string): Promise<boolean> {
    return (
      (await this.redis.sismember(this.keyspace.processedEvents, eventId)) === 1
    );
  }

  async markProcessed(eventId: string): Promise<void> {
    await this.redis.sadd(this.keyspace.processedEvents, eventId);
  }

  async applyProjection(
    update: ProjectionUpdate,
    context: Pick<RegistryProjectionRecord, 'updatedAtBlock' | 'updatedAtEvent'>
  ): Promise<void> {
    await this.redis.hset(this.keyspace.sensorState(update.sensorId), {
      sensor_id: update.sensorId,
      enabled: String(update.enabled),
      updated_at_block: String(context.updatedAtBlock),
      updated_at_event: context.updatedAtEvent,
    });
  }

  async readSensor(sensorId: string): Promise<RegistryProjectionRecord | null> {
    const entry = await this.redis.hgetall(this.keyspace.sensorState(sensorId));
    if (!entry.sensor_id || !entry.enabled) {
      return null;
    }

    return {
      sensorId: entry.sensor_id,
      enabled: entry.enabled === 'true',
      updatedAtBlock: Number.parseInt(entry.updated_at_block ?? '0', 10) || 0,
      updatedAtEvent: entry.updated_at_event ?? '',
    };
  }

  async isNonceSeen(sensorId: string, nonce: string): Promise<boolean> {
    return (
      (await this.redis.exists(this.keyspace.nonceState(sensorId, nonce))) === 1
    );
  }

  async rememberNonce(
    sensorId: string,
    nonce: string,
    ttlSeconds: number
  ): Promise<void> {
    await this.redis.set(
      this.keyspace.nonceState(sensorId, nonce),
      '1',
      'EX',
      ttlSeconds,
      'NX'
    );
  }

  async publishDlq(payload: object): Promise<void> {
    await this.redis.rpush(this.keyspace.dlqEvents, JSON.stringify(payload));
  }
}
