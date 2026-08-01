import type { FinalizedRegistryEventSource } from '../src/chain-source.js';
import type { RegistryEvent } from '../src/keyspace.js';
import type { RedisLike } from '../src/projection-store.js';

export class FakeRedis implements RedisLike {
  private readonly values = new Map<string, string>();
  private readonly hashes = new Map<string, Record<string, string>>();
  private readonly sets = new Map<string, Set<string>>();
  private readonly lists = new Map<string, string[]>();
  public failHsetTimes = 0;

  async get(key: string): Promise<string | null> {
    return this.values.get(key) ?? null;
  }

  async set(
    key: string,
    value: string,
    mode?: string,
    duration?: number,
    flag?: string
  ): Promise<'OK' | null> {
    if (flag === 'NX' && this.values.has(key)) {
      return null;
    }

    this.values.set(key, value);
    if (mode === 'EX' && typeof duration === 'number' && duration > 0) {
      void duration;
    }
    return 'OK';
  }

  async eval(_script: string, numkeys: number, ...args: string[]): Promise<number> {
    if (numkeys !== 1 || args.length < 2) {
      return 0;
    }

    const [key, rawHeight] = args;
    if (!key || !rawHeight) {
      return 0;
    }

    const next = Number.parseInt(rawHeight, 10);
    const current = Number.parseInt(this.values.get(key) ?? '', 10);
    if (!Number.isFinite(current) || next > current) {
      this.values.set(key, rawHeight);
      return 1;
    }

    return 0;
  }

  async exists(key: string): Promise<number> {
    return this.values.has(key) ? 1 : 0;
  }

  async sadd(key: string, member: string): Promise<number> {
    const set = this.sets.get(key) ?? new Set<string>();
    const before = set.size;
    set.add(member);
    this.sets.set(key, set);
    return set.size > before ? 1 : 0;
  }

  async sismember(key: string, member: string): Promise<number> {
    return this.sets.get(key)?.has(member) ? 1 : 0;
  }

  async hset(key: string, map: Record<string, string>): Promise<number> {
    if (this.failHsetTimes > 0) {
      this.failHsetTimes -= 1;
      throw new Error('transient redis failure');
    }

    const current = this.hashes.get(key) ?? {};
    this.hashes.set(key, { ...current, ...map });
    return Object.keys(map).length;
  }

  async hgetall(key: string): Promise<Record<string, string>> {
    return { ...(this.hashes.get(key) ?? {}) };
  }

  async rpush(key: string, value: string): Promise<number> {
    const list = this.lists.get(key) ?? [];
    list.push(value);
    this.lists.set(key, list);
    return list.length;
  }

  async incr(key: string): Promise<number> {
    const next = Number.parseInt(this.values.get(key) ?? '0', 10) + 1;
    this.values.set(key, String(next));
    return next;
  }

  async del(key: string): Promise<number> {
    const existed = this.values.delete(key);
    return existed ? 1 : 0;
  }

  async quit(): Promise<'OK'> {
    return 'OK';
  }

  getList(key: string): string[] {
    return [...(this.lists.get(key) ?? [])];
  }
}

export class FixtureEventSource implements FinalizedRegistryEventSource {
  constructor(private readonly events: RegistryEvent[]) {}

  async connect(): Promise<void> {
    return;
  }

  async disconnect(): Promise<void> {
    return;
  }

  async getLatestFinalizedHeight(): Promise<number> {
    return this.events.reduce((max, current) => Math.max(max, current.blockHeight), 0);
  }

  async startFrom(
    fromInclusiveHeight: number,
    onEvent: (event: RegistryEvent) => Promise<void>,
    onFinalizedHead?: (height: number) => Promise<void> | void
  ): Promise<void> {
    for (const event of this.events) {
      if (event.blockHeight >= fromInclusiveHeight) {
        await onEvent(event);
        await onFinalizedHead?.(event.blockHeight);
      }
    }
  }

  async stop(): Promise<void> {
    return;
  }
}
