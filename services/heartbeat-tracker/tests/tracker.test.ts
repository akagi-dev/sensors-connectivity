import { TELEMETRY_TOPICS } from '@scp/contracts';
import { describe, expect, it } from 'vitest';
import { createHeartbeatTrackerState, handleTelemetryMessage } from '../src/index.js';

function createClock(start = Date.parse('2026-01-01T00:00:00Z')) {
  let current = start;
  return {
    now: () => current,
    set: (next: number) => {
      current = next;
    },
    advance: (ms: number) => {
      current += ms;
    }
  };
}

function authorizedEnvelope(sensorId: string) {
  return JSON.stringify({
    event_id: `evt-${sensorId}`,
    event_type: TELEMETRY_TOPICS.AUTHORIZED,
    event_version: 'v1',
    occurred_at: '2026-01-01T00:00:00Z',
    source: 'authorizer',
    payload: {
      sensor_id: sensorId,
      timestamp: '2026-01-01T00:00:00Z',
      nonce: `nonce-${sensorId}`,
      measurements: { temp: 21 },
      signature: '0xabc'
    }
  });
}

class FakeRedis {
  private readonly hashes = new Map<string, Record<string, string>>();
  private readonly sets = new Map<string, Set<string>>();

  async hgetall(key: string): Promise<Record<string, string>> {
    return { ...(this.hashes.get(key) ?? {}) };
  }

  async hset(key: string, map: Record<string, string>): Promise<number> {
    const current = this.hashes.get(key) ?? {};
    this.hashes.set(key, { ...current, ...map });
    return Object.keys(map).length;
  }

  async sadd(key: string, member: string): Promise<number> {
    const set = this.sets.get(key) ?? new Set<string>();
    const before = set.size;
    set.add(member);
    this.sets.set(key, set);
    return set.size > before ? 1 : 0;
  }

  async smembers(key: string): Promise<string[]> {
    return [...(this.sets.get(key) ?? new Set<string>())];
  }

  async srem(key: string, member: string): Promise<number> {
    const set = this.sets.get(key);
    if (!set) return 0;
    const deleted = set.delete(member) ? 1 : 0;
    if (set.size === 0) {
      this.sets.delete(key);
    }
    return deleted;
  }

  async del(key: string): Promise<number> {
    const existed = this.hashes.has(key) ? 1 : 0;
    this.hashes.delete(key);
    return existed;
  }
}

describe('heartbeat tracker state', () => {
  it('counts a sensor online right after an authorized message', async () => {
    const clock = createClock();
    const tracker = createHeartbeatTrackerState(new FakeRedis(), 'heartbeat-tracker:test', 30000, 300000, clock.now);
    const consumed = { value: 0 };

    await handleTelemetryMessage(authorizedEnvelope('sensor-1'), tracker, consumed);

    const metrics = await tracker.createMetrics(consumed.value);
    expect(metrics.sensors_online).toBe(1);
    expect(metrics.sensors_total_tracked).toBe(1);
    expect(consumed.value).toBe(1);
  });

  it('drops sensor from online count once time passes the window', async () => {
    const clock = createClock();
    const tracker = createHeartbeatTrackerState(new FakeRedis(), 'heartbeat-tracker:test', 30000, 300000, clock.now);
    const consumed = { value: 0 };

    await handleTelemetryMessage(authorizedEnvelope('sensor-1'), tracker, consumed);
    clock.advance(30001);

    const metrics = await tracker.createMetrics(consumed.value);
    expect(metrics.sensors_online).toBe(0);
    expect(metrics.sensors_total_tracked).toBe(1);
  });

  it('recomputes online count correctly across multiple sensors', async () => {
    const base = Date.parse('2026-01-01T00:00:00Z');
    const clock = createClock(base);
    const tracker = createHeartbeatTrackerState(new FakeRedis(), 'heartbeat-tracker:test', 30000, 300000, clock.now);
    const consumed = { value: 0 };

    await handleTelemetryMessage(authorizedEnvelope('sensor-a'), tracker, consumed);
    clock.set(base + 10000);
    await handleTelemetryMessage(authorizedEnvelope('sensor-b'), tracker, consumed);
    clock.set(base + 39000);
    await handleTelemetryMessage(authorizedEnvelope('sensor-c'), tracker, consumed);

    const metrics = await tracker.createMetrics(consumed.value);
    expect(metrics.sensors_total_tracked).toBe(3);
    expect(metrics.sensors_online).toBe(2);
    expect(metrics.sensor_uptime_seconds).toMatchObject({
      'sensor-b': 29,
      'sensor-c': 0
    });
    expect(metrics.sensor_uptime_seconds['sensor-a']).toBeUndefined();
  });

  it('keeps onlineSince stable within window and resets after large gap', async () => {
    const base = Date.parse('2026-01-01T00:00:00Z');
    const clock = createClock(base);
    const tracker = createHeartbeatTrackerState(new FakeRedis(), 'heartbeat-tracker:test', 30000, 300000, clock.now);
    const consumed = { value: 0 };

    await handleTelemetryMessage(authorizedEnvelope('sensor-1'), tracker, consumed);
    clock.advance(10000);
    await handleTelemetryMessage(authorizedEnvelope('sensor-1'), tracker, consumed);
    clock.advance(5000);

    let metrics = await tracker.createMetrics(consumed.value);
    expect(metrics.sensor_uptime_seconds['sensor-1']).toBe(15);

    clock.advance(35000);
    await handleTelemetryMessage(authorizedEnvelope('sensor-1'), tracker, consumed);

    metrics = await tracker.createMetrics(consumed.value);
    expect(metrics.sensor_uptime_seconds['sensor-1']).toBe(0);
  });

  it('computes uptime aggregates and returns avg 0 when none are online', async () => {
    const base = Date.parse('2026-01-01T00:00:00Z');
    const clock = createClock(base);
    const tracker = createHeartbeatTrackerState(new FakeRedis(), 'heartbeat-tracker:test', 30000, 300000, clock.now);
    const consumed = { value: 0 };

    await handleTelemetryMessage(authorizedEnvelope('sensor-a'), tracker, consumed);
    clock.set(base + 10000);
    await handleTelemetryMessage(authorizedEnvelope('sensor-b'), tracker, consumed);
    clock.set(base + 25000);

    let metrics = await tracker.createMetrics(consumed.value);
    expect(metrics.max_uptime_seconds).toBe(25);
    expect(metrics.avg_uptime_seconds).toBe(20);

    clock.set(base + 80001);
    metrics = await tracker.createMetrics(consumed.value);
    expect(metrics.sensors_online).toBe(0);
    expect(metrics.max_uptime_seconds).toBe(0);
    expect(metrics.avg_uptime_seconds).toBe(0);
  });

  it('ignores invalid and non-authorized envelopes', async () => {
    const clock = createClock();
    const tracker = createHeartbeatTrackerState(new FakeRedis(), 'heartbeat-tracker:test', 30000, 300000, clock.now);
    const consumed = { value: 0 };

    await handleTelemetryMessage('not-json', tracker, consumed);
    await handleTelemetryMessage(JSON.stringify({ event_type: TELEMETRY_TOPICS.AUTHORIZED }), tracker, consumed);
    await handleTelemetryMessage(
      JSON.stringify({
        event_id: 'evt-rejected',
        event_type: TELEMETRY_TOPICS.REJECTED,
        event_version: 'v1',
        occurred_at: '2026-01-01T00:00:00Z',
        source: 'authorizer',
        payload: {
          reason_code: 'forbidden'
        }
      }),
      tracker,
      consumed
    );

    const metrics = await tracker.createMetrics(consumed.value);
    expect(consumed.value).toBe(0);
    expect(metrics.sensors_total_tracked).toBe(0);
    expect(metrics.sensors_online).toBe(0);
  });

  it('prunes sensors not seen within retention window', async () => {
    const base = Date.parse('2026-01-01T00:00:00Z');
    const clock = createClock(base);
    const onlineWindowMs = 30000;
    const retentionWindowMs = 300000; // 5 minutes
    const tracker = createHeartbeatTrackerState(new FakeRedis(), 'heartbeat-tracker:test', onlineWindowMs, retentionWindowMs, clock.now);
    const consumed = { value: 0 };

    // Add two sensors
    await handleTelemetryMessage(authorizedEnvelope('sensor-old'), tracker, consumed);
    clock.set(base + 10000);
    await handleTelemetryMessage(authorizedEnvelope('sensor-recent'), tracker, consumed);

    // Verify both are tracked
    let metrics = await tracker.createMetrics(consumed.value);
    expect(metrics.sensors_total_tracked).toBe(2);
    expect(metrics.sensors_online).toBe(2);

    // Move time beyond retention window for sensor-old, but keep sensor-recent within retention
    clock.set(base + retentionWindowMs + 1000);
    await handleTelemetryMessage(authorizedEnvelope('sensor-recent'), tracker, consumed);

    // Check metrics - sensor-old should be pruned
    metrics = await tracker.createMetrics(consumed.value);
    expect(metrics.sensors_total_tracked).toBe(1);
    expect(metrics.sensors_online).toBe(1);
    expect(metrics.sensors_uptime.length).toBe(1);
    expect(metrics.sensors_uptime.at(0)?.sensor_id).toBe('sensor-recent');
  });
});
