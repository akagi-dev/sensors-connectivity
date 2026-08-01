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

describe('heartbeat tracker state', () => {
  it('counts a sensor online right after an authorized message', () => {
    const clock = createClock();
    const tracker = createHeartbeatTrackerState(30000, clock.now);
    const consumed = { value: 0 };

    handleTelemetryMessage(authorizedEnvelope('sensor-1'), tracker, consumed);

    const metrics = tracker.createMetrics(consumed.value);
    expect(metrics.sensors_online).toBe(1);
    expect(metrics.sensors_total_tracked).toBe(1);
    expect(consumed.value).toBe(1);
  });

  it('drops sensor from online count once time passes the window', () => {
    const clock = createClock();
    const tracker = createHeartbeatTrackerState(30000, clock.now);
    const consumed = { value: 0 };

    handleTelemetryMessage(authorizedEnvelope('sensor-1'), tracker, consumed);
    clock.advance(30001);

    const metrics = tracker.createMetrics(consumed.value);
    expect(metrics.sensors_online).toBe(0);
    expect(metrics.sensors_total_tracked).toBe(1);
  });

  it('recomputes online count correctly across multiple sensors', () => {
    const base = Date.parse('2026-01-01T00:00:00Z');
    const clock = createClock(base);
    const tracker = createHeartbeatTrackerState(30000, clock.now);
    const consumed = { value: 0 };

    handleTelemetryMessage(authorizedEnvelope('sensor-a'), tracker, consumed);
    clock.set(base + 10000);
    handleTelemetryMessage(authorizedEnvelope('sensor-b'), tracker, consumed);
    clock.set(base + 39000);
    handleTelemetryMessage(authorizedEnvelope('sensor-c'), tracker, consumed);

    const metrics = tracker.createMetrics(consumed.value);
    expect(metrics.sensors_total_tracked).toBe(3);
    expect(metrics.sensors_online).toBe(2);
    expect(metrics.sensor_uptime_seconds).toMatchObject({
      'sensor-b': 29,
      'sensor-c': 0
    });
    expect(metrics.sensor_uptime_seconds['sensor-a']).toBeUndefined();
  });

  it('keeps onlineSince stable within window and resets after large gap', () => {
    const base = Date.parse('2026-01-01T00:00:00Z');
    const clock = createClock(base);
    const tracker = createHeartbeatTrackerState(30000, clock.now);
    const consumed = { value: 0 };

    handleTelemetryMessage(authorizedEnvelope('sensor-1'), tracker, consumed);
    clock.advance(10000);
    handleTelemetryMessage(authorizedEnvelope('sensor-1'), tracker, consumed);
    clock.advance(5000);

    let metrics = tracker.createMetrics(consumed.value);
    expect(metrics.sensor_uptime_seconds['sensor-1']).toBe(15);

    clock.advance(35000);
    handleTelemetryMessage(authorizedEnvelope('sensor-1'), tracker, consumed);

    metrics = tracker.createMetrics(consumed.value);
    expect(metrics.sensor_uptime_seconds['sensor-1']).toBe(0);
  });

  it('computes uptime aggregates and returns avg 0 when none are online', () => {
    const base = Date.parse('2026-01-01T00:00:00Z');
    const clock = createClock(base);
    const tracker = createHeartbeatTrackerState(30000, clock.now);
    const consumed = { value: 0 };

    handleTelemetryMessage(authorizedEnvelope('sensor-a'), tracker, consumed);
    clock.set(base + 10000);
    handleTelemetryMessage(authorizedEnvelope('sensor-b'), tracker, consumed);
    clock.set(base + 25000);

    let metrics = tracker.createMetrics(consumed.value);
    expect(metrics.max_uptime_seconds).toBe(25);
    expect(metrics.avg_uptime_seconds).toBe(20);

    clock.set(base + 80001);
    metrics = tracker.createMetrics(consumed.value);
    expect(metrics.sensors_online).toBe(0);
    expect(metrics.max_uptime_seconds).toBe(0);
    expect(metrics.avg_uptime_seconds).toBe(0);
  });

  it('ignores invalid and non-authorized envelopes', () => {
    const clock = createClock();
    const tracker = createHeartbeatTrackerState(30000, clock.now);
    const consumed = { value: 0 };

    handleTelemetryMessage('not-json', tracker, consumed);
    handleTelemetryMessage(JSON.stringify({ event_type: TELEMETRY_TOPICS.AUTHORIZED }), tracker, consumed);
    handleTelemetryMessage(
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

    const metrics = tracker.createMetrics(consumed.value);
    expect(consumed.value).toBe(0);
    expect(metrics.sensors_total_tracked).toBe(0);
    expect(metrics.sensors_online).toBe(0);
  });
});
