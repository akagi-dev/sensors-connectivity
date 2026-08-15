import { describe, expect, it } from 'vitest';
import { loadHeartbeatTrackerConfig } from '../src/config.js';

describe('heartbeat tracker config', () => {
  it('uses 30000ms as default online window', () => {
    const config = loadHeartbeatTrackerConfig({});

    expect(config.onlineWindowMs).toBe(30000);
    expect(config.retentionWindowMs).toBe(300000); // 10x online window
    expect(config.redisUrl).toBe('redis://localhost:6379');
    expect(config.redisKeyPrefix).toBe('heartbeat-tracker:v1');
  });

  it('respects HEARTBEAT_TRACKER_ONLINE_WINDOW_MS override', () => {
    const config = loadHeartbeatTrackerConfig({
      HEARTBEAT_TRACKER_ONLINE_WINDOW_MS: '45000',
    });

    expect(config.onlineWindowMs).toBe(45000);
    expect(config.retentionWindowMs).toBe(450000); // 10x online window
  });

  it('respects HEARTBEAT_TRACKER_RETENTION_WINDOW_MS override', () => {
    const config = loadHeartbeatTrackerConfig({
      HEARTBEAT_TRACKER_ONLINE_WINDOW_MS: '30000',
      HEARTBEAT_TRACKER_RETENTION_WINDOW_MS: '600000',
    });

    expect(config.onlineWindowMs).toBe(30000);
    expect(config.retentionWindowMs).toBe(600000);
  });

  it('respects HEARTBEAT_TRACKER_REDIS_PREFIX override', () => {
    const config = loadHeartbeatTrackerConfig({
      REDIS_URL: 'redis://redis.internal:6379',
      HEARTBEAT_TRACKER_REDIS_PREFIX: 'heartbeat-tracker:prod',
    });

    expect(config.redisUrl).toBe('redis://redis.internal:6379');
    expect(config.redisKeyPrefix).toBe('heartbeat-tracker:prod');
  });
});
