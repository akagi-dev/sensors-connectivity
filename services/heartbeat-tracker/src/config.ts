export interface HeartbeatTrackerConfig {
  kafkaBrokers: string[];
  consumerGroupId: string;
  source: string;
  healthPort: number;
  onlineWindowMs: number;
  retentionWindowMs: number;
  redisUrl: string;
  redisKeyPrefix: string;
}

function parsePositiveInt(value: string | undefined, fallback: number): number {
  const parsed = Number.parseInt(value ?? '', 10);
  return Number.isFinite(parsed) && parsed >= 0 ? parsed : fallback;
}

export function loadHeartbeatTrackerConfig(
  env: NodeJS.ProcessEnv = process.env
): HeartbeatTrackerConfig {
  const onlineWindowMs = parsePositiveInt(
    env.HEARTBEAT_TRACKER_ONLINE_WINDOW_MS,
    30000
  );
  const retentionWindowMs = parsePositiveInt(
    env.HEARTBEAT_TRACKER_RETENTION_WINDOW_MS,
    onlineWindowMs * 10
  );
  return {
    kafkaBrokers: (env.KAFKA_BROKERS ?? 'localhost:9092')
      .split(',')
      .map((broker) => broker.trim())
      .filter((broker) => broker.length > 0),
    consumerGroupId: env.HEARTBEAT_TRACKER_GROUP_ID ?? 'heartbeat-tracker-v1',
    source: env.HEARTBEAT_TRACKER_SOURCE ?? 'heartbeat-tracker',
    healthPort: parsePositiveInt(env.HEARTBEAT_TRACKER_HEALTH_PORT, 3030),
    onlineWindowMs,
    retentionWindowMs,
    redisUrl: env.REDIS_URL ?? 'redis://localhost:6379',
    redisKeyPrefix:
      env.HEARTBEAT_TRACKER_REDIS_PREFIX ?? 'heartbeat-tracker:v1',
  };
}
