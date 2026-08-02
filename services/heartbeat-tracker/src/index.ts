import {
  TELEMETRY_TOPICS,
  validateEnvelopeWithKnownPayload,
  type Envelope,
  type TelemetryAuthorizedPayload
} from '@scp/contracts';
import Redis from 'ioredis';
import { Kafka, type Consumer } from 'kafkajs';
import { createServer, type Server } from 'node:http';
import { fileURLToPath } from 'node:url';
import pino from 'pino';
import { loadHeartbeatTrackerConfig, type HeartbeatTrackerConfig } from './config.js';

interface HeartbeatTrackerMetrics {
  sensors_online: number;
  sensors_total_tracked: number;
  online_window_ms: number;
  consumed: number;
  sensor_uptime_seconds: Record<string, number>;
  sensors_uptime: Array<{
    sensor_id: string;
    online: boolean;
    first_seen: string;
    last_seen: string;
    uptime_seconds: number;
    seconds_since_last_seen: number;
  }>;
  max_uptime_seconds: number;
  avg_uptime_seconds: number;
}

export interface HeartbeatTrackerService {
  start(): Promise<void>;
  stop(): Promise<void>;
  getMetrics(): Promise<Readonly<HeartbeatTrackerMetrics>>;
}

export interface HeartbeatTrackerState {
  recordAuthorizedSensor(sensorId: string): Promise<void>;
  createMetrics(consumed: number): Promise<HeartbeatTrackerMetrics>;
}

interface HeartbeatTrackerDeps {
  createConsumer?: () => Consumer;
  createHealthServer?: (getMetrics: () => Promise<HeartbeatTrackerMetrics>, port: number) => Server;
  redis?: RedisLike;
  now?: () => number;
}

interface RedisLike {
  hgetall(key: string): Promise<Record<string, string>>;
  hset(key: string, map: Record<string, string>): Promise<number>;
  sadd(key: string, member: string): Promise<number>;
  smembers(key: string): Promise<string[]>;
  srem(key: string, member: string): Promise<number>;
  del(key: string): Promise<number>;
  quit?(): Promise<'OK'>;
  disconnect?(): void;
}

const logger = pino({
  name: 'heartbeat-tracker',
  level: process.env.HEARTBEAT_TRACKER_LOG_LEVEL ?? process.env.LOG_LEVEL ?? 'info'
});

function logInfo(message: string, context?: Record<string, unknown>): void {
  logger.info(context ?? {}, message);
}

function logDebug(message: string, context?: Record<string, unknown>): void {
  logger.debug(context ?? {}, message);
}

function logWarn(message: string, context?: Record<string, unknown>): void {
  logger.warn(context ?? {}, message);
}

function logError(message: string, error: unknown, context?: Record<string, unknown>): void {
  logger.error(
    {
      ...(context ?? {}),
      error: error instanceof Error ? error.message : String(error)
    },
    message
  );
}

type RedisConstructor = new (url: string) => RedisLike;

function createHeartbeatTrackerKeyspace(prefix: string) {
  return {
    sensors: `${prefix}:sensors`,
    sensor: (sensorId: string) => `${prefix}:sensor:${sensorId}`
  };
}

export function createHeartbeatTrackerState(
  redis: RedisLike,
  redisKeyPrefix: string,
  onlineWindowMs: number,
  retentionWindowMs: number,
  now: () => number = Date.now
): HeartbeatTrackerState {
  const keyspace = createHeartbeatTrackerKeyspace(redisKeyPrefix);

  return {
    async recordAuthorizedSensor(sensorId: string): Promise<void> {
      const observedAt = now();
      const key = keyspace.sensor(sensorId);
      const existing = await redis.hgetall(key);
      const existingLastSeen = Number.parseInt(existing.lastSeen ?? '', 10);
      const existingOnlineSince = Number.parseInt(existing.onlineSince ?? '', 10);
      const existingFirstSeen = Number.parseInt(existing.firstSeen ?? '', 10);
      const hasExisting = Number.isFinite(existingLastSeen);

      let onlineSince = Number.isFinite(existingOnlineSince) ? existingOnlineSince : observedAt;
      const isNewSensor = !hasExisting;
      const gapMs = hasExisting ? observedAt - existingLastSeen : 0;
      
      if (hasExisting && observedAt - existingLastSeen > onlineWindowMs) {
        onlineSince = observedAt;
        logDebug('sensor uptime streak reset after offline gap', {
          sensor_id: sensorId,
          gap_ms: gapMs,
          online_window_ms: onlineWindowMs
        });
      }

      await redis.hset(key, {
        firstSeen: String(Number.isFinite(existingFirstSeen) ? existingFirstSeen : observedAt),
        lastSeen: String(observedAt),
        onlineSince: String(onlineSince)
      });
      await redis.sadd(keyspace.sensors, sensorId);

      if (isNewSensor) {
        logInfo('new sensor tracked', { sensor_id: sensorId });
      } else {
        logDebug('sensor heartbeat recorded', {
          sensor_id: sensorId,
          gap_ms: gapMs
        });
      }
    },
    async createMetrics(consumed: number): Promise<HeartbeatTrackerMetrics> {
      const currentTime = now();
      const uptimeMap: Record<string, number> = {};
      const details: HeartbeatTrackerMetrics['sensors_uptime'] = [];
      const onlineUptimes: number[] = [];
      const sensorIds = await redis.smembers(keyspace.sensors);

      logDebug('computing metrics', {
        total_sensor_ids: sensorIds.length,
        retention_window_ms: retentionWindowMs,
        online_window_ms: onlineWindowMs
      });

      const heartbeats = await Promise.all(
        sensorIds.map((sensorId) => redis.hgetall(keyspace.sensor(sensorId)))
      );

      const staleSensorIds: string[] = [];

      for (let i = 0; i < sensorIds.length; i++) {
        const sensorId = sensorIds[i];
        const heartbeat = heartbeats[i];
        if (sensorId === undefined || heartbeat === undefined) {
          continue;
        }
        const firstSeen = Number.parseInt(heartbeat.firstSeen ?? '', 10);
        const lastSeen = Number.parseInt(heartbeat.lastSeen ?? '', 10);
        const onlineSince = Number.parseInt(heartbeat.onlineSince ?? '', 10);
        if (!Number.isFinite(firstSeen) || !Number.isFinite(lastSeen) || !Number.isFinite(onlineSince)) {
          logDebug('skipping sensor with invalid heartbeat data', { sensor_id: sensorId });
          continue;
        }
        const secondsSinceLastSeen = Math.max(0, (currentTime - lastSeen) / 1000);
        const online = currentTime - lastSeen <= onlineWindowMs;
        const uptimeSeconds = online ? Math.max(0, (currentTime - onlineSince) / 1000) : 0;

        // Mark sensor as stale if not seen within retention window
        if (currentTime - lastSeen > retentionWindowMs) {
          staleSensorIds.push(sensorId);
          logDebug('sensor marked for pruning', {
            sensor_id: sensorId,
            seconds_since_last_seen: secondsSinceLastSeen,
            retention_window_seconds: retentionWindowMs / 1000
          });
          continue;
        }

        if (online) {
          uptimeMap[sensorId] = uptimeSeconds;
          onlineUptimes.push(uptimeSeconds);
        }

        details.push({
          sensor_id: sensorId,
          online,
          first_seen: new Date(firstSeen).toISOString(),
          last_seen: new Date(lastSeen).toISOString(),
          uptime_seconds: uptimeSeconds,
          seconds_since_last_seen: secondsSinceLastSeen
        });
      }

      // Prune stale sensors from Redis
      if (staleSensorIds.length > 0) {
        logInfo('pruning stale sensors from Redis', {
          stale_count: staleSensorIds.length,
          sensor_ids: staleSensorIds
        });
        await Promise.all(
          staleSensorIds.map(async (sensorId) => {
            await redis.srem(keyspace.sensors, sensorId);
            await redis.del(keyspace.sensor(sensorId));
          })
        );
      }

      const sensorsOnline = onlineUptimes.length;
      const maxUptimeSeconds = sensorsOnline > 0 ? Math.max(...onlineUptimes) : 0;
      const avgUptimeSeconds =
        sensorsOnline > 0
          ? onlineUptimes.reduce((total, current) => total + current, 0) / sensorsOnline
          : 0;

      logDebug('metrics computed', {
        sensors_online: sensorsOnline,
        sensors_tracked: sensorIds.length - staleSensorIds.length,
        sensors_pruned: staleSensorIds.length,
        max_uptime_seconds: maxUptimeSeconds,
        avg_uptime_seconds: avgUptimeSeconds
      });

      return {
        sensors_online: sensorsOnline,
        sensors_total_tracked: sensorIds.length - staleSensorIds.length,
        online_window_ms: onlineWindowMs,
        consumed,
        sensor_uptime_seconds: uptimeMap,
        sensors_uptime: details,
        max_uptime_seconds: maxUptimeSeconds,
        avg_uptime_seconds: avgUptimeSeconds
      };
    }
  };
}

export function handleTelemetryMessage(
  raw: string,
  tracker: HeartbeatTrackerState,
  consumed: { value: number }
): Promise<void> {
  const parsed = safeJsonParse(raw);
  if (!parsed.success) {
    logWarn('invalid JSON message ignored', { reason: parsed.error });
    return Promise.resolve();
  }

  const envelopeResult = validateEnvelopeWithKnownPayload(parsed.data);
  if (!envelopeResult.success) {
    logWarn('invalid envelope ignored', { reason: envelopeResult.error.message });
    return Promise.resolve();
  }

  if (envelopeResult.data.event_type !== TELEMETRY_TOPICS.AUTHORIZED) {
    logDebug('non-authorized envelope ignored', { eventType: envelopeResult.data.event_type });
    return Promise.resolve();
  }

  const envelope = envelopeResult.data as Envelope & {
    event_type: typeof TELEMETRY_TOPICS.AUTHORIZED;
    payload: TelemetryAuthorizedPayload;
  };

  return tracker.recordAuthorizedSensor(envelope.payload.sensor_id).then(() => {
    consumed.value += 1;
    logDebug('authorized event consumed', {
      sensor_id: envelope.payload.sensor_id,
      event_id: envelope.event_id,
      total_consumed: consumed.value
    });
  });
}

export function createHeartbeatTrackerService(
  config: HeartbeatTrackerConfig = loadHeartbeatTrackerConfig(),
  deps: HeartbeatTrackerDeps = {}
): HeartbeatTrackerService {
  const kafka = new Kafka({ clientId: 'heartbeat-tracker', brokers: config.kafkaBrokers });
  const consumer = deps.createConsumer?.() ?? kafka.consumer({ groupId: config.consumerGroupId });
  const RedisClient = Redis as unknown as RedisConstructor;
  const redis = deps.redis ?? new RedisClient(config.redisUrl);
  const createHealthServer = deps.createHealthServer ?? startHealthAndMetricsServer;
  const tracker = createHeartbeatTrackerState(
    redis,
    config.redisKeyPrefix,
    config.onlineWindowMs,
    config.retentionWindowMs,
    deps.now ?? Date.now
  );

  let started = false;
  let runPromise: Promise<void> | null = null;
  let healthServer: Server | null = null;
  const consumed = { value: 0 };

  const getMetrics = async (): Promise<HeartbeatTrackerMetrics> => tracker.createMetrics(consumed.value);

  return {
    async start(): Promise<void> {
      if (started) {
        logInfo('start skipped; service already started');
        return;
      }

      started = true;
      logInfo('starting service', {
        consumerGroupId: config.consumerGroupId,
        kafkaBrokers: config.kafkaBrokers,
        healthPort: config.healthPort,
        onlineWindowMs: config.onlineWindowMs,
        retentionWindowMs: config.retentionWindowMs,
        redisUrl: config.redisUrl,
        redisKeyPrefix: config.redisKeyPrefix,
        source: config.source
      });

      try {
        await consumer.connect();
        await consumer.subscribe({ topic: TELEMETRY_TOPICS.AUTHORIZED, fromBeginning: false });
        healthServer = createHealthServer(getMetrics, config.healthPort);

        runPromise = consumer.run({
          eachMessage: async ({ message, partition, topic }) => {
            await handleTelemetryMessage(message.value?.toString('utf8') ?? '', tracker, consumed);
            logDebug('kafka message processed', { topic, partition, offset: message.offset, consumed: consumed.value });
          }
        });
        logInfo('service started');
      } catch (error) {
        logError('service failed to start', error);
        started = false;
        runPromise = null;

        if (healthServer) {
          await new Promise<void>((resolve) => {
            healthServer?.close(() => {
              resolve();
            });
          });
          healthServer = null;
        }

        await consumer.disconnect();
        if (typeof redis.quit === 'function') {
          await redis.quit();
        } else if (typeof redis.disconnect === 'function') {
          redis.disconnect();
        }
        logInfo('startup rollback complete');
        throw error;
      }
    },
    async stop(): Promise<void> {
      if (!started) {
        logInfo('stop skipped; service not started');
        return;
      }

      started = false;
      logInfo('stopping service');
      await consumer.stop();
      await consumer.disconnect();
      if (typeof redis.quit === 'function') {
        await redis.quit();
      } else if (typeof redis.disconnect === 'function') {
        redis.disconnect();
      }
      if (healthServer) {
        await new Promise<void>((resolve, reject) => {
          healthServer?.close((error) => {
            if (error) {
              reject(error);
              return;
            }
            resolve();
          });
        });
        healthServer = null;
      }
      await runPromise?.catch(() => undefined);
      runPromise = null;
      logInfo('service stopped');
    },
    getMetrics(): Promise<Readonly<HeartbeatTrackerMetrics>> {
      return getMetrics();
    }
  };
}

export function startHealthAndMetricsServer(
  getMetrics: () => Promise<HeartbeatTrackerMetrics>,
  port: number
): Server {
  const server = createServer(async (request, response) => {
    if (request.url === '/health') {
      logDebug('health check requested');
      response.statusCode = 200;
      response.setHeader('content-type', 'application/json; charset=utf-8');
      response.end(JSON.stringify({ status: 'ok' }));
      return;
    }

    if (request.url === '/metrics') {
      try {
        logDebug('metrics endpoint requested');
        const metrics = await getMetrics();
        logInfo('metrics served', {
          sensors_online: metrics.sensors_online,
          sensors_total_tracked: metrics.sensors_total_tracked,
          consumed: metrics.consumed
        });
        response.statusCode = 200;
        response.setHeader('content-type', 'application/json; charset=utf-8');
        response.end(JSON.stringify(metrics));
      } catch (error) {
        response.statusCode = 500;
        response.setHeader('content-type', 'application/json; charset=utf-8');
        response.end(JSON.stringify({ error: 'failed to collect metrics' }));
        logError('failed to collect metrics', error);
      }
      return;
    }

    response.statusCode = 404;
    response.end('not found');
  });
  server.listen({ host: '0.0.0.0', port });
  logInfo('HTTP server listening', { port, host: '0.0.0.0' });
  return server;
}

function safeJsonParse(raw: string): { success: true; data: unknown } | { success: false; error: string } {
  try {
    return { success: true, data: JSON.parse(raw) };
  } catch (error) {
    return {
      success: false,
      error: error instanceof Error ? error.message : 'Invalid JSON payload'
    };
  }
}

export async function startHeartbeatTracker(): Promise<HeartbeatTrackerService> {
  const service = createHeartbeatTrackerService();
  await service.start();
  logInfo('service started (direct run)');
  return service;
}

const isDirectRun = process.argv[1] === fileURLToPath(import.meta.url);
if (isDirectRun) {
  startHeartbeatTracker().catch((error: unknown) => {
    logError('failed to start (direct run)', error);
    process.exitCode = 1;
  });
}
