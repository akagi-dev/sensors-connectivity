import {
  TELEMETRY_TOPICS,
  validateEnvelopeWithKnownPayload,
  type Envelope,
  type TelemetryAuthorizedPayload
} from '@scp/contracts';
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

interface SensorHeartbeat {
  firstSeen: number;
  lastSeen: number;
  onlineSince: number;
}

export interface HeartbeatTrackerService {
  start(): Promise<void>;
  stop(): Promise<void>;
  getMetrics(): Readonly<HeartbeatTrackerMetrics>;
}

export interface HeartbeatTrackerState {
  recordAuthorizedSensor(sensorId: string): void;
  createMetrics(consumed: number): HeartbeatTrackerMetrics;
}

interface HeartbeatTrackerDeps {
  createConsumer?: () => Consumer;
  createHealthServer?: (getMetrics: () => HeartbeatTrackerMetrics, port: number) => Server;
  now?: () => number;
}

const logger = pino({
  name: 'heartbeat-tracker',
  level: process.env.HEARTBEAT_TRACKER_LOG_LEVEL ?? process.env.LOG_LEVEL ?? 'info'
});

function logInfo(message: string, context?: Record<string, unknown>): void {
  logger.info(context ?? {}, message);
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

export function createHeartbeatTrackerState(
  onlineWindowMs: number,
  now: () => number = Date.now
): HeartbeatTrackerState {
  const sensors = new Map<string, SensorHeartbeat>();

  return {
    recordAuthorizedSensor(sensorId: string): void {
      const observedAt = now();
      const existing = sensors.get(sensorId);
      if (!existing) {
        sensors.set(sensorId, {
          firstSeen: observedAt,
          lastSeen: observedAt,
          onlineSince: observedAt
        });
        return;
      }

      if (observedAt - existing.lastSeen > onlineWindowMs) {
        existing.onlineSince = observedAt;
      }

      existing.lastSeen = observedAt;
    },
    createMetrics(consumed: number): HeartbeatTrackerMetrics {
      const currentTime = now();
      const uptimeMap: Record<string, number> = {};
      const details: HeartbeatTrackerMetrics['sensors_uptime'] = [];
      const onlineUptimes: number[] = [];

      for (const [sensorId, heartbeat] of sensors.entries()) {
        const secondsSinceLastSeen = Math.max(0, (currentTime - heartbeat.lastSeen) / 1000);
        const online = currentTime - heartbeat.lastSeen <= onlineWindowMs;
        const uptimeSeconds = online ? Math.max(0, (currentTime - heartbeat.onlineSince) / 1000) : 0;

        if (online) {
          uptimeMap[sensorId] = uptimeSeconds;
          onlineUptimes.push(uptimeSeconds);
        }

        details.push({
          sensor_id: sensorId,
          online,
          first_seen: new Date(heartbeat.firstSeen).toISOString(),
          last_seen: new Date(heartbeat.lastSeen).toISOString(),
          uptime_seconds: uptimeSeconds,
          seconds_since_last_seen: secondsSinceLastSeen
        });
      }

      const sensorsOnline = onlineUptimes.length;
      const maxUptimeSeconds = sensorsOnline > 0 ? Math.max(...onlineUptimes) : 0;
      const avgUptimeSeconds =
        sensorsOnline > 0
          ? onlineUptimes.reduce((total, current) => total + current, 0) / sensorsOnline
          : 0;

      return {
        sensors_online: sensorsOnline,
        sensors_total_tracked: sensors.size,
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
): void {
  const parsed = safeJsonParse(raw);
  if (!parsed.success) {
    logWarn('invalid JSON message ignored', { reason: parsed.error });
    return;
  }

  const envelopeResult = validateEnvelopeWithKnownPayload(parsed.data);
  if (!envelopeResult.success) {
    logWarn('invalid envelope ignored', { reason: envelopeResult.error.message });
    return;
  }

  if (envelopeResult.data.event_type !== TELEMETRY_TOPICS.AUTHORIZED) {
    logWarn('non-authorized envelope ignored', { eventType: envelopeResult.data.event_type });
    return;
  }

  const envelope = envelopeResult.data as Envelope & {
    event_type: typeof TELEMETRY_TOPICS.AUTHORIZED;
    payload: TelemetryAuthorizedPayload;
  };

  tracker.recordAuthorizedSensor(envelope.payload.sensor_id);
  consumed.value += 1;
}

export function createHeartbeatTrackerService(
  config: HeartbeatTrackerConfig = loadHeartbeatTrackerConfig(),
  deps: HeartbeatTrackerDeps = {}
): HeartbeatTrackerService {
  const kafka = new Kafka({ clientId: 'heartbeat-tracker', brokers: config.kafkaBrokers });
  const consumer = deps.createConsumer?.() ?? kafka.consumer({ groupId: config.consumerGroupId });
  const createHealthServer = deps.createHealthServer ?? startHealthAndMetricsServer;
  const tracker = createHeartbeatTrackerState(config.onlineWindowMs, deps.now ?? Date.now);

  let started = false;
  let runPromise: Promise<void> | null = null;
  let healthServer: Server | null = null;
  const consumed = { value: 0 };

  const getMetrics = (): HeartbeatTrackerMetrics => tracker.createMetrics(consumed.value);

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
        source: config.source
      });

      try {
        await consumer.connect();
        await consumer.subscribe({ topic: TELEMETRY_TOPICS.AUTHORIZED, fromBeginning: false });
        healthServer = createHealthServer(getMetrics, config.healthPort);

        runPromise = consumer.run({
          eachMessage: async ({ message, partition, topic }) => {
            handleTelemetryMessage(message.value?.toString('utf8') ?? '', tracker, consumed);
            logInfo('message observed', { topic, partition, offset: message.offset, consumed: consumed.value });
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
    getMetrics(): Readonly<HeartbeatTrackerMetrics> {
      return getMetrics();
    }
  };
}

export function startHealthAndMetricsServer(
  getMetrics: () => HeartbeatTrackerMetrics,
  port: number
): Server {
  const server = createServer((request, response) => {
    if (request.url === '/health') {
      response.statusCode = 200;
      response.setHeader('content-type', 'application/json; charset=utf-8');
      response.end(JSON.stringify({ status: 'ok' }));
      return;
    }

    if (request.url === '/metrics') {
      response.statusCode = 200;
      response.setHeader('content-type', 'application/json; charset=utf-8');
      response.end(JSON.stringify(getMetrics()));
      return;
    }

    response.statusCode = 404;
    response.end('not found');
  });
  server.listen({ host: '0.0.0.0', port });
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
