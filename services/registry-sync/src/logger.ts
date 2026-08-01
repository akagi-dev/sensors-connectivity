import pino from 'pino';

const logger = pino({
  name: 'registry-sync',
  level: process.env.REGISTRY_SYNC_LOG_LEVEL ?? process.env.LOG_LEVEL ?? 'info'
});

export function logInfo(message: string, context?: Record<string, unknown>): void {
  logger.info(context ?? {}, message);
}

export function logWarn(message: string, context?: Record<string, unknown>): void {
  logger.warn(context ?? {}, message);
}

export function logError(message: string, error: unknown, context?: Record<string, unknown>): void {
  logger.error(
    {
      ...(context ?? {}),
      error: error instanceof Error ? error.message : String(error)
    },
    message
  );
}
