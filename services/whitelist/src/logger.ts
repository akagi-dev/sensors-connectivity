import pino from 'pino';

const logger = pino({
  name: 'whitelist',
  level: process.env.WHITELIST_LOG_LEVEL ?? process.env.LOG_LEVEL ?? 'info'
});

export function logInfo(message: string, context?: Record<string, unknown>): void {
  logger.info(context ?? {}, message);
}

export function logDebug(message: string, context?: Record<string, unknown>): void {
  logger.debug(context ?? {}, message);
}

export function logWarn(message: string, context?: Record<string, unknown>): void {
  logger.warn(context ?? {}, message);
}

export function logError(message: string, error: unknown, context?: Record<string, unknown>): void {
  const normalizedError = error instanceof Error
    ? { name: error.name, message: error.message, stack: error.stack }
    : { message: String(error) };
  logger.error(
    {
      ...(context ?? {}),
      error: normalizedError
    },
    message
  );
}
