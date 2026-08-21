/**
 * Copyright 2026 Robonomics Network
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
import pino from 'pino';

const logger = pino({
  level: process.env.LOG_LEVEL ?? 'info',
  name: 'blockchain-anchor',
});

export function logInfo(message: string, metadata?: Record<string, unknown>) {
  logger.info(metadata ?? {}, message);
}

export function logWarn(message: string, metadata?: Record<string, unknown>) {
  logger.warn(metadata ?? {}, message);
}

export function logError(
  message: string,
  error: unknown,
  metadata?: Record<string, unknown>
) {
  const errorDetails = {
    ...(metadata ?? {}),
    error: error instanceof Error ? error.message : String(error),
    stack: error instanceof Error ? error.stack : undefined,
  };
  logger.error(errorDetails, message);
}

export function logDebug(message: string, metadata?: Record<string, unknown>) {
  logger.debug(metadata ?? {}, message);
}
