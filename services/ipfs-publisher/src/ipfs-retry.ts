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
export interface IpfsRetryOptions {
  maxAttempts: number;
  backoffMs: number;
  onRetry?: (error: unknown, attempt: number) => void | Promise<void>;
  sleep?: (milliseconds: number) => Promise<void>;
}

const transientErrorCodes = new Set([
  'ECONNABORTED',
  'ECONNREFUSED',
  'ECONNRESET',
  'EHOSTUNREACH',
  'ENETDOWN',
  'ENETUNREACH',
  'ENOTFOUND',
  'EPIPE',
  'ETIMEDOUT',
  'UND_ERR_CONNECT_TIMEOUT',
  'UND_ERR_HEADERS_TIMEOUT',
  'UND_ERR_SOCKET',
]);

/** Determines whether a Kubo error can be retried without changing the input. */
export function isTransientIpfsError(error: unknown): boolean {
  for (const candidate of errorChain(error)) {
    if ('terminal' in candidate && candidate.terminal === true) {
      return false;
    }
    if ('retriable' in candidate && typeof candidate.retriable === 'boolean') {
      return candidate.retriable;
    }

    const status = numericProperty(candidate, 'statusCode', 'status');
    if (
      status === 408 ||
      status === 425 ||
      status === 429 ||
      (status !== undefined && status >= 500 && status <= 599)
    ) {
      return true;
    }
    if (
      typeof candidate.code === 'string' &&
      transientErrorCodes.has(candidate.code.toUpperCase())
    ) {
      return true;
    }

    const message =
      candidate instanceof Error
        ? candidate.message.toLowerCase()
        : typeof candidate.message === 'string'
          ? candidate.message.toLowerCase()
          : '';
    if (
      message.includes('fetch failed') ||
      message.includes('network') ||
      message.includes('socket') ||
      message.includes('temporar') ||
      message.includes('timeout') ||
      message.includes('timed out') ||
      message.includes('unavailable') ||
      message.includes('did not confirm pin')
    ) {
      return true;
    }
  }

  return false;
}

/** Retries an IPFS operation only for transient errors and within the configured limit. */
export async function runWithBoundedIpfsRetry<T>(
  operation: () => Promise<T>,
  options: IpfsRetryOptions
): Promise<T> {
  if (!Number.isInteger(options.maxAttempts) || options.maxAttempts < 1) {
    throw new Error('IPFS retry maxAttempts must be a positive integer');
  }
  if (!Number.isInteger(options.backoffMs) || options.backoffMs < 0) {
    throw new Error('IPFS retry backoffMs must be a non-negative integer');
  }

  for (let attempt = 1; attempt <= options.maxAttempts; attempt += 1) {
    try {
      return await operation();
    } catch (error) {
      if (attempt >= options.maxAttempts || !isTransientIpfsError(error)) {
        throw error;
      }

      await options.onRetry?.(error, attempt);
      await (options.sleep ?? sleep)(options.backoffMs);
    }
  }

  throw new Error('IPFS retry loop ended without a result');
}

/** Returns an error object followed by its accessible cause chain. */
function errorChain(error: unknown): Record<string, unknown>[] {
  const chain: Record<string, unknown>[] = [];
  const visited = new Set<unknown>();
  let current = error;

  while (
    current !== null &&
    typeof current === 'object' &&
    !visited.has(current)
  ) {
    visited.add(current);
    chain.push(current as Record<string, unknown>);
    current = (current as { cause?: unknown }).cause;
  }

  return chain;
}

/** Reads the first finite numeric status code from an error object. */
function numericProperty(
  value: Record<string, unknown>,
  ...names: string[]
): number | undefined {
  for (const name of names) {
    const candidate = value[name];
    if (typeof candidate === 'number' && Number.isFinite(candidate)) {
      return candidate;
    }
  }
  return undefined;
}

/** Waits for the configured delay between attempts. */
async function sleep(milliseconds: number): Promise<void> {
  await new Promise<void>((resolve) => {
    setTimeout(resolve, milliseconds);
  });
}
