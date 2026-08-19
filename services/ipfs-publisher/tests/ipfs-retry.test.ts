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
import { describe, expect, it, vi } from 'vitest';
import {
  isTransientIpfsError,
  runWithBoundedIpfsRetry,
} from '../src/ipfs-retry.js';

describe('bounded IPFS retry', () => {
  it('retries transient failures up to the configured limit', async () => {
    const operation = vi
      .fn<() => Promise<string>>()
      .mockRejectedValueOnce(new Error('fetch failed'))
      .mockRejectedValueOnce(
        Object.assign(new Error('socket closed'), { code: 'ECONNRESET' })
      )
      .mockResolvedValue('bafy-result');
    const onRetry = vi.fn();
    const sleep = vi.fn(async () => undefined);

    await expect(
      runWithBoundedIpfsRetry(operation, {
        maxAttempts: 3,
        backoffMs: 25,
        onRetry,
        sleep,
      })
    ).resolves.toBe('bafy-result');

    expect(operation).toHaveBeenCalledTimes(3);
    expect(onRetry).toHaveBeenCalledTimes(2);
    expect(onRetry.mock.calls.map((call) => call[1])).toEqual([1, 2]);
    expect(sleep).toHaveBeenNthCalledWith(1, 25);
    expect(sleep).toHaveBeenNthCalledWith(2, 25);
  });

  it('stops immediately for a terminal failure', async () => {
    const operation = vi.fn(async () => {
      throw new Error('Kubo pinned unexpected CID');
    });

    await expect(
      runWithBoundedIpfsRetry(operation, {
        maxAttempts: 3,
        backoffMs: 0,
        sleep: async () => undefined,
      })
    ).rejects.toThrow('unexpected CID');
    expect(operation).toHaveBeenCalledTimes(1);
  });

  it('rethrows a transient failure after exhausting the attempt limit', async () => {
    const operation = vi.fn(async () => {
      throw new Error('network unavailable');
    });

    await expect(
      runWithBoundedIpfsRetry(operation, {
        maxAttempts: 2,
        backoffMs: 0,
        sleep: async () => undefined,
      })
    ).rejects.toThrow('network unavailable');
    expect(operation).toHaveBeenCalledTimes(2);
  });

  it('classifies Kubo network and HTTP errors without retrying validation errors', () => {
    expect(isTransientIpfsError(new Error('fetch failed'))).toBe(true);
    expect(isTransientIpfsError({ status: 503 })).toBe(true);
    expect(isTransientIpfsError({ retriable: true })).toBe(true);
    expect(
      isTransientIpfsError(
        Object.assign(new Error('outer failure'), {
          cause: { code: 'ETIMEDOUT' },
        })
      )
    ).toBe(true);
    expect(
      isTransientIpfsError(new Error('Kubo add returned an empty CID'))
    ).toBe(false);
  });
});
