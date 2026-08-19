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
import { once } from 'node:events';
import type { AddressInfo } from 'node:net';
import { afterEach, describe, expect, it } from 'vitest';
import {
  createIpfsPublisherMetrics,
  startIpfsPublisherHealthServer,
} from '../src/metrics.js';

let closeServer: (() => Promise<void>) | undefined;

afterEach(async () => {
  await closeServer?.();
  closeServer = undefined;
});

describe('ipfs-publisher health and metrics server', () => {
  it('serves health and Prometheus metrics and rejects unknown paths', async () => {
    const metrics = createIpfsPublisherMetrics();
    metrics.batchCount = 3;
    metrics.pinCount = 2;
    metrics.pinLatencyMs = 17;
    metrics.pinLatencyTotalMs = 29;
    metrics.retryCount = 1;
    metrics.dlqCount = 1;
    const server = startIpfsPublisherHealthServer(metrics, 0);
    closeServer = async () => {
      await new Promise<void>((resolve, reject) => {
        server.close((error) => (error ? reject(error) : resolve()));
      });
    };
    await once(server, 'listening');
    const address = server.address() as AddressInfo;
    const baseUrl = `http://127.0.0.1:${address.port}`;

    const health = await fetch(`${baseUrl}/health`);
    expect(health.status).toBe(200);
    await expect(health.json()).resolves.toEqual({ status: 'ok' });

    const response = await fetch(`${baseUrl}/metrics`);
    expect(response.status).toBe(200);
    expect(response.headers.get('content-type')).toContain('text/plain');
    const body = await response.text();
    expect(body).toContain('ipfs_publisher_batches_total 3');
    expect(body).toContain('ipfs_publisher_pin_latency_ms 17');
    expect(body).toContain('ipfs_publisher_pin_latency_ms_sum 29');
    expect(body).toContain('ipfs_publisher_pin_latency_ms_count 2');
    expect(body).toContain('ipfs_publisher_retries_total 1');
    expect(body).toContain('ipfs_publisher_dlq_total 1');

    const missing = await fetch(`${baseUrl}/missing`);
    expect(missing.status).toBe(404);
  });
});
