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
import type { Consumer } from '@platformatic/kafka';
import { TELEMETRY_TOPICS } from '@scp/core';
import type { Server } from 'node:http';
import { describe, expect, it, vi } from 'vitest';
import type { IpfsBatchClient } from '../src/batch-publisher.js';
import { createIpfsPublisherService } from '../src/service.js';
import type { IpfsPublisherConfig } from '../src/config.js';
import type { IpfsResultPublisher } from '../src/result-publisher.js';
import { authorizedEnvelopeBytes } from './helpers.js';

const config: IpfsPublisherConfig = {
  kafkaBrokers: ['localhost:9092'],
  consumerGroupId: 'ipfs-publisher-test',
  batchMaxEvents: 1,
  batchMaxWaitMs: 1000,
  maxRetries: 1,
  retryBackoffMs: 0,
  ipfsApiUrl: 'http://localhost:5001',
  healthPort: 3040,
};
const cid = 'bafybeideterministic';

function consumerDouble(calls: string[]) {
  const commit = vi.fn(async () => {
    calls.push('consumer.commit');
  });
  const stream = {
    async *[Symbol.asyncIterator]() {
      yield {
        topic: TELEMETRY_TOPICS.AUTHORIZED,
        partition: 0,
        offset: 7n,
        value: authorizedEnvelopeBytes(),
        commit,
      };
    },
    async close() {
      calls.push('stream.close');
    },
  };
  const consumer = {
    async consume(options: { topics: string[]; autocommit: boolean }) {
      calls.push(`consumer.consume:${options.topics[0]}:${options.autocommit}`);
      return stream;
    },
    async close() {
      calls.push('consumer.close');
    },
  };
  return { consumer: consumer as unknown as Consumer, commit };
}

function resultDouble(calls: string[]): IpfsResultPublisher {
  return {
    async connect() {
      calls.push('result.connect');
    },
    async disconnect() {
      calls.push('result.disconnect');
    },
    async publish() {
      calls.push('result.publish');
    },
    async publishDlq() {
      calls.push('result.publishDlq');
    },
  };
}

function healthDouble(calls: string[]): Server {
  return {
    close(callback: (error?: Error) => void) {
      calls.push('health.close');
      callback();
    },
  } as unknown as Server;
}

function workingIpfs(calls: string[]): IpfsBatchClient {
  return {
    async add() {
      calls.push('ipfs.add');
      return { cid: { toString: () => cid } };
    },
    pin: {
      async add() {
        calls.push('pin.add');
        return { toString: () => cid };
      },
      async *ls() {
        calls.push('pin.ls');
        yield { cid: { toString: () => cid } };
      },
    },
  };
}

describe('ipfs-publisher service lifecycle', () => {
  it('consumes protobuf, publishes, manually commits, and cleans up', async () => {
    const calls: string[] = [];
    const consumer = consumerDouble(calls);
    const service = createIpfsPublisherService(config, {
      ipfs: workingIpfs(calls),
      createConsumer: () => consumer.consumer,
      resultPublisher: resultDouble(calls),
      createHealthServer: () => healthDouble(calls),
    });
    await service.start();
    await new Promise<void>((resolve) => setImmediate(resolve));
    await service.stop();

    expect(calls).toContain(
      `consumer.consume:${TELEMETRY_TOPICS.AUTHORIZED}:false`
    );
    expect(calls).toEqual(
      expect.arrayContaining([
        'ipfs.add',
        'pin.add',
        'pin.ls',
        'result.publish',
        'consumer.commit',
        'stream.close',
        'health.close',
        'consumer.close',
        'result.disconnect',
      ])
    );
    expect(calls.indexOf('result.publish')).toBeLessThan(
      calls.indexOf('consumer.commit')
    );
    expect(consumer.commit).toHaveBeenCalledOnce();
  });

  it('treats acknowledged DLQ as a normal status and still disconnects', async () => {
    const calls: string[] = [];
    const consumer = consumerDouble(calls);
    const ipfs = workingIpfs(calls);
    (ipfs.add as ReturnType<typeof vi.fn>) = vi.fn(async () => {
      calls.push('ipfs.add');
      throw new Error('terminal IPFS failure');
    });
    const service = createIpfsPublisherService(config, {
      ipfs,
      createConsumer: () => consumer.consumer,
      resultPublisher: resultDouble(calls),
      createHealthServer: () => healthDouble(calls),
    });
    await service.start();
    await new Promise<void>((resolve) => setImmediate(resolve));
    await expect(service.stop()).resolves.toBeUndefined();

    expect(calls).toContain('result.publishDlq');
    expect(calls).toContain('consumer.close');
    expect(calls).toContain('result.disconnect');
    expect(consumer.commit).not.toHaveBeenCalled();
  });
});
