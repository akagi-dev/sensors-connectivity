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
import { describe, expect, it } from 'vitest';
import { loadIpfsPublisherConfig } from '../src/config.js';

describe('ipfs-publisher configuration', () => {
  it('uses local defaults', () => {
    expect(loadIpfsPublisherConfig({})).toEqual({
      kafkaBrokers: ['localhost:9092'],
      consumerGroupId: 'ipfs-publisher-v1',
      batchMaxEvents: 100,
      batchMaxWaitMs: 1000,
      maxRetries: 3,
      retryBackoffMs: 250,
      ipfsApiUrl: 'http://localhost:5001',
      healthPort: 3040,
    });
  });

  it('parses broker and service overrides', () => {
    expect(
      loadIpfsPublisherConfig({
        KAFKA_BROKERS: ' kafka-1:9092, kafka-2:9092 ',
        IPFS_PUBLISHER_GROUP_ID: ' publisher-test ',
        IPFS_PUBLISHER_BATCH_MAX_EVENTS: '25',
        IPFS_PUBLISHER_BATCH_MAX_WAIT_MS: '500',
        IPFS_PUBLISHER_MAX_RETRIES: '4',
        IPFS_PUBLISHER_RETRY_BACKOFF_MS: '50',
        IPFS_API_URL: ' http://ipfs:5001 ',
        IPFS_PUBLISHER_HEALTH_PORT: '4040',
      })
    ).toMatchObject({
      kafkaBrokers: ['kafka-1:9092', 'kafka-2:9092'],
      consumerGroupId: 'publisher-test',
      ipfsApiUrl: 'http://ipfs:5001',
      batchMaxEvents: 25,
    });
  });

  it('rejects invalid numeric settings', () => {
    expect(() =>
      loadIpfsPublisherConfig({ IPFS_PUBLISHER_BATCH_MAX_EVENTS: '0' })
    ).toThrow('IPFS_PUBLISHER_BATCH_MAX_EVENTS');
    expect(() =>
      loadIpfsPublisherConfig({ IPFS_PUBLISHER_MAX_RETRIES: 'invalid' })
    ).toThrow('IPFS_PUBLISHER_MAX_RETRIES');
    expect(() =>
      loadIpfsPublisherConfig({ IPFS_PUBLISHER_RETRY_BACKOFF_MS: '-1' })
    ).toThrow('IPFS_PUBLISHER_RETRY_BACKOFF_MS');
  });

  it('rejects empty broker and consumer group settings', () => {
    expect(() => loadIpfsPublisherConfig({ KAFKA_BROKERS: ' , ' })).toThrow(
      'KAFKA_BROKERS'
    );
    expect(() =>
      loadIpfsPublisherConfig({ IPFS_PUBLISHER_GROUP_ID: '   ' })
    ).toThrow('IPFS_PUBLISHER_GROUP_ID');
  });
});
