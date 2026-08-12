import { describe, expect, it } from 'vitest';
import { loadIpfsPublisherConfig } from '../src/config.js';

describe('ipfs-publisher Kafka configuration', () => {
  it('uses local defaults', () => {
    expect(loadIpfsPublisherConfig({})).toEqual({
      kafkaBrokers: ['localhost:9092'],
      consumerGroupId: 'ipfs-publisher-v1',
      batchMaxEvents: 100,
      batchMaxWaitMs: 1000,
      maxRetries: 3,
      retryBackoffMs: 250,
      ipfsApiUrl: 'http://localhost:5001',
      healthPort: 3040
    });
  });

  it('parses broker list and consumer group overrides', () => {
    expect(
      loadIpfsPublisherConfig({
        KAFKA_BROKERS: ' kafka-1:9092, kafka-2:9092 ',
        IPFS_PUBLISHER_GROUP_ID: 'publisher-test',
        IPFS_PUBLISHER_BATCH_MAX_EVENTS: '25',
        IPFS_PUBLISHER_BATCH_MAX_WAIT_MS: '500',
        IPFS_PUBLISHER_MAX_RETRIES: '4',
        IPFS_PUBLISHER_RETRY_BACKOFF_MS: '50',
        IPFS_API_URL: 'http://ipfs:5001',
        IPFS_PUBLISHER_HEALTH_PORT: '4040'
      })
    ).toEqual({
      kafkaBrokers: ['kafka-1:9092', 'kafka-2:9092'],
      consumerGroupId: 'publisher-test',
      batchMaxEvents: 25,
      batchMaxWaitMs: 500,
      maxRetries: 4,
      retryBackoffMs: 50,
      ipfsApiUrl: 'http://ipfs:5001',
      healthPort: 4040
    });
  });

  it('rejects invalid batch limits', () => {
    expect(() =>
      loadIpfsPublisherConfig({ IPFS_PUBLISHER_BATCH_MAX_EVENTS: '0' })
    ).toThrow('IPFS_PUBLISHER_BATCH_MAX_EVENTS');
    expect(() =>
      loadIpfsPublisherConfig({ IPFS_PUBLISHER_BATCH_MAX_WAIT_MS: 'invalid' })
    ).toThrow('IPFS_PUBLISHER_BATCH_MAX_WAIT_MS');
    expect(() =>
      loadIpfsPublisherConfig({ IPFS_PUBLISHER_MAX_RETRIES: '0' })
    ).toThrow('IPFS_PUBLISHER_MAX_RETRIES');
    expect(() =>
      loadIpfsPublisherConfig({ IPFS_PUBLISHER_RETRY_BACKOFF_MS: '-1' })
    ).toThrow('IPFS_PUBLISHER_RETRY_BACKOFF_MS');
    expect(() =>
      loadIpfsPublisherConfig({ IPFS_PUBLISHER_HEALTH_PORT: '0' })
    ).toThrow('IPFS_PUBLISHER_HEALTH_PORT');
  });
});
