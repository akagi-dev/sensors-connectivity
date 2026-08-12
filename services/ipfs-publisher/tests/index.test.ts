import { describe, expect, it } from 'vitest';
import {
  createIpfsPublisherService,
  decodeAuthorizedKafkaEnvelope,
  decodeAuthorizedKafkaMessage,
  processAuthorizedPayload,
  processSealedAuthorizedBatch,
  startIpfsPublisher
} from '../src/index.js';

describe('ipfs-publisher public entrypoint', () => {
  it('preserves the public runtime exports after module decomposition', () => {
    const publicFunctions = [
      createIpfsPublisherService,
      decodeAuthorizedKafkaEnvelope,
      decodeAuthorizedKafkaMessage,
      processAuthorizedPayload,
      processSealedAuthorizedBatch,
      startIpfsPublisher
    ];
    for (const publicFunction of publicFunctions) {
      expect(publicFunction).toBeTypeOf('function');
    }
  });
});
