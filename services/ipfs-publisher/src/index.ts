import { fileURLToPath } from 'node:url';
import { logError, logInfo } from './logger.js';
import {
  createIpfsPublisherService,
  startIpfsPublisher,
  type IpfsPublisherDependencies,
  type IpfsPublisherService
} from './service.js';

export {
  decodeAuthorizedKafkaEnvelope,
  decodeAuthorizedKafkaMessage
} from './authorized-message.js';
export {
  processAuthorizedPayload,
  processSealedAuthorizedBatch,
  type AuthorizedBatchProcessingStatus,
  type AuthorizedPayloadProcessingDependencies
} from './batch-processor.js';
export {
  createIpfsPublisherService,
  startIpfsPublisher,
  type IpfsPublisherDependencies,
  type IpfsPublisherService
};

const isDirectRun = process.argv[1] === fileURLToPath(import.meta.url);
if (isDirectRun) {
  startIpfsPublisher()
    .then((service) => {
      const shutdown = async (signal: NodeJS.Signals): Promise<void> => {
        logInfo('shutdown signal received', { signal });
        await service.stop();
      };
      process.once('SIGINT', () => void shutdown('SIGINT'));
      process.once('SIGTERM', () => void shutdown('SIGTERM'));
    })
    .catch((error: unknown) => {
      logError('failed to start', error);
      process.exitCode = 1;
    });
}
