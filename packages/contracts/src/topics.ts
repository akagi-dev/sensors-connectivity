export const TELEMETRY_TOPICS: {
  readonly AUTHORIZED: string;
  readonly REJECTED: string;
  readonly PUBSUB_RESULT: string;
  readonly IPFS_RESULT: string;
  readonly BLOCKCHAIN_RESULT: string;
  readonly RETRY: string;
  readonly DLQ: string;
} = Object.freeze({
  AUTHORIZED: 'telemetry.authorized.v1',
  REJECTED: 'telemetry.rejected.v1',
  PUBSUB_RESULT: 'telemetry.pubsub.result.v1',
  IPFS_RESULT: 'telemetry.ipfs.result.v1',
  BLOCKCHAIN_RESULT: 'telemetry.blockchain.result.v1',
  RETRY: 'telemetry.retry.v1',
  DLQ: 'telemetry.dlq.v1',
} as const);

export type TelemetryTopic =
  (typeof TELEMETRY_TOPICS)[keyof typeof TELEMETRY_TOPICS];
