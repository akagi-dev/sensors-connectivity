export const TELEMETRY_TOPICS = Object.freeze({
  AUTHORIZED: 'telemetry.authorized.v1',
  REJECTED: 'telemetry.rejected.v1',
  IPFS_PUBLISHED: 'ipfs.published.v1',
  DLQ: 'telemetry.dlq.v1',
} as const);

export type TelemetryTopic =
  (typeof TELEMETRY_TOPICS)[keyof typeof TELEMETRY_TOPICS];
