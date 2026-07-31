export interface PubsubBroadcasterConfig {
  pubsubTopic: string;
}

export function loadPubsubBroadcasterConfig(env: NodeJS.ProcessEnv = process.env): PubsubBroadcasterConfig {
  return {
    pubsubTopic: env.PUBSUB_TOPIC ?? 'telemetry/authorized/v1'
  };
}
