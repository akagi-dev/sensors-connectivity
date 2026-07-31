export interface BlockchainAnchorConfig {
  substrateWsUrl: string;
  target: string;
}

export function loadBlockchainAnchorConfig(env: NodeJS.ProcessEnv = process.env): BlockchainAnchorConfig {
  return {
    substrateWsUrl: env.SUBSTRATE_WS_URL ?? 'ws://localhost:9944',
    target: env.BLOCKCHAIN_TARGET ?? 'robonomics-dev'
  };
}
