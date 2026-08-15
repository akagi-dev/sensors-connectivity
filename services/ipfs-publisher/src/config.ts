export interface IpfsPublisherConfig {
  ipfsApiUrl: string;
}

export function loadIpfsPublisherConfig(
  env: NodeJS.ProcessEnv = process.env
): IpfsPublisherConfig {
  return {
    ipfsApiUrl: env.IPFS_API_URL ?? 'http://localhost:5001',
  };
}
