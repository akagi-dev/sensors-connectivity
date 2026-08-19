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
export interface IpfsPublisherConfig {
  kafkaBrokers: string[];
  consumerGroupId: string;
  source: string;
  healthPort: number;
  ipfsApiUrl: string;
  batchSize: number;
  batchTimeoutMs: number;
  enableCompression: boolean;
}

function parsePositiveInt(value: string | undefined, fallback: number): number {
  const parsed = Number.parseInt(value ?? '', 10);
  return Number.isFinite(parsed) && parsed > 0 ? parsed : fallback;
}

export function loadIpfsPublisherConfig(
  env: NodeJS.ProcessEnv = process.env
): IpfsPublisherConfig {
  return {
    kafkaBrokers: (env.KAFKA_BROKERS ?? 'localhost:9092')
      .split(',')
      .map((broker) => broker.trim())
      .filter((broker) => broker.length > 0),
    consumerGroupId: env.IPFS_PUBLISHER_GROUP_ID ?? 'ipfs-publisher-v1',
    source: env.IPFS_PUBLISHER_SOURCE ?? 'ipfs-publisher',
    healthPort: parsePositiveInt(env.IPFS_PUBLISHER_HEALTH_PORT, 3030),
    ipfsApiUrl: env.IPFS_API_URL ?? 'http://localhost:5001',
    batchSize: parsePositiveInt(env.IPFS_PUBLISHER_BATCH_SIZE, 100),
    batchTimeoutMs: parsePositiveInt(
      env.IPFS_PUBLISHER_BATCH_TIMEOUT_MS,
      30000
    ),
    enableCompression: env.IPFS_PUBLISHER_ENABLE_COMPRESSION !== 'false',
  };
}
