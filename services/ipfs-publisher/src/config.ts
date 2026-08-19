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
  batchMaxEvents: number;
  batchMaxWaitMs: number;
  maxRetries: number;
  retryBackoffMs: number;
  ipfsApiUrl: string;
  healthPort: number;
}

function parsePositiveInteger(
  value: string | undefined,
  fallback: number,
  name: string
): number {
  const parsed = value === undefined ? fallback : Number(value);
  if (!Number.isInteger(parsed) || parsed < 1) {
    throw new Error(`${name} must be a positive integer`);
  }
  return parsed;
}

function parseNonNegativeInteger(
  value: string | undefined,
  fallback: number,
  name: string
): number {
  const parsed = value === undefined ? fallback : Number(value);
  if (!Number.isInteger(parsed) || parsed < 0) {
    throw new Error(`${name} must be a non-negative integer`);
  }
  return parsed;
}

function parseNonEmptyString(
  value: string | undefined,
  fallback: string,
  name: string
): string {
  const parsed = (value ?? fallback).trim();
  if (parsed.length === 0) {
    throw new Error(`${name} must not be empty`);
  }
  return parsed;
}

export function loadIpfsPublisherConfig(
  env: NodeJS.ProcessEnv = process.env
): IpfsPublisherConfig {
  const kafkaBrokers = (env.KAFKA_BROKERS ?? 'localhost:9092')
    .split(',')
    .map((broker) => broker.trim())
    .filter((broker) => broker.length > 0);
  if (kafkaBrokers.length === 0) {
    throw new Error('KAFKA_BROKERS must contain at least one broker');
  }

  return {
    kafkaBrokers,
    consumerGroupId: parseNonEmptyString(
      env.IPFS_PUBLISHER_GROUP_ID,
      'ipfs-publisher-v1',
      'IPFS_PUBLISHER_GROUP_ID'
    ),
    batchMaxEvents: parsePositiveInteger(
      env.IPFS_PUBLISHER_BATCH_MAX_EVENTS,
      100,
      'IPFS_PUBLISHER_BATCH_MAX_EVENTS'
    ),
    batchMaxWaitMs: parsePositiveInteger(
      env.IPFS_PUBLISHER_BATCH_MAX_WAIT_MS,
      1_000,
      'IPFS_PUBLISHER_BATCH_MAX_WAIT_MS'
    ),
    maxRetries: parsePositiveInteger(
      env.IPFS_PUBLISHER_MAX_RETRIES,
      3,
      'IPFS_PUBLISHER_MAX_RETRIES'
    ),
    retryBackoffMs: parseNonNegativeInteger(
      env.IPFS_PUBLISHER_RETRY_BACKOFF_MS,
      250,
      'IPFS_PUBLISHER_RETRY_BACKOFF_MS'
    ),
    ipfsApiUrl: parseNonEmptyString(
      env.IPFS_API_URL,
      'http://localhost:5001',
      'IPFS_API_URL'
    ),
    healthPort: parsePositiveInteger(
      env.IPFS_PUBLISHER_HEALTH_PORT,
      3_040,
      'IPFS_PUBLISHER_HEALTH_PORT'
    ),
  };
}
