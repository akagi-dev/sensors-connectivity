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
import type { SensorAuth } from '@scp/core';
import { formatSensorId } from '@scp/core';
import { decodeAddress } from '@polkadot/util-crypto';
import { logDebug, logInfo } from './logger.js';

function bytesEqual(a: Uint8Array, b: Uint8Array): boolean {
  if (a.length !== b.length) return false;
  for (let i = 0; i < a.length; i++) {
    if (a[i] !== b[i]) return false;
  }
  return true;
}

/**
 * Whitelist-based sensor authentication strategy.
 *
 * This implementation provides simple allowlist-based authorization where
 * sensors are authorized based on membership in a predefined list.
 *
 * Accepts Polkadot SS58 addresses in config, decodes them to public keys (binary),
 * and compares against incoming binary sensor IDs.
 */
export class WhitelistAuth implements SensorAuth {
  private readonly allowedSensors: Uint8Array[];
  private readonly nonces = new Set<string>();

  constructor(allowedSensorAddresses: string[]) {
    // Decode SS58 addresses to binary public keys
    this.allowedSensors = allowedSensorAddresses
      .map((addr) => {
        try {
          return decodeAddress(addr);
        } catch (error) {
          logInfo('failed to decode sensor address, skipping', {
            address: addr,
            error: error instanceof Error ? error.message : String(error),
          });
          return new Uint8Array(0);
        }
      })
      .filter((key) => key.length === 32);

    logInfo('whitelist auth initialized', {
      sensor_count: this.allowedSensors.length,
    });
  }

  /**
   * Authenticates a sensor by checking if it exists in the whitelist.
   *
   * @param sensorId - The sensor ID to authenticate (32-byte public key)
   * @returns Promise resolving to true if the sensor is in the whitelist, false otherwise
   */
  async authenticate(sensorId: Uint8Array): Promise<boolean> {
    const allowed = this.allowedSensors.some((allowedId) =>
      bytesEqual(sensorId, allowedId)
    );
    logDebug('whitelist authentication check', {
      sensor_id: formatSensorId(sensorId),
      allowed,
    });
    return allowed;
  }

  /**
   * Checks if a nonce has been seen before for replay protection.
   *
   * @param sensorId - The sensor ID (32-byte public key)
   * @param nonce - The nonce to check
   * @returns Promise resolving to true if the nonce has been seen, false otherwise
   */
  async isNonceSeen(sensorId: Uint8Array, nonce: Uint8Array): Promise<boolean> {
    const sensorIdHex = Buffer.from(sensorId).toString('hex');
    const nonceHex = Buffer.from(nonce).toString('hex');
    const key = `${sensorIdHex}:${nonceHex}`;
    const seen = this.nonces.has(key);
    logDebug('nonce seen', {
      sensor_id: formatSensorId(sensorId),
      nonce: nonceHex,
      seen,
    });
    return seen;
  }

  /**
   * Remembers a nonce to prevent replay attacks.
   *
   * @param sensorId - The sensor ID (32-byte public key)
   * @param nonce - The nonce to remember
   */
  async rememberNonce(sensorId: Uint8Array, nonce: Uint8Array): Promise<void> {
    const sensorIdHex = Buffer.from(sensorId).toString('hex');
    const nonceHex = Buffer.from(nonce).toString('hex');
    const key = `${sensorIdHex}:${nonceHex}`;
    this.nonces.add(key);
    logDebug('nonce remembered', {
      sensor_id: formatSensorId(sensorId),
      nonce: nonceHex,
    });
  }

  /**
   * Gets the list of allowed sensor IDs.
   *
   * @returns Array of allowed sensor IDs in SS58 format
   */
  getAllowedSensors(): string[] {
    return this.allowedSensors.map(formatSensorId);
  }
}
