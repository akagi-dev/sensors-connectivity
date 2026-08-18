import type { SensorAuth } from '@scp/contracts';
import { formatSensorId } from '@scp/contracts';
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
    const key = `${formatSensorId(sensorId)}:${formatSensorId(nonce)}`;
    return this.nonces.has(key);
  }

  /**
   * Remembers a nonce to prevent replay attacks.
   *
   * @param sensorId - The sensor ID (32-byte public key)
   * @param nonce - The nonce to remember
   */
  async rememberNonce(sensorId: Uint8Array, nonce: Uint8Array): Promise<void> {
    const key = `${formatSensorId(sensorId)}:${formatSensorId(nonce)}`;
    this.nonces.add(key);
    logDebug('nonce remembered', {
      sensor_id: formatSensorId(sensorId),
      nonce: formatSensorId(nonce),
    });
  }

  /**
   * Gets the list of allowed sensor IDs (as hex strings for display).
   *
   * @returns Array of allowed sensor IDs in hex format
   */
  getAllowedSensors(): string[] {
    return this.allowedSensors.map(formatSensorId);
  }
}
