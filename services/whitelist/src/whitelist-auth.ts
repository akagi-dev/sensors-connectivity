import type { SensorAuth } from '@scp/contracts';
import { logDebug, logInfo } from './logger.js';

/**
 * Whitelist-based sensor authentication strategy.
 *
 * This implementation provides simple allowlist-based authorization where
 * sensors are authorized based on membership in a predefined list.
 */
export class WhitelistAuth implements SensorAuth {
  private readonly allowedSensors: Set<string>;
  private readonly nonces = new Set<string>();

  constructor(allowedSensorIds: string[]) {
    this.allowedSensors = new Set(allowedSensorIds);
    logInfo('whitelist auth initialized', {
      sensor_count: this.allowedSensors.size,
    });
  }

  /**
   * Authenticates a sensor by checking if it exists in the whitelist.
   *
   * @param sensorId - The sensor ID to authenticate
   * @returns Promise resolving to true if the sensor is in the whitelist, false otherwise
   */
  async authenticate(sensorId: string): Promise<boolean> {
    const allowed = this.allowedSensors.has(sensorId);
    logDebug('whitelist authentication check', {
      sensor_id: sensorId,
      allowed,
    });
    return allowed;
  }

  /**
   * Checks if a nonce has been seen before for replay protection.
   *
   * @param sensorId - The sensor ID
   * @param nonce - The nonce to check
   * @returns Promise resolving to true if the nonce has been seen, false otherwise
   */
  async isNonceSeen(sensorId: string, nonce: string): Promise<boolean> {
    const key = `${sensorId}:${nonce}`;
    return this.nonces.has(key);
  }

  /**
   * Remembers a nonce to prevent replay attacks.
   *
   * @param sensorId - The sensor ID
   * @param nonce - The nonce to remember
   */
  async rememberNonce(sensorId: string, nonce: string): Promise<void> {
    const key = `${sensorId}:${nonce}`;
    this.nonces.add(key);
    logDebug('nonce remembered', {
      sensor_id: sensorId,
      nonce,
    });
  }

  /**
   * Gets the list of allowed sensor IDs.
   *
   * @returns Array of allowed sensor IDs
   */
  getAllowedSensors(): string[] {
    return Array.from(this.allowedSensors);
  }
}
