/**
 * Shared sensor authentication contract.
 *
 * This interface defines the standard contract for sensor authentication strategies.
 * Implementations can be swapped at runtime to support different authorization mechanisms
 * (e.g., registry-based validation, whitelist-based validation).
 */
export interface SensorAuth {
  /**
   * Authenticates a sensor by its ID.
   *
   * @param sensorId - The sensor ID to authenticate (32-byte public key)
   * @returns Promise resolving to true if the sensor is authorized, false otherwise
   */
  authenticate(sensorId: Uint8Array): Promise<boolean>;
}
