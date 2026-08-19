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
