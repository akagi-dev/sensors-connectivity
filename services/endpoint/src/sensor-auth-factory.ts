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
import { type RegistryReader } from '@scp/registry-sync';
import { WhitelistAuth, loadWhitelistConfig } from '@scp/whitelist';
import { logInfo, logWarn } from './logger.js';

/**
 * Sensor authentication strategy types.
 */
export type SensorAuthStrategy = 'registry-sync' | 'whitelist';

/**
 * Configuration for sensor authentication.
 */
export interface SensorAuthConfig {
  strategy: SensorAuthStrategy;
}

/**
 * Loads sensor authentication configuration from environment variables.
 *
 * Environment variables:
 * - SENSOR_AUTH_STRATEGY: Authentication strategy to use (registry-sync or whitelist)
 */
export function loadSensorAuthConfig(
  env: NodeJS.ProcessEnv = process.env
): SensorAuthConfig {
  const strategyStr = env.SENSOR_AUTH_STRATEGY ?? 'registry-sync';
  const strategy = strategyStr === 'whitelist' ? 'whitelist' : 'registry-sync';

  if (strategyStr !== strategy) {
    logWarn('invalid SENSOR_AUTH_STRATEGY, defaulting to registry-sync', {
      provided: strategyStr,
      using: strategy,
    });
  }

  return { strategy };
}

/**
 * Creates a sensor authentication provider based on the configured strategy.
 *
 * @param strategy - The authentication strategy to use
 * @param registryReaderFactory - Factory function to create a RegistryReader instance
 * @returns A RegistryReader instance (registry-sync strategy returns native RegistryReader,
 *          whitelist strategy returns a WhitelistAuth adapter that implements RegistryReader)
 */
export function createSensorAuthProvider(
  strategy: SensorAuthStrategy,
  registryReaderFactory: () => RegistryReader
): RegistryReader {
  logInfo('creating sensor auth provider', { strategy });

  if (strategy === 'whitelist') {
    const whitelistConfig = loadWhitelistConfig();
    const whitelistAuth = new WhitelistAuth(whitelistConfig.allowedSensorIds);

    // Adapt WhitelistAuth to RegistryReader interface
    return {
      async authenticate(sensorId: Uint8Array): Promise<boolean> {
        return whitelistAuth.authenticate(sensorId);
      },
      async getSensorRecord(sensorId: Uint8Array) {
        const isAuthenticated = await whitelistAuth.authenticate(sensorId);
        if (!isAuthenticated) {
          return null;
        }
        return {
          sensorId,
          enabled: true,
        };
      },
      async isNonceSeen(
        sensorId: Uint8Array,
        nonce: Uint8Array
      ): Promise<boolean> {
        return whitelistAuth.isNonceSeen(sensorId, nonce);
      },
      async rememberNonce(
        sensorId: Uint8Array,
        nonce: Uint8Array
      ): Promise<void> {
        await whitelistAuth.rememberNonce(sensorId, nonce);
      },
    };
  }

  // Default to registry-sync strategy
  return registryReaderFactory();
}
