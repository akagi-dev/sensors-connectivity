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
export interface WhitelistConfig {
  allowedSensorIds: string[];
}

/**
 * Loads whitelist configuration from environment variables.
 *
 * Environment variables:
 * - WHITELIST_SENSOR_IDS: Comma-separated list of allowed sensor IDs
 */
export function loadWhitelistConfig(): WhitelistConfig {
  const env = process.env;

  const allowedSensorIds = env.WHITELIST_SENSOR_IDS
    ? env.WHITELIST_SENSOR_IDS.split(',')
        .map((id) => id.trim())
        .filter(Boolean)
    : [];

  return {
    allowedSensorIds,
  };
}
