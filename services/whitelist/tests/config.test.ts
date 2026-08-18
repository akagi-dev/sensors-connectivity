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
import { describe, it, expect, beforeEach, afterEach } from 'vitest';
import { loadWhitelistConfig } from '../src/config.js';

describe('loadWhitelistConfig', () => {
  const originalEnv = process.env;

  beforeEach(() => {
    process.env = { ...originalEnv };
  });

  afterEach(() => {
    process.env = originalEnv;
  });

  it('should load sensor IDs from environment variable', () => {
    process.env.WHITELIST_SENSOR_IDS = 'sensor-1,sensor-2,sensor-3';

    const config = loadWhitelistConfig();

    expect(config.allowedSensorIds).toEqual([
      'sensor-1',
      'sensor-2',
      'sensor-3',
    ]);
  });

  it('should trim whitespace from sensor IDs', () => {
    process.env.WHITELIST_SENSOR_IDS = ' sensor-1 , sensor-2 , sensor-3 ';

    const config = loadWhitelistConfig();

    expect(config.allowedSensorIds).toEqual([
      'sensor-1',
      'sensor-2',
      'sensor-3',
    ]);
  });

  it('should filter empty sensor IDs', () => {
    process.env.WHITELIST_SENSOR_IDS = 'sensor-1,,sensor-2,';

    const config = loadWhitelistConfig();

    expect(config.allowedSensorIds).toEqual(['sensor-1', 'sensor-2']);
  });

  it('should return empty array when env var is not set', () => {
    delete process.env.WHITELIST_SENSOR_IDS;

    const config = loadWhitelistConfig();

    expect(config.allowedSensorIds).toEqual([]);
  });

  it('should handle single sensor ID', () => {
    process.env.WHITELIST_SENSOR_IDS = 'sensor-1';

    const config = loadWhitelistConfig();

    expect(config.allowedSensorIds).toEqual(['sensor-1']);
  });
});
