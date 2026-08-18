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
