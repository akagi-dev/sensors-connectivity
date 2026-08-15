import { z } from 'zod';

const whitelistConfigSchema = z.object({
  allowedSensorIds: z.array(z.string()).default([])
});

export type WhitelistConfig = z.infer<typeof whitelistConfigSchema>;

/**
 * Loads whitelist configuration from environment variables.
 *
 * Environment variables:
 * - WHITELIST_SENSOR_IDS: Comma-separated list of allowed sensor IDs
 */
export function loadWhitelistConfig(): WhitelistConfig {
  const env = process.env;
  
  const allowedSensorIds = env.WHITELIST_SENSOR_IDS
    ? env.WHITELIST_SENSOR_IDS.split(',').map((id) => id.trim()).filter(Boolean)
    : [];

  return whitelistConfigSchema.parse({
    allowedSensorIds
  });
}
