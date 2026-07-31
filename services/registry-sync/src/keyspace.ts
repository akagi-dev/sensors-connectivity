export interface RegistryProjectionRecord {
  sensorAddress: string;
  enabled: boolean;
  updatedAtBlock: number;
  updatedAtEvent: string;
}

export interface RegistryEvent {
  blockHeight: number;
  eventIndex: number;
  section: string;
  method: string;
  sensorAddress?: string;
  enabled?: boolean;
  rawData?: unknown;
}

export interface ProjectionUpdate {
  sensorAddress: string;
  enabled: boolean;
}

export interface RedisKeyspace {
  sensorState: (sensorAddress: string) => string;
  nonceState: (sensorAddress: string, nonce: string) => string;
  processedEvents: string;
  cursorHeight: string;
  dlqEvents: string;
  retryAttempts: (eventId: string) => string;
}

export function createRedisKeyspace(prefix: string): RedisKeyspace {
  return {
    sensorState: (sensorAddress: string) => `${prefix}:sensor:${sensorAddress}`,
    nonceState: (sensorAddress: string, nonce: string) => `${prefix}:nonce:${sensorAddress}:${nonce}`,
    processedEvents: `${prefix}:events:processed`,
    cursorHeight: `${prefix}:cursor:finalized-height`,
    dlqEvents: `${prefix}:dlq:events`,
    retryAttempts: (eventId: string) => `${prefix}:retry:${eventId}`
  };
}

export function toChainEventId(event: Pick<RegistryEvent, 'blockHeight' | 'eventIndex'>): string {
  return `${event.blockHeight}:${event.eventIndex}`;
}

export function mapRegistryEventToUpdate(event: RegistryEvent): ProjectionUpdate | null {
  const sensorAddress = event.sensorAddress?.trim();
  if (!sensorAddress) {
    return null;
  }

  const normalizedMethod = `${event.section}.${event.method}`.toLowerCase();
  const explicitEnabled = event.enabled;
  const isRwsEligibility =
    normalizedMethod === 'rws.set_devices' || normalizedMethod === 'rws.newdevices';
  const impliedDisabled =
    normalizedMethod.includes('disable') ||
    normalizedMethod.includes('revoke') ||
    normalizedMethod.includes('remove');

  const impliedEnabled =
    isRwsEligibility ||
    normalizedMethod.includes('create') ||
    normalizedMethod.includes('update') ||
    normalizedMethod.includes('enable') ||
    normalizedMethod.includes('set');

  const enabled =
    explicitEnabled ?? (impliedDisabled ? false : impliedEnabled ? true : undefined);
  if (typeof enabled !== 'boolean') {
    return null;
  }

  return {
    sensorAddress,
    enabled
  };
}
