export interface RegistryProjectionRecord {
  sensorId: string;
  enabled: boolean;
  updatedAtBlock: number;
  updatedAtEvent: string;
}

export interface RegistryEvent {
  blockHeight: number;
  eventIndex: number;
  section: string;
  method: string;
  sensorId?: string;
  enabled?: boolean;
  rawData?: unknown;
}

export interface ProjectionUpdate {
  sensorId: string;
  enabled: boolean;
}

export interface RedisKeyspace {
  sensorState: (sensorId: string) => string;
  nonceState: (sensorId: string, nonce: string) => string;
  processedEvents: string;
  cursorHeight: string;
  dlqEvents: string;
}

export function createRedisKeyspace(prefix: string): RedisKeyspace {
  return {
    sensorState: (sensorId: string) => `${prefix}:sensor:${sensorId}`,
    nonceState: (sensorId: string, nonce: string) =>
      `${prefix}:nonce:${sensorId}:${nonce}`,
    processedEvents: `${prefix}:events:processed`,
    cursorHeight: `${prefix}:cursor:finalized-height`,
    dlqEvents: `${prefix}:dlq:events`,
  };
}

export function toChainEventId(
  event: Pick<RegistryEvent, 'blockHeight' | 'eventIndex'>
): string {
  return `${event.blockHeight}:${event.eventIndex}`;
}

export function mapRegistryEventToUpdate(
  event: RegistryEvent
): ProjectionUpdate | null {
  const sensorId = event.sensorId?.trim();
  if (!sensorId) {
    return null;
  }

  const normalizedMethod = `${event.section}.${event.method}`.toLowerCase();
  const explicitEnabled = event.enabled;
  const isRwsEligibility = normalizedMethod === 'rws.newdevices';
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
    explicitEnabled ??
    (impliedDisabled ? false : impliedEnabled ? true : undefined);
  if (typeof enabled !== 'boolean') {
    return null;
  }

  return {
    sensorId,
    enabled,
  };
}
