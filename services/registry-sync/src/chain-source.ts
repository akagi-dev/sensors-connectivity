import { ApiPromise, WsProvider } from '@polkadot/api';
import { decodeAddress, encodeAddress } from '@polkadot/util-crypto';
import type { RegistryEvent } from './keyspace.js';
import { logDebug, logError, logInfo, logWarn } from './logger.js';

const ROBONOMICS_SS58_PREFIX = 32;

export interface FinalizedRegistryEventSource {
  connect(): Promise<void>;
  disconnect(): Promise<void>;
  getLatestFinalizedHeight(): Promise<number>;
  startFrom(
    fromInclusiveHeight: number,
    onEvent: (event: RegistryEvent) => Promise<void>,
    onFinalizedHead?: (height: number) => Promise<void> | void
  ): Promise<void>;
  stop(): Promise<void>;
}

export class SubstrateFinalizedRegistryEventSource implements FinalizedRegistryEventSource {
  private readonly provider: WsProvider;
  private api: ApiPromise | null = null;
  private unsubscribe: (() => void) | null = null;
  private processing = Promise.resolve();

  constructor(private readonly substrateWsUrl: string) {
    this.provider = new WsProvider(this.substrateWsUrl, 0);
  }

  async connect(): Promise<void> {
    logInfo('connecting to substrate finalized event source', {
      substrateWsUrl: this.substrateWsUrl
    });
    this.api = await ApiPromise.create({ provider: this.provider });
    await this.api.isReady;
    logInfo('connected to substrate finalized event source', {
      substrateWsUrl: this.substrateWsUrl
    });
  }

  async disconnect(): Promise<void> {
    await this.stop();
    await this.api?.disconnect();
    this.api = null;
    logInfo('disconnected from substrate finalized event source');
  }

  async getLatestFinalizedHeight(): Promise<number> {
    const api = this.requireApi();
    const header = await api.rpc.chain
      .getFinalizedHead()
      .then((hash) => api.rpc.chain.getHeader(hash));
    logDebug('fetched latest finalized height', {
      height: header.number.toNumber()
    });
    return header.number.toNumber();
  }

  async startFrom(
    fromInclusiveHeight: number,
    onEvent: (event: RegistryEvent) => Promise<void>,
    onFinalizedHead?: (height: number) => Promise<void> | void
  ): Promise<void> {
    const api = this.requireApi();

    const latestFinalizedHash = await api.rpc.chain.getFinalizedHead();
    const latestFinalizedHeader = await api.rpc.chain.getHeader(latestFinalizedHash);
    const latestFinalizedHeight = latestFinalizedHeader.number.toNumber();
    logInfo('starting finalized head backfill', {
      fromInclusiveHeight,
      latestFinalizedHeight
    });

    for (let height = fromInclusiveHeight; height <= latestFinalizedHeight; height += 1) {
      logDebug('processing backfill finalized block', {
        height,
        latestFinalizedHeight
      });
      const hash = await api.rpc.chain.getBlockHash(height);
      const eventsQuery = api.query.system?.events;
      if (!eventsQuery) {
        logWarn('system.events query unavailable while backfilling', { height });
        continue;
      }

      const codec = await eventsQuery.at(hash as never);
      const normalized = normalizeRegistryEvents(toEventRecords(codec), height);
      logDebug('normalized registry events from backfill block', {
        height,
        eventCount: normalized.length
      });
      for (const event of normalized) {
        await onEvent(event);
      }
      await onFinalizedHead?.(height);
    }

    this.unsubscribe = await api.rpc.chain.subscribeFinalizedHeads((header: HeaderLike) => {
      this.processing = this.processing
        .then(async () => {
          const height = header.number.toNumber();
          logDebug('received finalized head', {
            height,
            fromInclusiveHeight
          });
          if (height < fromInclusiveHeight) {
            logDebug('skipping finalized head below start height', {
              height,
              fromInclusiveHeight
            });
            return;
          }

          const eventsQuery = api.query.system?.events;
          if (!eventsQuery) {
            logWarn('system.events query unavailable on finalized head', { height });
            return;
          }

          const codec = await eventsQuery.at(header.hash as never);
          const normalized = normalizeRegistryEvents(toEventRecords(codec), height);
          logDebug('normalized registry events from finalized head', {
            height,
            eventCount: normalized.length
          });
          for (const event of normalized) {
            await onEvent(event);
          }
          await onFinalizedHead?.(height);
        })
        .catch((error) => {
          logError('failed to process finalized head', error, {
            fromInclusiveHeight
          });
        });
    });
    logInfo('subscribed to finalized heads', {
      fromInclusiveHeight
    });
  }

  async stop(): Promise<void> {
    if (this.unsubscribe) {
      logDebug('unsubscribing from finalized heads');
      const current = this.unsubscribe;
      this.unsubscribe = null;
      current();
    }
    await this.processing;
    logDebug('finalized head processing drained');
  }

  private requireApi(): ApiPromise {
    if (!this.api) {
      throw new Error('Substrate API is not connected');
    }
    return this.api;
  }
}

export function normalizeRegistryEvents(
  records: readonly EventRecordLike[],
  blockHeight: number
): RegistryEvent[] {
  const registryEvents: RegistryEvent[] = [];

  records.forEach((record, fallbackIndex) => {
    const section = String(record.event.section ?? '').trim();
    const method = String(record.event.method ?? '').trim();
    const loweredIdentity = `${section}.${method}`.toLowerCase();

    if (!looksLikeRegistryEvent(loweredIdentity)) {
      return;
    }

    const eventIndex = fallbackIndex;

    const rawData = safeToJson(record.event.data);
    const extractedEntries = extractRegistryEntries(rawData, loweredIdentity);

    if (extractedEntries.length === 0) {
      registryEvents.push({
        blockHeight,
        eventIndex,
        section,
        method,
        rawData
      });
      return;
    }

    extractedEntries.forEach((entry, entryIndex) => {
      const normalizedIndex =
        extractedEntries.length === 1
          ? eventIndex
          : eventIndex * 10_000 + entryIndex;

      const candidate: RegistryEvent = {
        blockHeight,
        eventIndex: normalizedIndex,
        section,
        method,
        rawData
      };

      if (entry.sensorId) {
        candidate.sensorId = entry.sensorId;
      }
      if (typeof entry.enabled === 'boolean') {
        candidate.enabled = entry.enabled;
      }

      registryEvents.push(candidate);
    });
  });

  return registryEvents;
}

function toEventRecords(codec: unknown): EventRecordLike[] {
  if (!codec || typeof codec !== 'object') {
    return [];
  }

  const candidate = codec as { toArray?: () => unknown[] };
  if (typeof candidate.toArray === 'function') {
    return candidate.toArray().filter(isEventRecordLike);
  }

  if (Array.isArray(codec)) {
    return codec.filter(isEventRecordLike);
  }

  return [];
}

function isEventRecordLike(value: unknown): value is EventRecordLike {
  if (!value || typeof value !== 'object') {
    return false;
  }

  const record = value as Partial<EventRecordLike>;
  return Boolean(record.event) && Boolean(record.phase);
}

function looksLikeRegistryEvent(name: string): boolean {
  if (name === 'rws.newdevices') {
    return true;
  }

  if (name.includes('registry')) {
    return true;
  }

  const referencesSensor = name.includes('sensor') || name.includes('device');
  const referencesKey = name.includes('key');
  const referencesAuth =
    name.includes('auth') || name.includes('enable') || name.includes('disable');

  return (referencesSensor || referencesKey) && referencesAuth;
}

function safeToJson(data: { toJSON?: () => unknown; toHuman?: () => unknown }): unknown {
  try {
    if (typeof data.toJSON === 'function') {
      return data.toJSON();
    }
    if (typeof data.toHuman === 'function') {
      return data.toHuman();
    }
  } catch {
    return undefined;
  }

  return undefined;
}

function extractRegistryEntries(
  raw: unknown,
  normalizedIdentity: string
): Array<{ sensorId?: string; enabled?: boolean }> {
  if (normalizedIdentity === 'rws.newdevices') {
    const addresses = extractDeviceAddresses(raw);
    return addresses.map((address) => ({
      sensorId: address,
      enabled: true
    }));
  }

  return [extractRegistryFields(raw)];
}

function extractDeviceAddresses(raw: unknown): string[] {
  if (Array.isArray(raw) && raw.every((value) => typeof value === 'string')) {
    return raw
      .filter((value): value is string => value.trim().length > 0)
      .map((value) => normalizeRobonomicsAddress(value));
  }

  if (Array.isArray(raw)) {
    for (const item of raw) {
      const nested = extractDeviceAddresses(item);
      if (nested.length > 0) {
        return nested;
      }
    }
  }

  if (raw && typeof raw === 'object') {
    const objectValue = raw as Record<string, unknown>;
    for (const [key, value] of Object.entries(objectValue)) {
      if (key.toLowerCase().includes('device')) {
        const nested = extractDeviceAddresses(value);
        if (nested.length > 0) {
          return nested;
        }
      }
    }
  }

  return [];
}

function normalizeRobonomicsAddress(address: string): string {
  const trimmed = address.trim();
  try {
    return encodeAddress(decodeAddress(trimmed), ROBONOMICS_SS58_PREFIX);
  } catch {
    return trimmed;
  }
}

function extractRegistryFields(raw: unknown): {
  sensorId?: string;
  enabled?: boolean;
} {
  const sensorId =
    findStringByKey(raw, ['sensorId', 'sensor_id', 'sensor', 'device', 'account']) ??
    findStringByPosition(raw, 0);
  const enabled = inferEnabled(raw);

  const result: { sensorId?: string; enabled?: boolean } = {};
  if (sensorId) {
    result.sensorId = sensorId;
  }
  if (typeof enabled === 'boolean') {
    result.enabled = enabled;
  }
  return result;
}

function findStringByKey(value: unknown, keys: readonly string[]): string | undefined {
  if (!value || typeof value !== 'object') {
    return undefined;
  }

  const objectValue = value as Record<string, unknown>;
  for (const key of keys) {
    const direct = objectValue[key];
    if (typeof direct === 'string' && direct.trim()) {
      return direct;
    }

    for (const [candidateKey, candidateValue] of Object.entries(objectValue)) {
      if (
        candidateKey.toLowerCase() === key.toLowerCase() &&
        typeof candidateValue === 'string' &&
        candidateValue.trim()
      ) {
        return candidateValue;
      }
    }
  }

  for (const nested of Object.values(objectValue)) {
    const nestedMatch = findStringByKey(nested, keys);
    if (nestedMatch) {
      return nestedMatch;
    }
  }

  return undefined;
}

function findStringByPosition(value: unknown, position: number): string | undefined {
  if (!Array.isArray(value)) {
    return undefined;
  }

  const candidate = value[position];
  return typeof candidate === 'string' && candidate.trim() ? candidate : undefined;
}

function inferEnabled(value: unknown): boolean | undefined {
  if (!value || typeof value !== 'object') {
    return undefined;
  }

  const objectValue = value as Record<string, unknown>;
  const enabledValue = objectValue.enabled ?? objectValue.isEnabled ?? objectValue.active;

  if (typeof enabledValue === 'boolean') {
    return enabledValue;
  }

  const disabledValue = objectValue.disabled ?? objectValue.isDisabled;
  if (typeof disabledValue === 'boolean') {
    return !disabledValue;
  }

  for (const nested of Object.values(objectValue)) {
    const nestedEnabled = inferEnabled(nested);
    if (typeof nestedEnabled === 'boolean') {
      return nestedEnabled;
    }
  }

  return undefined;
}

interface HeaderLike {
  number: {
    toNumber(): number;
  };
  hash: unknown;
}

interface EventRecordLike {
  event: {
    section?: unknown;
    method?: unknown;
    data: {
      toJSON?: () => unknown;
      toHuman?: () => unknown;
    };
  };
  phase: {
    isApplyExtrinsic: boolean;
    asApplyExtrinsic: {
      toNumber(): number;
    };
  };
}
