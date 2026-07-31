import { ApiPromise, WsProvider } from '@polkadot/api';
import type { Header } from '@polkadot/types/interfaces/runtime';
import type { EventRecord } from '@polkadot/types/interfaces/system';
import type { RegistryEvent } from './keyspace.js';

export interface FinalizedRegistryEventSource {
  connect(): Promise<void>;
  disconnect(): Promise<void>;
  getLatestFinalizedHeight(): Promise<number>;
  startFrom(
    fromInclusiveHeight: number,
    onEvent: (event: RegistryEvent) => Promise<void>
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
    this.api = await ApiPromise.create({ provider: this.provider });
    await this.api.isReady;
  }

  async disconnect(): Promise<void> {
    await this.stop();
    await this.api?.disconnect();
    this.api = null;
  }

  async getLatestFinalizedHeight(): Promise<number> {
    const api = this.requireApi();
    const header = await api.rpc.chain.getFinalizedHead().then((hash) => api.rpc.chain.getHeader(hash));
    return header.number.toNumber();
  }

  async startFrom(
    fromInclusiveHeight: number,
    onEvent: (event: RegistryEvent) => Promise<void>
  ): Promise<void> {
    const api = this.requireApi();

    this.unsubscribe = await api.rpc.chain.subscribeFinalizedHeads((header: Header) => {
      this.processing = this.processing
        .then(async () => {
          const height = header.number.toNumber();
          if (height < fromInclusiveHeight) {
            return;
          }

          const blockHash = header.hash;
          const events = await api.query.system.events.at(blockHash);
          const normalized = normalizeRegistryEvents(events, height);
          for (const event of normalized) {
            await onEvent(event);
          }
        })
        .catch(() => {
          // keep subscription alive; service-level retry handles processing errors.
        });
    });
  }

  async stop(): Promise<void> {
    if (this.unsubscribe) {
      const current = this.unsubscribe;
      this.unsubscribe = null;
      current();
    }
    await this.processing;
  }

  private requireApi(): ApiPromise {
    if (!this.api) {
      throw new Error('Substrate API is not connected');
    }
    return this.api;
  }
}

export function normalizeRegistryEvents(
  records: readonly EventRecord[],
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

    const eventIndex =
      record.phase.isApplyExtrinsic
        ? record.phase.asApplyExtrinsic.toNumber()
        : fallbackIndex;

    const rawData = safeToJson(record.event.data);
    const extracted = extractRegistryFields(rawData);

    registryEvents.push({
      blockHeight,
      eventIndex,
      section,
      method,
      sensorAddress: extracted.sensorAddress,
      publicKey: extracted.publicKey,
      enabled: extracted.enabled,
      rawData
    });
  });

  return registryEvents;
}

function looksLikeRegistryEvent(name: string): boolean {
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

function extractRegistryFields(raw: unknown): {
  sensorAddress?: string;
  publicKey?: string;
  enabled?: boolean;
} {
  const sensorAddress =
    findStringByKey(raw, ['sensorAddress', 'sensor_address', 'sensor', 'device', 'account']) ??
    findStringByPosition(raw, 0);
  const publicKey =
    findStringByKey(raw, ['publicKey', 'public_key', 'pubKey', 'key']) ??
    findStringByPosition(raw, 1);
  const enabled = inferEnabled(raw);

  return {
    sensorAddress,
    publicKey,
    enabled
  };
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
      if (candidateKey.toLowerCase() === key.toLowerCase() && typeof candidateValue === 'string' && candidateValue.trim()) {
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

  const enabledValue =
    objectValue.enabled ??
    objectValue.isEnabled ??
    objectValue.active;

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
