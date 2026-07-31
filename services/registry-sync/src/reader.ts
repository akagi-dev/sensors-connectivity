export interface SensorRegistryRecord {
  sensorAddress: string;
  publicKey: string;
  enabled: boolean;
}

export interface RegistryReader {
  getSensorRecord(sensorAddress: string): Promise<SensorRegistryRecord | null>;
  isNonceSeen(sensorAddress: string, nonce: string): Promise<boolean>;
  rememberNonce(sensorAddress: string, nonce: string): Promise<void>;
}

export class InMemoryRegistryReader implements RegistryReader {
  private readonly sensors = new Map<string, SensorRegistryRecord>();
  private readonly seenNonces = new Set<string>();

  constructor(seed: SensorRegistryRecord[] = []) {
    seed.forEach((record) => {
      this.sensors.set(record.sensorAddress, record);
    });
  }

  async getSensorRecord(sensorAddress: string): Promise<SensorRegistryRecord | null> {
    return this.sensors.get(sensorAddress) ?? null;
  }

  async isNonceSeen(sensorAddress: string, nonce: string): Promise<boolean> {
    return this.seenNonces.has(`${sensorAddress}:${nonce}`);
  }

  async rememberNonce(sensorAddress: string, nonce: string): Promise<void> {
    this.seenNonces.add(`${sensorAddress}:${nonce}`);
  }
}

export function createRegistryReaderFromEnv(): RegistryReader {
  // TODO: wire ioredis-backed projection tables written by the registry sync consumer.
  return new InMemoryRegistryReader([
    {
      sensorAddress: 'sensor-dev-1',
      publicKey: 'TODO_SENSOR_PUBLIC_KEY_BASE64',
      enabled: true
    }
  ]);
}
