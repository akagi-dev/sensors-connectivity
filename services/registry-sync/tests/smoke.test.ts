import { describe, expect, it } from 'vitest';
import { createRegistrySyncService } from '../src/index.js';
import { createRegistryReaderFromEnv } from '../src/reader.js';

describe('registry-sync smoke', () => {
  it('starts and stops in stub mode', async () => {
    const service = createRegistrySyncService();
    await service.start();
    await service.stop();
    expect(true).toBe(true);
  });

  it('provides registry read interface', async () => {
    const reader = createRegistryReaderFromEnv();
    const record = await reader.getSensorRecord('sensor-dev-1');
    expect(record?.enabled).toBe(true);
  });
});
