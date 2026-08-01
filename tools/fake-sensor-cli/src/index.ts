import { fileURLToPath } from 'node:url';
import { randomUUID } from 'node:crypto';
import { canonicalize } from 'json-canonicalize';
import { cryptoWaitReady, ed25519PairFromSeed, ed25519Sign, encodeAddress } from '@polkadot/util-crypto';

export interface FakeSensorCliOptions {
  endpointUrl: string;
  sensorAddress: string;
  count: number;
  intervalMs: number;
  sensorZone?: string;
  signerSeedHex: string;
}

const DEFAULT_ENDPOINT_URL = 'http://localhost:3000/v1/telemetry';
const DEFAULT_COUNT = 1;
const DEFAULT_INTERVAL_MS = 1000;
const DEFAULT_SIGNER_SEED_HEX = '0x0000000000000000000000000000000000000000000000000000000000000001';

export function parseFakeSensorCliOptions(args: string[], env: NodeJS.ProcessEnv): FakeSensorCliOptions {
  const argValues = new Map<string, string>();
  for (let i = 0; i < args.length; i += 1) {
    const token = args[i];
    if (!token) {
      continue;
    }

    if (!token.startsWith('--')) {
      continue;
    }

    const [rawKey, inlineValue] = token.slice(2).split('=', 2);
    if (!rawKey) {
      continue;
    }
    const key = rawKey.trim();
    const nextToken = args[i + 1];
    const value = inlineValue ?? (nextToken && !nextToken.startsWith('--') ? nextToken : undefined);
    if (value !== undefined && inlineValue === undefined && nextToken && !nextToken.startsWith('--')) {
      i += 1;
    }

    if (value !== undefined) {
      argValues.set(key, value);
    }
  }

  const endpointUrl = argValues.get('endpoint') ?? env.SENSOR_FAKE_ENDPOINT_URL ?? DEFAULT_ENDPOINT_URL;
  const signerSeedHex = argValues.get('signer-seed-hex') ?? env.SENSOR_FAKE_SIGNER_SEED_HEX ?? DEFAULT_SIGNER_SEED_HEX;
  const count = parsePositiveInteger(argValues.get('count') ?? env.SENSOR_FAKE_COUNT, DEFAULT_COUNT);
  const intervalMs = parseNonNegativeInteger(argValues.get('interval-ms') ?? env.SENSOR_FAKE_INTERVAL_MS, DEFAULT_INTERVAL_MS);
  const sensorZone = argValues.get('sensor-zone') ?? env.SENSOR_FAKE_SENSOR_ZONE;
  const seed = parseSeedHex(signerSeedHex);
  const signer = ed25519PairFromSeed(seed);
  const derivedAddress = encodeAddress(signer.publicKey);
  const sensorAddress =
    argValues.get('sensor-address') ??
    argValues.get('sensor-id') ??
    env.SENSOR_FAKE_SENSOR_ADDRESS ??
    env.SENSOR_FAKE_SENSOR_ID ??
    derivedAddress;

  if (sensorAddress !== derivedAddress) {
    throw new Error(`sensor address does not match signer seed: ${sensorAddress}`);
  }

  const options: FakeSensorCliOptions = {
    endpointUrl,
    sensorAddress,
    count,
    intervalMs,
    signerSeedHex
  };

  if (sensorZone) {
    options.sensorZone = sensorZone;
  }

  return options;
}

export function createFakePayload(sensorAddress: string) {
  return {
    sensor_address: sensorAddress,
    timestamp: new Date().toISOString(),
    nonce: randomUUID(),
    measurements: {
      temperature_c: Number((18 + Math.random() * 8).toFixed(2)),
      humidity_pct: Number((30 + Math.random() * 40).toFixed(2))
    }
  };
}

function parsePositiveInteger(value: string | undefined, fallback: number): number {
  if (value === undefined) {
    return fallback;
  }

  const parsed = Number.parseInt(value, 10);
  if (!Number.isFinite(parsed) || parsed < 1) {
    throw new Error(`Invalid positive integer value: ${value}`);
  }

  return parsed;
}

function parseNonNegativeInteger(value: string | undefined, fallback: number): number {
  if (value === undefined) {
    return fallback;
  }

  const parsed = Number.parseInt(value, 10);
  if (!Number.isFinite(parsed) || parsed < 0) {
    throw new Error(`Invalid non-negative integer value: ${value}`);
  }

  return parsed;
}

function parseSeedHex(seedHex: string): Uint8Array {
  const normalizedSeed = seedHex.startsWith('0x') ? seedHex.slice(2) : seedHex;
  if (!/^[0-9a-fA-F]{64}$/.test(normalizedSeed)) {
    throw new Error('signer seed must be 32-byte hex');
  }
  return Uint8Array.from(Buffer.from(normalizedSeed, 'hex'));
}

function signPayload(
  payload: ReturnType<typeof createFakePayload>,
  signerSeedHex: string
): string {
  const pair = ed25519PairFromSeed(parseSeedHex(signerSeedHex));
  const canonicalMeasurements = canonicalize(payload.measurements);
  const message = `${canonicalMeasurements}${payload.timestamp}${payload.nonce}${payload.sensor_address}`;
  const signature = ed25519Sign(new TextEncoder().encode(message), pair);
  return Buffer.from(signature).toString('base64');
}

function sleep(ms: number): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

function printUsage() {
  console.log(`Usage: pnpm --filter @scp/fake-sensor-cli fake-sensor -- [options]\n
Options:
  --endpoint <url>         Target telemetry endpoint (default: http://localhost:3000/v1/telemetry)
  --sensor-address <ss58>  Sensor address. Must match signer seed public key.
  --signer-seed-hex <hex>  32-byte Ed25519 seed hex (default: deterministic debug seed)
  --sensor-zone <zone>     Optional X-Sensor-Zone header (ru|eu-west|us-east|ap-southeast)
  --count <n>              Number of messages to send (default: 1)
  --interval-ms <ms>       Delay between messages in ms (default: 1000)
  --help                   Show this help

Environment variable equivalents:
  SENSOR_FAKE_ENDPOINT_URL
  SENSOR_FAKE_SENSOR_ADDRESS
  SENSOR_FAKE_SIGNER_SEED_HEX
  SENSOR_FAKE_SENSOR_ZONE
  SENSOR_FAKE_COUNT
  SENSOR_FAKE_INTERVAL_MS`);
}

export async function runFakeSensorCli(args: string[], env: NodeJS.ProcessEnv): Promise<number> {
  if (args.includes('--help')) {
    printUsage();
    return 0;
  }

  let options: FakeSensorCliOptions;
  try {
    await cryptoWaitReady();
    options = parseFakeSensorCliOptions(args, env);
  } catch (error) {
    console.error('[fake-sensor-cli] invalid options', error);
    return 1;
  }

  for (let i = 0; i < options.count; i += 1) {
    const unsignedPayload = createFakePayload(options.sensorAddress);
    const payload = {
      ...unsignedPayload,
      signature: signPayload(unsignedPayload, options.signerSeedHex)
    };
    const headers: Record<string, string> = {
      'content-type': 'application/json; charset=utf-8',
      'x-request-id': randomUUID()
    };

    if (options.sensorZone) {
      headers['x-sensor-zone'] = options.sensorZone;
    }

    console.log(`[fake-sensor-cli] sending ${i + 1}/${options.count}`, payload);

    try {
      const response = await fetch(options.endpointUrl, {
        method: 'POST',
        headers,
        body: JSON.stringify(payload)
      });
      console.log(`[fake-sensor-cli] response ${response.status} ${response.statusText}`);
      if (!response.ok) {
        return 1;
      }
    } catch (error) {
      console.error('[fake-sensor-cli] request failed', error);
      return 1;
    }

    if (i + 1 < options.count && options.intervalMs > 0) {
      await sleep(options.intervalMs);
    }
  }

  return 0;
}

const isDirectRun = process.argv[1] === fileURLToPath(import.meta.url);
if (isDirectRun) {
  runFakeSensorCli(process.argv.slice(2), process.env)
    .then((exitCode) => {
      process.exitCode = exitCode;
    })
    .catch((error: unknown) => {
      console.error('[fake-sensor-cli] unexpected failure', error);
      process.exitCode = 1;
    });
}
