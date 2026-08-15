import { fileURLToPath } from 'node:url';
import { randomBytes, randomUUID } from 'node:crypto';
import { cryptoWaitReady, ed25519PairFromSeed, ed25519Sign, encodeAddress } from '@polkadot/util-crypto';
import {
  buildEnvelopeSigningBytes,
  concatBytes,
  createSignedEnvelope,
  toSignedEnvelopeBytes
} from '@scp/contracts';
import pino from 'pino';

export interface FakeSensorCliOptions {
  endpointUrl: string;
  sensorId: string;
  count: number;
  intervalMs: number;
  sensorZone?: string;
  signerSeedHex: string;
}

const DEFAULT_ENDPOINT_URL = 'http://localhost:3000/v1/telemetry';
const DEFAULT_COUNT = 1;
const DEFAULT_INTERVAL_MS = 1000;
const DEFAULT_SIGNER_SEED_HEX = '0x0000000000000000000000000000000000000000000000000000000000000001';

const logger = pino({
  name: 'fake-sensor-cli',
  level: process.env.FAKE_SENSOR_CLI_LOG_LEVEL ?? process.env.LOG_LEVEL ?? 'info'
});

function logInfo(message: string, context?: Record<string, unknown>): void {
  logger.info(context ?? {}, message);
}

function logWarn(message: string, context?: Record<string, unknown>): void {
  logger.warn(context ?? {}, message);
}

function logError(message: string, error: unknown, context?: Record<string, unknown>): void {
  logger.error(
    {
      ...(context ?? {}),
      error: error instanceof Error ? error.message : String(error)
    },
    message
  );
}

export function parseFakeSensorCliOptions(args: string[], env: NodeJS.ProcessEnv): FakeSensorCliOptions {
  const argValues = new Map<string, string>();
  const supportedKeys = new Set([
    'endpoint',
    'signer-seed-hex',
    'count',
    'interval-ms',
    'sensor-zone',
    'sensor-id',
    'sensor-id'
  ]);

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
    if (!supportedKeys.has(key)) {
      throw new Error(`Unknown option: --${key}`);
    }

    const nextToken = args[i + 1];
    const value = inlineValue ?? (nextToken && !nextToken.startsWith('--') ? nextToken : undefined);
    if (value === undefined) {
      throw new Error(`Missing value for --${key}`);
    }
    if (inlineValue === undefined && nextToken && !nextToken.startsWith('--')) {
      i += 1;
    }

    argValues.set(key, value);
  }

  const endpointUrl = argValues.get('endpoint') ?? env.SENSOR_FAKE_ENDPOINT_URL ?? DEFAULT_ENDPOINT_URL;
  const signerSeedHex = argValues.get('signer-seed-hex') ?? env.SENSOR_FAKE_SIGNER_SEED_HEX ?? DEFAULT_SIGNER_SEED_HEX;
  const count = parsePositiveInteger(argValues.get('count') ?? env.SENSOR_FAKE_COUNT, DEFAULT_COUNT);
  const intervalMs = parseNonNegativeInteger(argValues.get('interval-ms') ?? env.SENSOR_FAKE_INTERVAL_MS, DEFAULT_INTERVAL_MS);
  const sensorZone = argValues.get('sensor-zone') ?? env.SENSOR_FAKE_SENSOR_ZONE;
  const seed = parseSeedHex(signerSeedHex);
  const signer = ed25519PairFromSeed(seed);
  const derivedAddress = encodeAddress(signer.publicKey);
  const sensorId =
    argValues.get('sensor-id') ??
    env.SENSOR_FAKE_SENSOR_ID ??
    derivedAddress;

  if (sensorId !== derivedAddress) {
    throw new Error(`sensor id does not match signer seed: ${sensorId}`);
  }

  const options: FakeSensorCliOptions = {
    endpointUrl,
    sensorId,
    count,
    intervalMs,
    signerSeedHex
  };

  if (sensorZone) {
    options.sensorZone = sensorZone;
  }

  return options;
}

export interface FakeEnvelopePayload {
  envelopeBytes: Uint8Array;
  sensorAddress: string;
  timestamp: bigint;
  nonce: Uint8Array;
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

function encodeVarint(value: number): Uint8Array {
  if (!Number.isSafeInteger(value) || value < 0) {
    throw new Error(`Invalid varint value: ${value}`);
  }
  const chunks: number[] = [];
  let remaining = value;
  while (remaining >= 0x80) {
    chunks.push((remaining & 0x7f) | 0x80);
    remaining >>>= 7;
  }
  chunks.push(remaining);
  return Uint8Array.from(chunks);
}

function encodeTag(fieldNumber: number, wireType: number): Uint8Array {
  return encodeVarint((fieldNumber << 3) | wireType);
}

function encodeLengthDelimitedField(fieldNumber: number, value: Uint8Array): Uint8Array {
  return concatBytes([encodeTag(fieldNumber, 2), encodeVarint(value.length), value]);
}

function encodeDoubleField(fieldNumber: number, value: number): Uint8Array {
  const bytes = new Uint8Array(8);
  new DataView(bytes.buffer).setFloat64(0, value, true);
  return concatBytes([encodeTag(fieldNumber, 1), bytes]);
}

function createBme280TemperatureSensor(temperatureCelsius: number): Uint8Array {
  const measurement = encodeLengthDelimitedField(1, encodeDoubleField(1, temperatureCelsius));
  return encodeLengthDelimitedField(2, measurement);
}

function createBme280HumiditySensor(humidityPercent: number): Uint8Array {
  const measurement = encodeLengthDelimitedField(2, encodeDoubleField(1, humidityPercent));
  return encodeLengthDelimitedField(2, measurement);
}

function createCoreMessageBytes(ownerPublicKey: Uint8Array, temperatureCelsius: number, humidityPercent: number): Uint8Array {
  const metadata = encodeLengthDelimitedField(1, encodeLengthDelimitedField(1, ownerPublicKey));
  const urban = encodeLengthDelimitedField(
    2,
    concatBytes([
      encodeLengthDelimitedField(1, createBme280TemperatureSensor(temperatureCelsius)),
      encodeLengthDelimitedField(1, createBme280HumiditySensor(humidityPercent))
    ])
  );
  return concatBytes([metadata, urban]);
}

export function createFakeEnvelopePayload(signerSeedHex: string): FakeEnvelopePayload {
  const pair = ed25519PairFromSeed(parseSeedHex(signerSeedHex));
  const temperature = Number((18 + Math.random() * 8).toFixed(2));
  const humidity = Number((30 + Math.random() * 40).toFixed(2));
  const messageBytes = createCoreMessageBytes(pair.publicKey, temperature, humidity);
  const timestamp = BigInt(Date.now());
  const nonce = randomBytes(16);
  const envelope = createSignedEnvelope({
    sensorId: pair.publicKey,
    timestamp,
    nonce,
    message: messageBytes
  });
  const signature = ed25519Sign(buildEnvelopeSigningBytes(envelope), pair);
  envelope.signature = signature;
  return {
    envelopeBytes: toSignedEnvelopeBytes(envelope),
    sensorAddress: encodeAddress(pair.publicKey, 32),
    timestamp,
    nonce
  };
}

function sleep(ms: number): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

function printUsage() {
  logInfo(`Usage: pnpm --filter @scp/fake-sensor-cli fake-sensor -- [options]\n
Options:
  --endpoint <url>         Target telemetry endpoint (default: http://localhost:3000/v1/telemetry)
  --sensor-id <ss58>  Sensor ID. Must match signer seed public key.
  --signer-seed-hex <hex>  32-byte Ed25519 seed hex (default: deterministic debug seed)
  --sensor-zone <zone>     Optional X-Sensor-Zone header (ru|eu-west|us-east|ap-southeast)
  --count <n>              Number of messages to send (default: 1)
  --interval-ms <ms>       Delay between messages in ms (default: 1000)
  --help                   Show this help

Environment variable equivalents:
  SENSOR_FAKE_ENDPOINT_URL
  SENSOR_FAKE_SENSOR_ID
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
    logError('invalid options', error);
    return 1;
  }

  for (let i = 0; i < options.count; i += 1) {
    const payload = createFakeEnvelopePayload(options.signerSeedHex);
    const headers: Record<string, string> = {
      'content-type': 'application/protobuf',
      'x-request-id': randomUUID()
    };

    if (options.sensorZone) {
      headers['x-sensor-zone'] = options.sensorZone;
    }

    logInfo('sending telemetry payload', {
      iteration: i + 1,
      total: options.count,
      sensor_id: payload.sensorAddress,
      timestamp: Number(payload.timestamp),
      nonce_hex: Buffer.from(payload.nonce).toString('hex')
    });

    try {
      const response = await fetch(options.endpointUrl, {
        method: 'POST',
        headers,
        body: Buffer.from(payload.envelopeBytes)
      });
      if (response.ok) {
        logInfo('response received', { status: response.status, statusText: response.statusText });
      } else {
        logWarn('response received with non-ok status', {
          status: response.status,
          statusText: response.statusText
        });
      }
      if (!response.ok) {
        return 1;
      }
    } catch (error) {
      logError('request failed', error);
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
      logError('unexpected failure', error);
      process.exitCode = 1;
    });
}
