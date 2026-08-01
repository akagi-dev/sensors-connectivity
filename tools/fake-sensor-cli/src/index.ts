import { fileURLToPath } from 'node:url';

export interface FakeSensorCliOptions {
  endpointUrl: string;
  sensorId: string;
  count: number;
  intervalMs: number;
  authHeader: string;
  authToken?: string;
}

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

  const endpointUrl = argValues.get('endpoint') ?? env.SENSOR_FAKE_ENDPOINT_URL ?? 'http://localhost:3000/v1/telemetry';
  const sensorId = argValues.get('sensor-id') ?? env.SENSOR_FAKE_SENSOR_ID ?? 'debug-sensor-001';
  const count = parsePositiveInteger(argValues.get('count') ?? env.SENSOR_FAKE_COUNT, 1);
  const intervalMs = parseNonNegativeInteger(argValues.get('interval-ms') ?? env.SENSOR_FAKE_INTERVAL_MS, 1000);
  const authHeader = argValues.get('auth-header') ?? env.SENSOR_FAKE_AUTH_HEADER ?? 'authorization';
  const authToken = argValues.get('auth-token') ?? env.SENSOR_FAKE_AUTH_TOKEN;

  const options: FakeSensorCliOptions = {
    endpointUrl,
    sensorId,
    count,
    intervalMs,
    authHeader
  };

  if (authToken) {
    options.authToken = authToken;
  }

  return options;
}

export function createFakePayload(sensorId: string) {
  return {
    sensor_id: sensorId,
    timestamp: new Date().toISOString(),
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

function sleep(ms: number): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

function printUsage() {
  console.log(`Usage: pnpm --filter @scp/fake-sensor-cli fake-sensor -- [options]\n
Options:
  --endpoint <url>       Target telemetry endpoint (default: http://localhost:3000/v1/telemetry)
  --sensor-id <id>       Sensor identifier (default: debug-sensor-001)
  --count <n>            Number of messages to send (default: 1)
  --interval-ms <ms>     Delay between messages in ms (default: 1000)
  --auth-token <token>   Optional auth token value
  --auth-header <name>   Header for auth token (default: authorization)
  --help                 Show this help

Environment variable equivalents:
  SENSOR_FAKE_ENDPOINT_URL
  SENSOR_FAKE_SENSOR_ID
  SENSOR_FAKE_COUNT
  SENSOR_FAKE_INTERVAL_MS
  SENSOR_FAKE_AUTH_TOKEN
  SENSOR_FAKE_AUTH_HEADER`);
}

export async function runFakeSensorCli(args: string[], env: NodeJS.ProcessEnv): Promise<number> {
  if (args.includes('--help')) {
    printUsage();
    return 0;
  }

  let options: FakeSensorCliOptions;
  try {
    options = parseFakeSensorCliOptions(args, env);
  } catch (error) {
    console.error('[fake-sensor-cli] invalid options', error);
    return 1;
  }

  for (let i = 0; i < options.count; i += 1) {
    const payload = createFakePayload(options.sensorId);
    const headers: Record<string, string> = {
      'content-type': 'application/json'
    };

    if (options.authToken) {
      headers[options.authHeader] = options.authToken;
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
