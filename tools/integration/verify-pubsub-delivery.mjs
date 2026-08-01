import { readFile, writeFile } from 'node:fs/promises';

function parseArgs(args) {
  const [mode, ...rest] = args;
  if (mode !== 'collect' && mode !== 'verify') {
    throw new Error('first argument must be "collect" or "verify"');
  }

  const values = new Map();
  for (let i = 0; i < rest.length; i += 1) {
    const token = rest[i];
    if (!token.startsWith('--')) {
      continue;
    }

    const key = token.slice(2);
    const next = rest[i + 1];
    if (!next || next.startsWith('--')) {
      throw new Error(`missing value for --${key}`);
    }
    values.set(key, next);
    i += 1;
  }

  if (mode === 'collect') {
    const topic = values.get('topic') ?? 'telemetry/authorized/v1';
    const expectedCount = Number.parseInt(values.get('expected-count') ?? '', 10);
    const timeoutMs = Number.parseInt(values.get('timeout-ms') ?? '', 10);
    const outputFile = values.get('output-file');
    const ipfsApiUrl = values.get('ipfs-api-url') ?? 'http://127.0.0.1:5001';
    const sensorId = values.get('sensor-id');

    if (!Number.isFinite(expectedCount) || expectedCount < 1) {
      throw new Error('collect mode requires --expected-count >= 1');
    }
    if (!Number.isFinite(timeoutMs) || timeoutMs < 1000) {
      throw new Error('collect mode requires --timeout-ms >= 1000');
    }
    if (!outputFile) {
      throw new Error('collect mode requires --output-file');
    }

    return {
      mode,
      topic,
      expectedCount,
      timeoutMs,
      outputFile,
      ipfsApiUrl,
      sensorId
    };
  }

  const fakeSensorLogFile = values.get('fake-sensor-log-file');
  const pubsubMessagesFile = values.get('pubsub-messages-file');
  const expectedCount = Number.parseInt(values.get('expected-count') ?? '', 10);

  if (!fakeSensorLogFile) {
    throw new Error('verify mode requires --fake-sensor-log-file');
  }
  if (!pubsubMessagesFile) {
    throw new Error('verify mode requires --pubsub-messages-file');
  }
  if (!Number.isFinite(expectedCount) || expectedCount < 1) {
    throw new Error('verify mode requires --expected-count >= 1');
  }

  return {
    mode,
    fakeSensorLogFile,
    pubsubMessagesFile,
    expectedCount
  };
}

function decodePubsubPayload(line) {
  try {
    const parsed = JSON.parse(line);
    if (!parsed?.data || typeof parsed.data !== 'string') {
      return null;
    }

    const decoded = Buffer.from(parsed.data, 'base64').toString('utf8');
    const payload = JSON.parse(decoded);
    if (!payload?.sensor_id || !payload?.nonce) {
      return null;
    }

    return payload;
  } catch {
    return null;
  }
}

async function collectMessages(options) {
  const controller = new AbortController();
  const timeout = setTimeout(() => {
    controller.abort();
  }, options.timeoutMs);

  const endpoint = `${options.ipfsApiUrl}/api/v0/pubsub/sub?arg=${encodeURIComponent(options.topic)}`;
  const response = await fetch(endpoint, {
    method: 'POST',
    signal: controller.signal
  });

  if (!response.ok || !response.body) {
    throw new Error(`failed to subscribe to IPFS pubsub: ${response.status} ${response.statusText}`);
  }

  const reader = response.body.getReader();
  const decoder = new TextDecoder();
  const payloads = [];
  let buffer = '';

  try {
    while (payloads.length < options.expectedCount) {
      const { done, value } = await reader.read();
      if (done) {
        break;
      }

      buffer += decoder.decode(value, { stream: true });
      const lines = buffer.split('\n');
      buffer = lines.pop() ?? '';

      for (const line of lines) {
        if (!line.trim()) {
          continue;
        }

        const payload = decodePubsubPayload(line);
        if (!payload) {
          continue;
        }

        if (options.sensorId && payload.sensor_id !== options.sensorId) {
          continue;
        }

        payloads.push(payload);
        console.log(`received ${payloads.length}/${options.expectedCount} topic messages`);

        if (payloads.length >= options.expectedCount) {
          break;
        }
      }
    }
  } catch (error) {
    if (!(error instanceof Error && error.name === 'AbortError')) {
      throw error;
    }
  } finally {
    clearTimeout(timeout);
    controller.abort();
    reader.releaseLock();
  }

  await writeFile(options.outputFile, `${JSON.stringify(payloads, null, 2)}\n`, 'utf8');

  if (payloads.length < options.expectedCount) {
    throw new Error(
      `timed out waiting for pubsub delivery: received ${payloads.length}/${options.expectedCount} messages`
    );
  }
}

function parseExpectedFromFakeSensorLog(rawLog) {
  const expected = [];

  for (const line of rawLog.split('\n')) {
    if (!line.trim()) {
      continue;
    }

    try {
      const entry = JSON.parse(line);
      if (entry.msg === 'sending telemetry payload' && entry.payload?.nonce) {
        expected.push(entry.payload);
      }
    } catch {
      // Ignore non-JSON lines in logs.
    }
  }

  return expected;
}

function assertDelivery(expected, received) {
  const receivedByNonce = new Map(received.map((item) => [item.nonce, item]));

  for (const sent of expected) {
    const delivered = receivedByNonce.get(sent.nonce);
    if (!delivered) {
      throw new Error(`missing pubsub message for nonce ${sent.nonce}`);
    }

    if (delivered.sensor_id !== sent.sensor_id) {
      throw new Error(`sensor_id mismatch for nonce ${sent.nonce}`);
    }

    if (delivered.timestamp !== sent.timestamp) {
      throw new Error(`timestamp mismatch for nonce ${sent.nonce}`);
    }

    if (delivered.signature !== sent.signature) {
      throw new Error(`signature mismatch for nonce ${sent.nonce}`);
    }

    if (JSON.stringify(delivered.measurements) !== JSON.stringify(sent.measurements)) {
      throw new Error(`measurements mismatch for nonce ${sent.nonce}`);
    }
  }
}

async function verifyMessages(options) {
  const [fakeSensorLog, pubsubMessagesRaw] = await Promise.all([
    readFile(options.fakeSensorLogFile, 'utf8'),
    readFile(options.pubsubMessagesFile, 'utf8')
  ]);

  const expected = parseExpectedFromFakeSensorLog(fakeSensorLog);
  if (expected.length !== options.expectedCount) {
    throw new Error(
      `expected ${options.expectedCount} fake-sensor payloads in log, found ${expected.length}`
    );
  }

  const received = JSON.parse(pubsubMessagesRaw);
  if (!Array.isArray(received)) {
    throw new Error('pubsub messages file is not an array');
  }

  assertDelivery(expected, received);
  console.log(`verified ${expected.length} pubsub messages match fake-sensor payloads`);
}

async function main() {
  const parsed = parseArgs(process.argv.slice(2));
  if (parsed.mode === 'collect') {
    await collectMessages(parsed);
    return;
  }

  await verifyMessages(parsed);
}

main().catch((error) => {
  const message = error instanceof Error ? error.message : String(error);
  console.error(`pubsub verification failed: ${message}`);
  process.exitCode = 1;
});
