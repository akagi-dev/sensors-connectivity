import { createServer, type Server } from 'node:http';

export interface IpfsPublisherMetrics {
  batchCount: number;
  pinCount: number;
  pinLatencyMs: number;
  pinLatencyTotalMs: number;
  retryCount: number;
  dlqCount: number;
}

/** Creates mutable state for the service's operational metrics. */
export function createIpfsPublisherMetrics(): IpfsPublisherMetrics {
  return {
    batchCount: 0,
    pinCount: 0,
    pinLatencyMs: 0,
    pinLatencyTotalMs: 0,
    retryCount: 0,
    dlqCount: 0
  };
}

/** Renders service metrics in the Prometheus text format. */
export function renderIpfsPublisherMetrics(
  metrics: Readonly<IpfsPublisherMetrics>
): string {
  return [
    '# HELP ipfs_publisher_batches_total Batches accepted for processing.',
    '# TYPE ipfs_publisher_batches_total counter',
    `ipfs_publisher_batches_total ${metrics.batchCount}`,
    '# HELP ipfs_publisher_pin_latency_ms Last successful IPFS pin latency in milliseconds.',
    '# TYPE ipfs_publisher_pin_latency_ms gauge',
    `ipfs_publisher_pin_latency_ms ${metrics.pinLatencyMs}`,
    '# HELP ipfs_publisher_pin_latency_ms_sum Total successful IPFS pin latency in milliseconds.',
    '# TYPE ipfs_publisher_pin_latency_ms_sum counter',
    `ipfs_publisher_pin_latency_ms_sum ${metrics.pinLatencyTotalMs}`,
    '# HELP ipfs_publisher_pin_latency_ms_count Successful IPFS pins measured.',
    '# TYPE ipfs_publisher_pin_latency_ms_count counter',
    `ipfs_publisher_pin_latency_ms_count ${metrics.pinCount}`,
    '# HELP ipfs_publisher_retries_total Retried IPFS operations.',
    '# TYPE ipfs_publisher_retries_total counter',
    `ipfs_publisher_retries_total ${metrics.retryCount}`,
    '# HELP ipfs_publisher_dlq_total Batches acknowledged by the DLQ producer.',
    '# TYPE ipfs_publisher_dlq_total counter',
    `ipfs_publisher_dlq_total ${metrics.dlqCount}`
  ].join('\n');
}

/** Starts the service health and Prometheus metrics HTTP endpoints. */
export function startIpfsPublisherHealthServer(
  metrics: IpfsPublisherMetrics,
  port: number
): Server {
  const server = createServer((request, response) => {
    if (request.url === '/health') {
      response.statusCode = 200;
      response.setHeader('content-type', 'application/json; charset=utf-8');
      response.end(JSON.stringify({ status: 'ok' }));
      return;
    }

    if (request.url === '/metrics') {
      response.statusCode = 200;
      response.setHeader('content-type', 'text/plain; version=0.0.4');
      response.end(renderIpfsPublisherMetrics(metrics));
      return;
    }

    response.statusCode = 404;
    response.end('not found');
  });

  server.listen({ host: '0.0.0.0', port });
  return server;
}
