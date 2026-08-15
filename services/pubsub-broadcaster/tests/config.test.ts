import { describe, expect, it } from 'vitest';
import { loadPubsubBroadcasterConfig } from '../src/config.js';

describe('pubsub broadcaster config', () => {
  it('parses empty reserved peers as an empty array', () => {
    const config = loadPubsubBroadcasterConfig({
      PUBSUB_RESERVED_PEERS: '',
    });

    expect(config.reservedPeers).toEqual([]);
  });

  it('parses a single reserved peer', () => {
    const config = loadPubsubBroadcasterConfig({
      PUBSUB_RESERVED_PEERS: '/ip4/127.0.0.1/tcp/4001/p2p/12D3KooWPeer1',
    });

    expect(config.reservedPeers).toEqual([
      '/ip4/127.0.0.1/tcp/4001/p2p/12D3KooWPeer1',
    ]);
  });

  it('parses and trims multiple reserved peers', () => {
    const config = loadPubsubBroadcasterConfig({
      PUBSUB_RESERVED_PEERS:
        ' /ip4/127.0.0.1/tcp/4001/p2p/12D3KooWPeer1 , , /dns4/node.local/tcp/4001/p2p/12D3KooWPeer2 ',
    });

    expect(config.reservedPeers).toEqual([
      '/ip4/127.0.0.1/tcp/4001/p2p/12D3KooWPeer1',
      '/dns4/node.local/tcp/4001/p2p/12D3KooWPeer2',
    ]);
  });
});
