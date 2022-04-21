# bitswap-monitoring-client

This package implements a client for the IPFS Bitswap monitoring TCP server.
It reads and processes messages from multiple monitors and outputs various metrics via prometheus.

See also [the plugin](https://github.com/scriptkitty/ipfs-metric-exporter).

## Configuration

Configuration is done via a YAML configuration file.
The location of the configuration file can be specified with the `--config` parameter, it defaults to `config.yaml`.
This is an example config file, see also the [file](./config.yaml) and the [implementation](./src/config.rs):

```yaml
# This is a config file for the wantlist-client tool.
monitors:
  - name: "DE1"
    address: "10.0.1.5:4321"
  - name: "DE2"
    address: "10.0.1.2:4321"
prometheus_address: "0.0.0.0:8080"
```

Each monitor is configured with a name and the remote endpoint to connect to.
The `prometheus_address` specifies the local endpoint to listen and serve Prometheus metrics on.

## Metrics

Metrics are provided via a Prometheus HTTP endpoint.

### `wantlist_messages_received`

A counter that tracks the number of wantlist messages received by monitor.
The IPFS instrumentation emits JSON objects that are either a connection event or a wantlist message -- this tracks only wantlist messages.
A message may contain multiple entries.

### `wantlist_entries_received`

A counter that tracks the number of wantlist entries received by monitor, message type, and `send_dont_have` flag.

### `connection_events`

A counter that tracks the number of connection events by monitor, event type, and whether there was a ledger entry present for the peer.
