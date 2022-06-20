# bitswap-monitoring-client

This package implements a client for the IPFS Bitswap monitoring TCP server.
It reads and processes messages from multiple monitors and outputs various metrics via prometheus.
It also uses [MaxMind's GeoLite2 database](https://dev.maxmind.com/geoip/geolite2-free-geolocation-data) to geolocate requests.

See also [the plugin](https://github.com/wiberlin/ipfs-metric-exporter).

## Configuration

Configuration is done via a YAML configuration file.
The location of the configuration file can be specified with the `--config` parameter, it defaults to `config.yaml`.
This is an example config file, see also the [file](./config.yaml) and the [implementation](./src/config.rs):

```yaml
# This is a config file for the bitswap-monitoring-client tool.

# Address to listen and serve prometheus metrics on.
prometheus_address: "0.0.0.0:8080"

# Specifies the path to the MaxMind GeoLite databases.
# Defaults to /usr/local/share/GeoIP if unspecified.
#geoip_database_path: "/usr/local/share/GeoIP"

# List of monitors to connect to.
monitors:
  - name: "DE1"
    address: "10.0.1.5:4321"
  - name: "DE2"
    address: "10.0.1.2:4321"
```

Each monitor is configured with a name and the remote endpoint to connect to.
The `prometheus_address` specifies the local endpoint to listen and serve Prometheus metrics on.

## Metrics

Metrics are provided via a Prometheus HTTP endpoint.
All metrics contain fields for the origin `monitor` (which is configured with the `name` field of the configuration file) as well as the `origin_country` of the logged event.
Metrics for origin countries are created on the fly, if any events from that country are logged.
There are two special countries `Unknown` and `Error`, indicating whether we were unable to determine an origin for an event, or whether GeoIP lookup failed with an error.
Multiaddresses containing a P2P circuit, i.e., relayed connections, are ignored and `Unknown` is used for their origin country.
See also the [implementation](./src/prom.rs).

### `bitswap_messages_received`

A counter that counts incoming Bitswap messages.
The IPFS instrumentation emits JSON objects that are either a connection event or an incoming Bitswap message.
This tracks incoming Bitswap messages, and counts whether they contain a wantlist.

### `bitswap_blocks_received`

A counter that counts how many blocks were received via Bitswap.

### `bitswap_block_presences_received`

A counter that counts how many block presence indications were received via Bitswap, by the `presence_type` (`have` or `dont_have`).

### `wantlist_messages_received`

A counter that tracks the number of Bitswap messages received for which the `want_list` was not empty, by whether the wantlist was `full` or incremental.
A message may contain multiple entries.

### `wantlist_entries_received`

A counter that tracks the number of wantlist entries received by `entry_type`, and `send_dont_have` flag.
Entry types are `want_block`, `want_have`, and `cancel`.
The `send_dont_have` flag is only valid for requests (i.e., it has no meaning for `cancel` entries).

### `connection_events_(connected|disconnected)`

Counters that track the number of connection or disconnection events.
