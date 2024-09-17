# bitswap-monitoring-client

This package implements a client for the IPFS Bitswap monitoring TCP server.
It reads and processes messages from multiple monitors and outputs various metrics via prometheus.
It also uses [MaxMind's GeoLite2 database](https://dev.maxmind.com/geoip/geolite2-free-geolocation-data) to geolocate requests.

See also [the plugin](https://github.com/trudi-group/ipfs-metric-exporter).

## Configuration

Configuration is done via a YAML configuration file.
The location of the configuration file can be specified with the `--config` parameter, it defaults to `config.yaml`.
This is an example config file, see also the [file](./config.yaml) and the [implementation](./src/config.rs):

```yaml
# This is a config file for the bitswap-monitoring-client tool.

# Address to listen and serve prometheus metrics on.
prometheus_address: "0.0.0.0:8088"

# Specifies the path to the MaxMind GeoLite databases.
# Defaults to /usr/local/share/GeoIP if unspecified.
#geoip_database_path: "/usr/local/share/GeoIP"

# Specifies the path to a public gateway ID file.
# Each line in the file should contain one peer ID.
# If not provided all traffic will be logged as non-gateway-traffic.
# Defaults to empty, i.e., no tagging of gateway traffic.
#gateway_file_path: "/usr/local/share/gateways.txt"

# Specifies a path to a directory to write JSON logs.
# A subdirectory per monitor will be created.
# If not provided, logging to disk will be disabled.
#disk_logging_directory: "traces"

# List of AMQP data sources to connect to.
amqp_servers:
  # Address of the AMQP server, using amqp or amqps (TLS transport) scheme.
  - amqp_server_address: "amqp://localhost:5672/%2f"
    # A list of monitors to subscribe to via this data source.
    monitor_names:
      - "local"
```

The `prometheus_address` specifies the local endpoint to listen and serve Prometheus metrics on.
For each (`amqp_server`, `monitor_name`) combination, a connection to the AMQP server will be opened.

### Docker

When running in docker via [../Dockerfile.bitswap-monitoring-client](../Dockerfile.bitswap-monitoring-client),
the client is started via the [docker-entrypoint.sh](docker-entrypoint.sh) script.
This looks for the environment variables `PUID` and `PGID`, `chown`s the logging directory, and drops root for the
given UID and GID.

## To-Disk Logging

If enabled via `disk_logging_directory`, the client writes logs as gzipped JSON files into the configured directory.
A subdirectory per monitor will be created, log files are rotated hourly.
While being written to, the files are named `<timestamp>.json.gz.tmp`.
Once rotated or on shutdown, files are renamed to `<timestamp>.json.gz`.
The client listens for `SIGINT` and `SIGTERM` to shut down, and finalizes the currently-opened file.

## Metrics

Metrics are provided via a Prometheus HTTP endpoint.
All metrics contain at least these labels:
- `monitor` for the origin (which is configured with the `name` field of the configuration file)
- `origin_country`, as determined via geolocating the first potential address for a peer, and
- `origin_is_gateway`, if a list of gateway IDs was supplied and the peer ID matches.

Metrics for origin countries are created on the fly, if any events from that country are logged.
There are two special countries `Unknown` and `Error`, indicating whether we were unable to determine an origin for an event, or whether GeoIP lookup failed with an error.
Multiaddresses containing a P2P circuit, i.e., relayed connections, are ignored and `Unknown` is used for their origin country.

Public gateway status is determined by matching the origin peer ID of an event to a list of known public gateway IDs.
This list is built using the [gateway-finder tool](../ipfs-gateway-finder) and can be hot-reloaded by sending `SIGUSR1` to the monitoring client.
See also the [implementation](./src/prom.rs).

### `bitswap_messages_received`

A counter that counts incoming Bitswap messages.
The IPFS instrumentation emits JSON objects that are either a connection event or an incoming Bitswap message.
This tracks incoming Bitswap messages.

### `bitswap_blocks_received`

A counter that counts how many blocks were received via Bitswap.
These are not requests, but responses received via Bitswap.
A message may contain multiple blocks.

### `bitswap_block_presences_received`

A counter that counts how many block presence indications were received via Bitswap, by the `presence_type` (`have` or `dont_have`).
These are not requests, but responses received via Bitswap.
A message may contain multiple block presences.

### `wantlists_received`

A counter that tracks the number of Bitswap `want_list`s received, by whether the wantlist was `full` or incremental.
A message may contain one or no wantlist, e.g., if it is a response message.
A wantlist may contain multiple entries, which are tracked separately.

### `wantlist_entries_received`

A counter that tracks the number of wantlist entries received by `entry_type`, and `send_dont_have` flag.
Entry types are `want_block`, `want_have`, and `cancel`.
The `send_dont_have` flag is only valid for requests (i.e., it has no meaning for `cancel` entries).
Wantlist entries are individual requests, for blocks or block presences.

### `connection_events_(connected|disconnected)`

Counters that track the number of connection or disconnection events.
