# bitswap-discovery-probe

This package implements a client for the IPFS real-time monitoring setup and Bitswap broadcast probing plugin.
It asks multiple monitors to broadcast a request for a list of CIDs and processes the incoming responses.

See also [the plugin](https://github.com/trudi-group/ipfs-metric-exporter).

## Configuration

Configuration is done via a YAML configuration file.
The location of the configuration file can be specified with the `--config` parameter, it defaults to `config.yaml`.
This is an example config file, see also the [file](./config.yaml) and the [implementation](./src/config.rs):

```yaml
# This is a config file for the bitswap-discovery-probe tool.
monitors:
  - name: "local"
    amqp_server_address: "amqp://localhost:5672/%2f"
    api_base_url: "http://localhost:8432"
prometheus_address: "0.0.0.0:8080"
cancel_after_seconds: 30
wait_after_cancel_seconds: 30
cids:
  - "<cid 1>"
  - ...
```

Each monitor is configured with a name and the remote endpoints to connect to.
The name must be the same as is used on the AMQP server for logging.
This is configured via [the plugin](https://github.com/trudi-group/ipfs-metric-exporter).
