# IPFS Monitoring Network Size Estimator

A tool to estimate the size of the IPFS network from neighbors of monitoring nodes with unlimited connection capacity.
This is a client for the HTTP API offered by our [plugin](https://github.com/trudi-group/ipfs-metric-exporter).
Sizes are estimated using two methodologies:
A hypergeometric estimator, which is applicable to any pair of monitors,
and an estimator based on the coupon-collectors problem, which is applicable to any set of monitors, but is simplified to work on the mean population size observed by all of them.

For details on the algorithms and reasoning behind them, read our [paper](https://arxiv.org/abs/2104.09202).

Sizes are estimated for a) the entire network and b) the number of peers supporting specific protocols.
The latter is done by first extracting all protocol strings advertised by connected peers.
The population samples of the two monitors are then filtered based on whether they support a protocol.
Finally, an estimate is computed based on these filtered samples.

_It is important to note that all estimators assume **uniform samples** of peers._
Specifically, this means that estimates for the size of the Kademlia DHT are **probably inaccurate**, because DHT connections are not a uniform sample of the total peer population.
There are DHT crawlers to estimate the size of the DHT.

## Configuration

Configuration is done via a YAML configuration file.
The location of the configuration file can be specified with the `--config` parameter, it defaults to `config.yaml`.
This is an example config file, see also the [file](./config.yaml) and the [implementation](./src/config.rs):

```yaml
# This is a config file for the monitoring-size-estimator tool.

# Address to listen and serve prometheus metrics on.
prometheus_address: "0.0.0.0:8088"

# The interval to sleep between estimates, in seconds.
sample_interval_seconds: 30

# Monitors to connect to.
monitors:
  - plugin_api_address: "http://daemon01:8432"
    name: "daemon01"
  - plugin_api_address: "http://daemon02:8432"
    name: "daemon02"
```

The `prometheus_address` specifies the local endpoint to listen and serve Prometheus metrics on.
The `monitors` section describes the monitors to connect to.
The `plugin_api_address` is the API address of the plugin, not of the kubo node.

## Metrics

Metrics are provided via a Prometheus HTTP endpoint.

For each estimation algorithm, results are given for the various protocols offered by connected peers.
The empty protocol `""` denotes that _all_ connected peers were taken into consideration for the estimate.

See also the [implementation](./src/prom.rs).
