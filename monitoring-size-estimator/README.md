# IPFS Monitoring Network Size Estimator

A tool to estimate the size of the IPFS network from neighbors of monitoring nodes with unlimited connection capacity.
This is a client for the HTTP API offered by our [plugin](https://github.com/trudi-group/ipfs-metric-exporter).
Sizes are estimated using two methodologies:
A hypergeometric estimator, which is applicable to any pair of monitors,
and an estimator based on the coupon-collectors problem, which is applicable to any set of monitors, but is simplified to work on the mean population size observed by all of them.

For details on the algorithms and reasoning behind them, read our [paper](https://arxiv.org/abs/2104.09202).

Sizes are estimated for:
- the entire network
- the number of peers supporting specific protocols
- the number of peers running a given agent version.
The latter two are obtained by filtering the samples of the monitors for peers with a given agent version or supporting a given protocol.
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
sample_interval_seconds: 60

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
The `name`s provided are used to label metrics for the hypergeometric estimator.

## Metrics

Metrics are provided via a Prometheus HTTP endpoint.
For each estimation algorithm, results are given for the various protocols offered by connected peers, as well as different agent versions.
The empty protocol `""` denotes that _all_ connected peers were taken into consideration for the estimate.
Similarly, the empty agent version `""` indicates that _all_ connected peers were taken into consideration.
Thus, the estimate given for the empty protocol and empty agent version is an estimate of the total network size.

#### `monitoring_size_estimator_hypergeom_size_estimate`

This records estimates obtained via the hypergeometric estimator.
The estimator can only operate on pairs of monitors, which are indicated via the `monitor_pair` label.
The other labels are `agent_version` and `protocol`.

#### `monitoring_size_estimator_coupon_size_estimate`

This records estimates obtained via the coupon collector estimator.
Metrics are labeled with `agent_version` and `protocol`.

See also the [implementation](./src/prom.rs).
