# IPFS Public Gateway Finder

Finds overlay addresses of public IPFS gateways through probing their HTTP side with crafted content.
Prints results to STDOUT in JSON or CSV format, logs to STDERR.
Requires the [metric-exporter-plugin](https://github.com/trudi-group/ipfs-metric-exporter).

## Usage

```
USAGE:
    ipfs-gateway-finder [FLAGS] [OPTIONS]

FLAGS:
    -h, --help       Prints help information
        --csv        Whether to produce CSV output (instead of the default JSON output)
    -V, --version    Prints version information

OPTIONS:
        --amqp-server-addr <ADDRESS>      The address of the AMQP server to connect to for real-time data. Including
                                          scheme amqp or amqps (TLS). [default: amqp://localhost:5672/%2f]
        --gateway-list <URL>              The URL of the JSON gateway list to use. Supported schemes are http, https,
                                          and file for local data [default:
                                          https://raw.githubusercontent.com/ipfs/public-
                                          gateway-checker/master/src/gateways.json]
        --http-tries <NUMBER OF TRIES>    The number of times the HTTP request to a gateway should be tried [default:
                                          10]
        --http-timeout <SECONDS>          The request timeout in seconds for HTTP requests to a gateway [default: 60]
        --monitor-api-addr <ADDRESS>      The address of the HTTP IPFS API of the monitor [default: localhost:5001]
        --monitor-name <NAME>             The name the monitor uses for data on the AMQP server [default: local]
```

Configure `amqp-server-addr`, `monitor-api-addr`, and `monitor-name` according to your own setup.
See also the configuration of the [plugin](https://github.com/trudi-group/ipfs-metric-exporter).

## Installation

Compiling this with an up-to-date stable Rust should work, like so:
```
cargo build --release
```
However, it is recommended to build the entire [workspace](../README.md), because that contains a `Cargo.lock` manifest, which pins dependencies to specific versions.
Building the entire workspace in docker is also a good option.

## Running

In order to run this, you'll need an IPFS node with public connectivity and the [metric-exporter-plugin](https://github.com/trudi-group/ipfs-metric-exporter) installed **and configured**.
Additionally, you'll need an AMQP server (RabbitMQ, for example) to broker the messages between plugin and client.

You can control the level of logging using the `RUST_LOG` environment variable, like so:
```
RUST_LOG="ipfs_gateway_finder=debug" ipfs-gateway-finder <options>
```

Using 10 HTTP tries and timeouts of 60 seconds should usually work.
Adding new content on IPFS and having it discovered by others is slow, so increase tries and timeouts if things are not working.
Also, running the tool at regular intervals is recommended, to discover unstable gateways etc.

## License

MIT, see [../LICENSE](../LICENSE).