# IPFS Public Gateway Finder

Finds overlay addresses of public IPFS gateways through probing their HTTP side with crafted content.

## Installation

Compiling this with an up-to-date stable Rust should work, like so:
```
cargo build --release
```
However, it is recommended to build the entire [workspace](../README.md), because that contains a `Cargo.lock` manifest, which pins dependencies to specific versions.
Building the entire workspace in docker is also a good option.

## Running

In order to run this, you'll need an IPFS node with public connectivity and the [metric-exporter-plugin](https://github.com/trudi-group/ipfs-metric-exporter) installed **and configured**.
Start the IPFS node with the plugin, watch for something like `Metric Export TCP server listening on 127.0.0.1:8181`, which shows the plugin working and exposing logging via TCP.

You can control the level of logging using the `RUST_LOG` environment variable, like so:
```
RUST_LOG="ipfs_gateway_finder=debug" ipfs-gateway-finder <options>
```

Usage:
```
USAGE:
    ipfs-gateway-finder [FLAGS] [OPTIONS]

FLAGS:
    -h, --help       Prints help information
        --csv        Whether to produce CSV output (instead of the default JSON output)
    -V, --version    Prints version information

OPTIONS:
        --monitor_logging_addr <ADDRESS>    The address of the bitswap monitor to connect to [default: localhost:8181]
        --gateway_list <URL>                The URL of the JSON gateway list to use [default: https://raw.githubusercontent.com/ipfs/public-gateway-checker/master/src/gateways.json]
        --http_tries <NUMBER OF TRIES>      The number of times the HTTP request to a gateway should be tried [default: 10]
        --http_timeout <SECONDS>            The request timeout in seconds for HTTP requests to a gateway [default: 60]
        --monitor_api_addr <ADDRESS>        The address of the HTTP IPFS API of the monitor [default: localhost:5001]
```

Remember to set `monitor_api_addr` and `monitor_logging_addr` according to your own setup!
By default, IPFS runs its API on `localhost:5001`.
You need to configure the metric export plugin to log via TCP.

Using 10 HTTP tries and timeouts of 60 seconds should usually work.
Adding new content on IPFS and having it discovered by others is slow, so increase tries and timeouts if things are not working.

## License

MIT, see [../LICENSE](../LICENSE).