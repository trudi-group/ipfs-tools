# IPFS Public Gateway Finder

Finds overlay addresses of public IPFS gateways through probing their HTTP side with crafted content.

## Installation

Compiling this with an up-to-date stable Rust should work.
In order to run it, you'll need an IPFS node with public connectivity and the [metric-exporter-plugin](https://github.com/scriptkitty/ipfs-metric-exporter) installed.

## Usage

```
USAGE:
    ipfs-gateway-finder [FLAGS] [OPTIONS]

FLAGS:
    -h, --help       Prints help information
        --csv        Whether to produce CSV output (instead of the default JSON output)
    -V, --version    Prints version information

OPTIONS:
        --monitor_logging_addr <ADDRESS>    The address of the bitswap monitor to connect to [default: localhost:4321]
        --gateway_list <URL>                The URL of the JSON gateway list to use [default: https://raw.githubusercontent.com/ipfs/public-gateway-checker/master/src/gateways.json]
        --http_tries <NUMBER OF TRIES>      The number of times the HTTP request to a gateway should be tried [default: 10]
        --http_timeout <SECONDS>            The request timeout in seconds for HTTP requests to a gateway [default: 60]
        --monitor_api_addr <ADDRESS>        The address of the HTTP IPFS API of the monitor [default: localhost:5003]
```

## License

MIT, see [../LICENSE](../LICENSE).