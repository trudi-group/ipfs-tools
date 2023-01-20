# ipfs-tools

Umbrella project for all things related to monitoring and analyzing IPFS stuff with Rust.

Most of the subprojects have their own README to explain some things in more detail.

## Sub-projects

### `cid-decode`

A binary that reads many CIDs and prints various counts about them.

### `common`

This library package holds basic building blocks used in all other packages, most of all logging and very basic types.
This also contains the code for simulating the BitSwap engine.

### `ipfs-gateway-finder`

This is a binary that identifies public gateways on the overlay network.
It downloads the list of public gateways off GitHub, crafts CIDs, queries them, and listens for BitSwap messages for 
these CIDs.

### `ipfs-json-to-csv`

This is a binary tool to convert logged BitSwap messages and connection events to CSV data to be analyzed in R.
It tracks connection durations and simulates the BitSwap engine.

### `ipfs-monitoring-plugin-client`

A library package implementing a client to our [monitoring plugin](https://github.com/trudi-group/ipfs-metric-exporter).
This provides TCP as well as HTTP functionality.

### `bitswap-monitoring-client`

This binary package implements a real-time analysis client for Bitswap messages.
Additionally, the binary runs a prometheus server to publish metrics about the message stream analysed.

### `unify-bitswap-traces`

This binary is used to unify traces from multiple monitors into CSV files for processing in R.
This is the tool used for [this paper](https://arxiv.org/abs/2104.09202).

### `monitoring-size-estimator`

A binary that connects to multiple monitors via our [monitoring plugin](https://github.com/trudi-group/ipfs-metric-exporter) to estimate the size of the network.

## Building

### Dependencies

We currently build with `rustls`, so no dependencies are required.

### Docker

There is a multi-stage build setup implemented with Docker.
The [builder stage](./Dockerfile.builder) compiles the binaries and caches dependencies, for faster incremental builds.
This should produce an image named `ipfs-tools-builder`, which is then used in the runner stages.
The runner stage(s) copy out compiled artifacts from `ipfs-tools-builder` and set up a minimal runtime environment.
There is a [build-docker-images.sh](./build-docker-images.sh) script that builds all these images.
There is also a [build-in-docker.sh](./build-in-docker.sh) script that builds the builder and copies out the artifacts to the `out/` directory of the project.

Docker builds against the current stable Rust on Debian Bullseye.
This gives us an older-ish libc, which improves compatibility with older-ish systems (Ubuntu LTS, for example) at no loss of functionality.

### Manually

You'll need the latest stable Rust.
Then execute, in the topmost directory:

```
cargo build --release --locked
```

This will take a while for the first build.
Also, this will build all subprojects, which then end up in the `target/` directory of the root project.

## Configuration

Most of the packages are configured via environment variables/`.env` files or CLI parameters.
The packages sometimes contain README files that detail their configuration, and an example, mostly complete `.env` file is given in
[.env](.env).

## Running

You can control the level of logging using the `RUST_LOG` environment variable.
Running with `RUST_BACKTRACE=1` is also a good idea, makes it easier for me to debug :)