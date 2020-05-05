# ipfs-resolver

Umbrella project for all things related to resolving and analyzing IPFS blocks with Rust.

Most of the sub-projects have their own README to explain some things in more detail.

## Sub-projects

### `common`

This library package holds basic building blocks used in all other packages, most of all logging and very basic types.

### `db`

This library package deals with all things db-related.
Specifically, it holds the schemas, types, and functions used by all other packages to interact with the database.

### `db-exporter`

This binary package implements a small tool that tracks the number of records in the database and exports them via
Prometheus.

### `resolver`

This binary package produces the actual resolver.
It interacts with an IPFS node via HTTP, parses IPFS blocks' protobuf, and finally puts the results in a database.

### `wantlist-client`

This binary package implements a TCP client to the TCP server implemented in `go-bitswap`.
This makes it possible to receive and process wantlist messages in real-time.
Additionally, the binary runs a prometheus server to publish metrics about the number of messages received.

## Building

You'll need the latest stable Rust.
You'll also need `protoc`, the protocol buffer compiler, from Google, somewhere on your `PATH`.
Then just:

```
cargo build --release
```

This will take a while for the first build.
Also, this will build all sub-projects, which then end up in the `target/` directory of the root project.

## Configuration

All of the packages are configured via environment variables/`.env` files.
The packages contain README files that detail their configuration, and an example, complete `.env` file is given in
[.env](.env).
