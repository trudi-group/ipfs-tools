# ipfs-resolver

Queries a local IPFS node for blocks, then analyzes and saves them to a database.

## Building

You'll need the latest stable Rust.
You'll also need `protoc`, the protocol buffer compiler, from Google, somewhere on your `PATH`.
Then just:

```
cargo build --release
```

## Running

The target IPFS instance's API endpoint is currently hardcoded to be `localhost:5002`.
`ipfs-resolver` expects just one argument: A CID.
So, for example:

```
ipfs-resolver QmPZ9gcCEpqKTo6aq61g2nXGUhM4iCL3ewB6LDXZCtioEB
```

If you want less logging, try setting `RUST_LOG` to something higher than `debug`, which is the default:

```
RUST_LOG=warn ipfs-resolver QmPZ9gcCEpqKTo6aq61g2nXGUhM4iCL3ewB6LDXZCtioEB
```
