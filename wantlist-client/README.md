# wantlist-client

This package implements a client for the `go-bitswap` TCP server.

## Configuration

As with other packages, this one also expects some configuration in a `.env` file.
The required keys are:

- `WANTLIST_LOGGING_TCP_ADDRESS` is the TCP endpoint on which the `go-bitswap` TCP server is listening.
    For example `localhost:4321`.
- `WANTLIST_CLIENT_PROMETHEUS_LISTEN_ADDR` is the address on which to server a Prometheus endpoint.
    For example `'127.0.0.1:7654'`

It is recommended to just share the `.env` file between the Go server and the Rust client, so that they always use the
same address.