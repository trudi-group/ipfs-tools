# ipfs-resolver

Queries a local IPFS node for blocks, then analyzes and saves them to a database.

## Running

As with the database, this package expects configuration in a `.env` file.
Specifically, these keys need to be set:

- `IPFS_RESOLVER_API_URL` is the base URL for the IPFS node to use.
    Typically, this would be something like `http://localhost:5001/`.
- `IPFS_RESOLVER_TIMEOUT_SECS` sets the timeout in seconds to pass on to the IPFS node when asking for block data.
    This is passed to every method call through the IPFS API, but practically only applies to the first call, because
    block data is usually cached after that.

To run, `ipfs-resolver` expects just one argument: A CID.
So, for example:

```
ipfs-resolver QmPZ9gcCEpqKTo6aq61g2nXGUhM4iCL3ewB6LDXZCtioEB
```

If you want less logging, try setting `RUST_LOG` to something higher than `debug`, which is the default:

```
RUST_LOG=warn ipfs-resolver QmPZ9gcCEpqKTo6aq61g2nXGUhM4iCL3ewB6LDXZCtioEB
```
