# ipfs-resolver-db-exporter

This is a small binary that connects to the database and publishes the number of records for some tables on Prometheus.

## Configuration

This tool expects configuration in a `.env` file, specifically these keys:

- `DB_EXPORTER_PROMETHEUS_LISTEN_ADDR` is the address to run a prometheus server on.
    For example `'127.0.0.1:4545'`.