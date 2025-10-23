# buildernet-orderflow-proxy-v2
This is the second version of the [BuilderNet](https://buildernet.org) orderflow proxy, designed to be highly performant, reliable and efficient. The proxy is responsible for receiving, validating, prioritizing, and forwarding orderflow to all BuilderNet builders. It is built in Rust and uses the [Tokio](https://tokio.rs) asynchronous runtime.

## CLI
```
Usage: buildernet-orderflow-proxy [OPTIONS] --user-listen-url <USER_LISTEN_ADDR> --system-listen-url <SYSTEM_LISTEN_ADDR> --builder-name <BUILDERNET_NODE_NAME>

Options:
      --user-listen-url <USER_LISTEN_ADDR>
          Listen URL for receiving user flow [env: USER_LISTEN_ADDR=]
      --system-listen-url <SYSTEM_LISTEN_ADDR>
          Listen URL for receiving system flow [env: SYSTEM_LISTEN_ADDR=]
      --builder-listen-url <BUILDER_LISTEN_ADDR>
          Listen URL for receiving builder stats [env: BUILDER_LISTEN_ADDR=]
      --builder-url <BUILDER_ENDPOINT>
          The URL of the local builder. This should be set in production [env: BUILDER_ENDPOINT=]
      --builder-ready-endpoint <BUILDER_READY_ENDPOINT>
          The endpoint to check if the local builder is ready [env: BUILDER_READY_ENDPOINT=] [default: http://127.0.0.1:6070]
      --builder-name <BUILDERNET_NODE_NAME>
          The name of the local builder [env: BUILDERNET_NODE_NAME=]
      --builder-hub-url <BUILDERHUB_ENDPOINT>
          The URL of BuilderHub [env: BUILDERHUB_ENDPOINT=]
      --metrics <METRICS_ADDR>
          Enable Prometheus metrics. The metrics will be served at the given interface and port [env: METRICS_ADDR=]
      --orderflow-signer <FLASHBOTS_ORDERFLOW_SIGNER>
          The orderflow signer of this proxy [env: FLASHBOTS_ORDERFLOW_SIGNER]
      --flashbots-signer <FLASHBOTS_ORDERFLOW_SIGNER_ADDRESS>
          The flashbots signer of this proxy [env: FLASHBOTS_ORDERFLOW_SIGNER_ADDRESS=]
      --max-request-size <MAX_REQUEST_SIZE>
          The maximum request size in bytes [default: 10485760]
      --max-txs-per-bundle <MAX_TXS_PER_BUNDLE>
          The maximum number of raw transactions per bundle [default: 100]
      --enable-rate-limiting
          Enable rate limiting
      --rate-limit-lookback-s <RATE_LIMIT_LOOKBACK_S>
          Number of seconds to look back for ratelimit computation [default: 1]
      --rate-limit-count <RATE_LIMIT_COUNT>
          Max number of requests sent per rolling `--ratelimit-lookback-s` window, per IP [default: 500]
      --score-lookback-s <SCORE_LOOKBACK_S>
          Number of seconds to look back for score computation [default: 60]
      --score-bucket-s <SCORE_BUCKET_S>
          The number of seconds in one scoring bucket [default: 4]
      --disable-forwarding
          Disable forwarding to peers (useful for testing)
      --log.json
          Outputs logs in JSON format if enabled [env: LOG_JSON=]
      --http.enable-gzip
          Flag indicating whether GZIP support is enabled
      --order-cache.ttl <ORDER_CACHE_TTL>
          The order cache TTL in seconds [default: 24]
      --order-cache.size <ORDER_CACHE_SIZE>
          The order cache size [default: 4096]
      --signer-cache.ttl <SIGNER_CACHE_TTL>
          The signer cache TTL in seconds [default: 36]
      --signer-cache.size <SIGNER_CACHE_SIZE>
          The signer cache size [default: 16384]
      --indexer.clickhouse.host <CLICKHOUSE_HOST>
          [env: CLICKHOUSE_HOST]
      --indexer.clickhouse.username <CLICKHOUSE_USERNAME>
          [env: CLICKHOUSE_USERNAME]
      --indexer.clickhouse.password <CLICKHOUSE_PASSWORD>
          [env: CLICKHOUSE_PASSWORD]
      --indexer.clickhouse.database <CLICKHOUSE_DATABASE>
          [env: CLICKHOUSE_DATABASE=]
      --indexer.clickhouse.bundles-table-name <CLICKHOUSE_BUNDLES_TABLE_NAME>
          The clickhouse table name to store bundles data [env: CLICKHOUSE_BUNDLES_TABLE_NAME=] [default: bundles]
      --indexer.clickhouse.bundle-receipts-table-name <CLICKHOUSE_BUNDLE_RECEIPTS_TABLE_NAME>
          The clickhouse table name to store bundle receipts data [env: CLICKHOUSE_BUNDLE_RECEIPTS_TABLE_NAME=] [default: bundle_receipts]
      --indexer.clickhouse.backup.memory-max-size-bytes <CLICKHOUSE_BACKUP_MEMORY_SIZE_BYTES>
          The maximum size in bytes for the in-memory backup in case of of disk-backup failure, for a certain data type (bundles or bundle receipts). Defaults to 1GiB [env: CLICKHOUSE_BACKUP_MEMORY_SIZE_BYTES=] [default: 1073741824]
      --indexer.clickhouse.backup.disk-database-path <CLICKHOUSE_BACKUP_DISK_DATABASE_PATH>
          The path of the (redb) database used to store failed clickhouse commits for retry. If not set, a default path of `~/.buildernet-of-proxy/clickhouse-backup.db` will be used [env: CLICKHOUSE_BACKUP_DISK_DATABASE_PATH=] [default: /Users/mempirate/.buildernet-orderflow-proxy/clickhouse_backup.db]
      --indexer.clickhouse.backup.disk-max-size-bytes <CLICKHOUSE_BACKUP_DISK_MAX_SIZE_BYTES>
          The maximum size in bytes for the disk-backed backup database. If the database exceeds this size, new entries will not be added until space is freed. Defaults to 10GiB [env: CLICKHOUSE_BACKUP_DISK_MAX_SIZE_BYTES=] [default: 10737418240]
      --indexer.parquet.bundle-receipts-file-path <PARQUET_BUNDLE_RECEIPTS_FILE_PATH>
          The file path to store bundle receipts data [env: PARQUET_BUNDLE_RECEIPTS_FILE_PATH=]
  -h, --help
          Print help
  -V, --version
          Print version
```

## Repository Structure
- [`src/`](src/): Source code.
- [`tests/`](tests/): Integration & e2e tests.
- [`fixtures/`](fixtures/): Clickhouse database fixtures for indexing orders.
- [`benches/`](benches/): Criterion benchmarks for performance testing.
- [`simulation/`](simulation/): Simulation harness with [Shadow](https://shadow.github.io/) for testing the proxy at scale.

## Provisioning Clickhouse
Install the Clickhouse client:
```bash
curl https://clickhouse.com/ | sh
```

Copy the example environment variables and fill in the values:
```bash
cp .env.example .env
```

Then you can use the `just` commands to provision the database and create the tables:
```bash
just provision-db
```

```bash
just reset-db
```

```bash
just extract-data "filename.parquet"
```