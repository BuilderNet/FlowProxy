# buildernet-orderflow-proxy-v2
This is the second version of the [BuilderNet](https://buildernet.org) orderflow proxy, designed to be highly performant, reliable and efficient. The proxy is responsible for receiving, validating, prioritizing, and forwarding orderflow to all BuilderNet builders. It is built in Rust and uses the [Tokio](https://tokio.rs) asynchronous runtime.

## CLI
```
cargo run -- --help
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