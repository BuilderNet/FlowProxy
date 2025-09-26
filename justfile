set dotenv-load

# display a help message about available commands
default:
  @just --list --unsorted

clippy:
  cargo clippy --all-targets --all-features -- -D warnings

fmt:
  rustup toolchain install nightly-2025-09-21 --component rustfmt > /dev/null 2>&1 && \
  cd {{justfile_directory()}} && \
  cargo +nightly-2025-09-21 fmt

test:
  cargo nextest run --lib --bins --no-tests=warn --retries 3

# Provision the database and create the required tables.
provision-db:
  # Create the database if it doesn't exist.
  ./clickhouse client --host $CLICKHOUSE_HOST --user $CLICKHOUSE_USER --secure --password $CLICKHOUSE_PASSWORD --query 'CREATE DATABASE IF NOT EXISTS buildernet_orderflow_proxy'
  # Create the bundles table.
  ./clickhouse client --host $CLICKHOUSE_HOST --user $CLICKHOUSE_USER --secure --password $CLICKHOUSE_PASSWORD -d buildernet_orderflow_proxy --queries-file ./fixtures/create_bundles_table.sql

# Drop and recreate the required tables.
reset-db:
  # Drop the bundles table.
  ./clickhouse client --host $CLICKHOUSE_HOST --user $CLICKHOUSE_USER --secure --password $CLICKHOUSE_PASSWORD -d buildernet_orderflow_proxy --query 'DROP TABLE bundles'
  # Create the bundles table.
  ./clickhouse client --host $CLICKHOUSE_HOST --user $CLICKHOUSE_USER --secure --password $CLICKHOUSE_PASSWORD -d buildernet_orderflow_proxy --queries-file ./fixtures/create_bundles_table.sql

query := "SELECT * REPLACE(toString(internal_uuid) AS internal_uuid, toString(replacement_uuid) AS replacement_uuid) \
FROM bundles \
WHERE (timestamp >= '2025-09-26 12:32:00.000000') AND (timestamp <= '2025-09-26 12:42:00.000000') \
INTO OUTFILE 'filename' \
FORMAT Parquet"

[confirm("Do you want to extract data into a parquet file?")]
extract-data FILE:
  ./clickhouse client --host $CLICKHOUSE_HOST --user $CLICKHOUSE_USER --secure --password $CLICKHOUSE_PASSWORD -d buildernet_orderflow_proxy --query "{{replace(query, "filename", FILE)}}"