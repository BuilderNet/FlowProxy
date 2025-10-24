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

build-reproducible:
  RUSTFLAGS="-C symbol-mangling-version=v0 -C strip=none -C link-arg=-Wl,--build-id=none -C metadata='' --remap-path-prefix $(pwd)=." \
  LC_ALL=C \
  TZ=UTC \
  SOURCE_DATE_EPOCH="$(git log -1 --pretty=%ct)" \
  cargo build --profile reproducible --locked --target x86_64-unknown-linux-gnu

# Provision the database and create the required tables.
provision-db:
  # Create the database if it doesn't exist.
  ./clickhouse client --host $CLICKHOUSE_HOST --user $CLICKHOUSE_USER --secure --password $CLICKHOUSE_PASSWORD --query 'CREATE DATABASE IF NOT EXISTS flowproxy'
  # Create the bundles table.
  ./clickhouse client --host $CLICKHOUSE_HOST --user $CLICKHOUSE_USER --secure --password $CLICKHOUSE_PASSWORD -d flowproxy --queries-file ./fixtures/create_bundles_table.sql

# Drop and recreate the required tables.
reset-db:
  # Drop the bundles table.
  ./clickhouse client --host $CLICKHOUSE_HOST --user $CLICKHOUSE_USER --secure --password $CLICKHOUSE_PASSWORD -d flowproxy --query 'DROP TABLE bundles'
  # Create the bundles table.
  ./clickhouse client --host $CLICKHOUSE_HOST --user $CLICKHOUSE_USER --secure --password $CLICKHOUSE_PASSWORD -d flowproxy --queries-file ./fixtures/create_bundles_table.sql

query := "SELECT * REPLACE(toString(internal_uuid) AS internal_uuid, toString(replacement_uuid) AS replacement_uuid) \
FROM bundles \
WHERE (timestamp >= '2025-09-29 12:26:48.000000') AND (timestamp <= '2025-09-29 12:36:48.000000') \
ORDER BY timestamp ASC \
INTO OUTFILE 'filename' \
FORMAT Parquet
SETTINGS output_format_parquet_string_as_string = 0"

#[confirm("Do you want to extract data into a parquet file?")]
extract-data FILE:
  ./clickhouse client --host $CLICKHOUSE_HOST --user $CLICKHOUSE_USER --secure --password $CLICKHOUSE_PASSWORD -d flowproxy --query "{{replace(query, "filename", FILE)}}"