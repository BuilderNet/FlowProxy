-- DDL to create a ClickHouse table for storing Ethereum bundles data.
CREATE TABLE bundles (
  `time` DateTime64(6),
  `transactions.hash` Array(FixedString(66)),
  `transactions.from` Array(FixedString(42)),
  `transactions.nonce` Array(UInt64),
  `transactions.r` Array(UInt256),
  `transactions.s` Array(UInt256),
  `transactions.v` Array(UInt256),
  `transactions.to` Array(Nullable(FixedString(42))),
  `transactions.gas` Array(UInt128),
  `transactions.type` Array(UInt64),
  `transactions.input` Array(String),
  `transactions.value` Array(UInt256),
  `transactions.gasPrice` Array(Nullable(UInt128)),
  `transactions.maxFeePerGas` Array(Nullable(UInt128)),
  `transactions.maxPriorityFeePerGas` Array(Nullable(UInt128)),
  `transactions.accessList` Array(Nullable(String)),
  `transactions.authorizationList` Array(Nullable(String)),

  `block_number` Nullable(UInt64),
  `min_timestamp` Nullable(UInt64),
  `max_timestamp` Nullable(UInt64),

  `reverting_tx_hashes` Array(FixedString(66)),
  `dropping_tx_hashes` Array(FixedString(66)),
  `refund_tx_hashes` Nullable(Array(FixedString(66))),

  `uuid` Nullable(String),
  `replacement_nonce` Nullable(UInt64),
  `refund_percent` Nullable(UInt8),
  `refund_recipient` Nullable(FixedString(42)),
  `signer_address` Nullable(FixedString(42)),
  `refund_identity` Nullable(FixedString(42)),

  `builder_name` LowCardinality(String) COMMENT 'name of the endpoint, or IP if missing',

  `hash` FixedString(66),
  `timestamp` DateTime64(6) ALIAS time,

  INDEX from_bloom_filter `transactions.from` TYPE bloom_filter GRANULARITY 10,
  INDEX transactions_hash_bloom_filter `transactions.hash` TYPE bloom_filter GRANULARITY 10,
  INDEX hash_bloom_filter hash TYPE bloom_filter GRANULARITY 10,
  INDEX uuid_bloom_filter uuid TYPE bloom_filter GRANULARITY 10,

  CONSTRAINT valid_transactions_hashes CHECK arrayAll(x -> ((x LIKE '0x%') AND (lower(x) = x)), `transactions.hash`),
  CONSTRAINT valid_transactions_from CHECK arrayAll(x -> ((x LIKE '0x%') AND (lower(x) = x)), `transactions.from`),
  CONSTRAINT valid_transactions_input CHECK arrayAll(x -> ((x LIKE '0x%') AND (lower(x) = x)), `transactions.input`),
  CONSTRAINT valid_refund_percent CHECK (refund_percent IS NULL) OR (refund_percent <= 100)
)
ENGINE = ReplicatedMergeTree('/clickhouse/tables/{uuid}/{shard}', '{replica}')
PARTITION BY toYYYYMM(time)
PRIMARY KEY (block_number, time)
ORDER BY (block_number, time)
TTL toDateTime(time) + toIntervalMonth(1) RECOMPRESS CODEC(ZSTD(6))
SETTINGS storage_policy = 'hot_cold', index_granularity = 8192;
