-- DDL to create a ClickHouse table for storing Ethereum bundles data.
CREATE TABLE bundles (
  `time` DateTime64(6),
  `timestamp` DateTime64(6) ALIAS time,
  `transactions.hash` Array(FixedString(32)),
  `transactions.from` Array(FixedString(20)),
  `transactions.nonce` Array(UInt64),
  `transactions.r` Array(UInt256),
  `transactions.s` Array(UInt256),
  `transactions.v` Array(UInt8),
  `transactions.to` Array(Nullable(FixedString(20))),
  `transactions.gas` Array(UInt64),
  `transactions.type` Array(UInt64),
  `transactions.input` Array(String),
  `transactions.value` Array(FixedString(32)),
  `transactions.gasPrice` Array(Nullable(UInt128)),
  `transactions.maxFeePerGas` Array(Nullable(UInt128)),
  `transactions.maxPriorityFeePerGas` Array(Nullable(UInt128)),
  `transactions.accessList` Array(Nullable(String)),
  `transactions.authorizationList` Array(Nullable(String)),

  `block_number` Nullable(UInt64),
  `min_timestamp` Nullable(UInt64),
  `max_timestamp` Nullable(UInt64),

  `reverting_tx_hashes` Array(FixedString(32)),
  `dropping_tx_hashes` Array(FixedString(32)),
  `refund_tx_hashes` Array(FixedString(32)),

  `uuid` Nullable(UUID),
  `replacement_uuid` Nullable(UUID),
  `replacement_nonce` Nullable(UInt64),
  `refund_percent` Nullable(UInt8),
  `refund_recipient` Nullable(FixedString(20)),
  `signer_address` Nullable(FixedString(20)),
  `refund_identity` Nullable(FixedString(20)),

  `builder_name` LowCardinality(String) COMMENT 'name of the endpoint, or IP if missing',

  `hash` FixedString(32),

  
  INDEX from_bloom_filter `transactions.from` TYPE bloom_filter GRANULARITY 10,
  INDEX transactions_hash_bloom_filter `transactions.hash` TYPE bloom_filter GRANULARITY 10,
  INDEX hash_bloom_filter hash TYPE bloom_filter GRANULARITY 10,
  INDEX uuid_bloom_filter uuid TYPE bloom_filter GRANULARITY 10,
  
  -- For bundles: `uuid` should be set.
  -- For replacement bundles with transactions: `uuid` and `replacement_uuid` should be set.
  -- For replacement bundles without transactions (a.k.a. "cancellations"): only `replacement_uuid` should be set.
  -- So this is the invariant we want to enforce:
  CONSTRAINT valid_uuid_or_replacement CHECK (uuid IS NOT NULL OR replacement_uuid IS NOT NULL)
)
ENGINE = ReplicatedMergeTree('/clickhouse/tables/{uuid}/{shard}', '{replica}')
PARTITION BY toYYYYMM(time)
PRIMARY KEY (time)
ORDER BY (time)
TTL toDateTime(time) + toIntervalMonth(1) RECOMPRESS CODEC(ZSTD(6))
SETTINGS index_granularity = 8192;
