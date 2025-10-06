-- DDL to create a ClickHouse table for storing Ethereum bundles data.
CREATE TABLE bundles (
  `time` DateTime64(6, 'UTC'),
  `timestamp` DateTime64(6, 'UTC') ALIAS time,
  `transactions.hash` Array(FixedString(32)),
  `transactions.from` Array(FixedString(20)),
  `transactions.nonce` Array(UInt64),
  `transactions.r` Array(UInt256),
  `transactions.s` Array(UInt256),
  `transactions.v` Array(UInt8),
  `transactions.to` Array(Nullable(FixedString(20))),
  `transactions.gas` Array(UInt64),
  `transactions.type` Array(UInt8),
  `transactions.input` Array(String),
  `transactions.value` Array(UInt256),
  `transactions.gasPrice` Array(Nullable(UInt128)),
  `transactions.maxFeePerGas` Array(Nullable(UInt128)),
  `transactions.maxPriorityFeePerGas` Array(Nullable(UInt128)),
  `transactions.accessList` Array(Nullable(String)),
  `transactions.authorizationList` Array(Nullable(String)),
  `transactions.raw` Array(String) COMMENT 'RLP-encoded transaction',

  `block_number` Nullable(UInt64),
  `min_timestamp` Nullable(UInt64),
  `max_timestamp` Nullable(UInt64),

  `reverting_tx_hashes` Array(FixedString(32)),
  `dropping_tx_hashes` Array(FixedString(32)),
  `refund_tx_hashes` Array(FixedString(32)),

  `replacement_uuid` Nullable(UUID),
  `replacement_nonce` Nullable(UInt64),
  `refund_percent` Nullable(UInt8),
  `refund_recipient` Nullable(FixedString(20)),
  `refund_identity` Nullable(FixedString(20)),

  `signer_address` Nullable(FixedString(20)),

  `hash` FixedString(32),
  `internal_uuid` UUID,

  `builder_name` LowCardinality(String),
  `version` UInt8,

  INDEX from_bloom_filter `transactions.from` TYPE bloom_filter GRANULARITY 10,
  INDEX transactions_hash_bloom_filter `transactions.hash` TYPE bloom_filter GRANULARITY 10,
  INDEX hash_bloom_filter hash TYPE bloom_filter GRANULARITY 10,
  INDEX internal_uuid_bloom_filter internal_uuid TYPE bloom_filter GRANULARITY 10,
)
ENGINE = ReplicatedMergeTree('/clickhouse/tables/{uuid}/{shard}', '{replica}')
PARTITION BY toYYYYMM(time)
PRIMARY KEY (time)
SETTINGS index_granularity = 8192;
