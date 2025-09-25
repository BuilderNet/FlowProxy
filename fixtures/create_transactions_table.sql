-- DDL to create a ClickHouse table for storing private transactions data.
CREATE TABLE transactions (
  `time` DateTime64(6, 'UTC'),
  `timestamp` DateTime64(6, 'UTC') alias time,
  `hash` FixedString(32),
  `from` FixedString(20),
  `nonce` UInt64,
  `r` String,
  `s` String,
  `v` UInt8,
  `to` Nullable(FixedString(20)),
  `gas` UInt64,
  `tx_type` UInt64,
  `input` String,
  `value` String,
  `gas_price` Nullable(UInt128),
  `max_fee_per_gas` Nullable(UInt128),
  `max_priority_fee_per_gas` Nullable(UInt128),
  `access_list` Nullable(String),
  `builder_name` LowCardinality(String),

  INDEX from_bloom_filter `from` TYPE bloom_filter GRANULARITY 10,
  INDEX transaction_hash_bloom_filter `hash` TYPE bloom_filter GRANULARITY 10,
)
ENGINE = ReplicatedMergeTree('/clickhouse/tables/{uuid}/{shard}', '{replica}')
PARTITION BY toYYYYMM(time)
PRIMARY KEY (time)
ORDER BY (time)
TTL toDateTime(time) + toIntervalMonth(1) RECOMPRESS CODEC(ZSTD(6))
SETTINGS index_granularity = 8192;
