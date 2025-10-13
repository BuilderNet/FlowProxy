-- DDL to create a ClickHouse table for storing bundle receipts data.
CREATE TABLE bundle_receipts (
  `bundle_hash` FixedString(32),
  `sent_at` Nullable(DateTime64(6, 'UTC')) COMMENT 'The time the bundle has been sent at, as present in the JSON-RPC header',
  `received_at` DateTime64(6, 'UTC') COMMENT 'The time the local operator has received the payload',
  `dst_builder_name` LowCardinality(String) COMMENT 'The operator that received the payload',
  `src_builder_name` LowCardinality(String) COMMENT 'The name of the operator which sent us the payload',
  `payload_size` UInt32,
  `priority` LowCardinality(String), -- Or UInt 8?

  INDEX bundle_hash_bloom_filter bundle_hash TYPE bloom_filter GRANULARITY 10,
)
ENGINE = MergeTree()
PARTITION BY toYYYYMM(received_at)
PRIMARY KEY (received_at)
ORDER BY (received_at)
SETTINGS index_granularity = 8192;
