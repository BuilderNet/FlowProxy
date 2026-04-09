CREATE TABLE transactions_from_send_raw_tx
(
    `received_at` DateTime64(3,
 'UTC'),
    `hash` String,
    `chain_id` String,
    `tx_type` Int64,
    `from` String,
    `to` String,
    `value` String,
    `nonce` String,
    `gas` String,
    `gas_price` String,
    `gas_tip_cap` String,
    `gas_fee_cap` String,
    `data_size` Int64,
    `data_4bytes` String,
    `raw_tx` String,
    `ver` Int64 MATERIALIZED -toUnixTimestamp(received_at)
)
ENGINE = ReplacingMergeTree(ver)
PARTITION BY toYYYYMM(received_at)
PRIMARY KEY hash
ORDER BY hash
SETTINGS index_granularity = 8192
COMMENT 'Transaction details,
 deduplicated by hash,
 will keep the transaction with earliest received_at.';