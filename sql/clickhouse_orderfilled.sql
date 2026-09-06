CREATE DATABASE IF NOT EXISTS poly_orderfilled;

CREATE TABLE IF NOT EXISTS poly_orderfilled.orderfilled_fact
(
    tx_hash String CODEC(ZSTD(3)),
    log_index UInt32 CODEC(Delta, ZSTD(3)),
    market_id UInt64 CODEC(Delta, ZSTD(3)),
    condition_id String CODEC(ZSTD(3)),
    token_id String CODEC(ZSTD(3)),
    outcome_code UInt8 CODEC(ZSTD(3)),
    maker String CODEC(ZSTD(3)),
    taker String CODEC(ZSTD(3)),
    side_code UInt8 CODEC(ZSTD(3)),
    price Decimal(20, 10) CODEC(ZSTD(3)),
    size Decimal(30, 10) CODEC(ZSTD(3)),
    block_number UInt64 CODEC(Delta, ZSTD(3)),
    order_hash String CODEC(ZSTD(3)),
    contract LowCardinality(String) CODEC(ZSTD(3)),
    maker_amount Nullable(UInt64) CODEC(ZSTD(3)),
    taker_amount Nullable(UInt64) CODEC(ZSTD(3)),
    fee Nullable(UInt64) CODEC(ZSTD(3)),
    ingested_at DateTime DEFAULT now() CODEC(Delta, ZSTD(3)),
    INDEX idx_tx_hash tx_hash TYPE bloom_filter(0.01) GRANULARITY 64,
    INDEX idx_maker maker TYPE bloom_filter(0.01) GRANULARITY 64,
    INDEX idx_taker taker TYPE bloom_filter(0.01) GRANULARITY 64
)
ENGINE = MergeTree
PARTITION BY intDiv(block_number, 1000000)
ORDER BY (market_id, token_id, block_number, log_index, tx_hash)
SETTINGS index_granularity = 8192;
