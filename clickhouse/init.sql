CREATE DATABASE IF NOT EXISTS fraud;

USE fraud;

-- Point-lookup-optimized: the evaluation service does WHERE transaction_id = ?
-- very frequently (once per ground-truth message). transaction_id must be the
-- leading ORDER BY column, or every lookup scans the whole table. No
-- time-based PARTITION BY -- nothing here queries by time range.
CREATE TABLE IF NOT EXISTS predictions
(
    transaction_id    String,
    timestamp         DateTime,
    merchant          String,
    amount            Float64,
    fraud_probability Float64,
    predicted_label   UInt8,
    processed_at      DateTime DEFAULT now()
)
ENGINE = MergeTree()
ORDER BY (transaction_id);

-- Time-range-optimized: Grafana only does WHERE $__timeFilter(timestamp)
-- GROUP BY ... aggregates against this table.
CREATE TABLE IF NOT EXISTS evaluation
(
    transaction_id     String,
    timestamp          DateTime,
    merchant           String,
    amount             Float64,
    predicted_label    UInt8,
    actual_label       UInt8,
    fraud_probability  Float64,
    correct_prediction UInt8,
    classification     LowCardinality(String),   -- 'TP' / 'TN' / 'FP' / 'FN'
    evaluated_at        DateTime DEFAULT now()
)
ENGINE = MergeTree()
ORDER BY (timestamp, transaction_id)
PARTITION BY toYYYYMM(timestamp);
