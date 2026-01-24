CREATE DATABASE IF NOT EXISTS fraud;
USE fraud;

CREATE TABLE IF NOT EXISTS transactions
(
    user_id UInt32,
    amount Float64,
    location String,

    lat Float64,
    lon Float64,

    timestamp DateTime,

    avg_amount Float64,
    distance_from_last_tx Float64,
    hours_since_last_tx Float64,
    location_mismatch UInt8,
    is_first_tx UInt8,
    fraud_probability Float64
)
ENGINE = MergeTree
ORDER BY (user_id, timestamp);

