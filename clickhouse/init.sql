CREATE DATABASE IF NOT EXISTS fraud;

CREATE TABLE IF NOT EXISTS fraud.transactions
(
    user_id UInt32,
    amount Float64,
    location String,
    timestamp DateTime
)
ENGINE = MergeTree
ORDER BY (user_id, timestamp);
