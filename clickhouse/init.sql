CREATE DATABASE IF NOT EXISTS fraud;

USE fraud;

CREATE TABLE IF NOT EXISTS transactions
(
    user_id Int32,
    amount Float64,
    merchant String,
    location String,
    lat Float64,
    lon Float64,
    timestamp DateTime,
    
    -- Amount features
    amount_log Float64,
    amount_zscore Float64,
    amount_ratio_to_avg Float64,
    is_round_amount UInt8,
    is_very_high UInt8,
    
    -- Time features
    hour UInt8,
    day_of_week UInt8,
    is_weekend UInt8,
    is_night UInt8,
    hours_since_last_tx Float64,
    hours_since_last_tx_log Float64,
    is_rapid_tx UInt8,
    
    -- Velocity features
    tx_count_1h UInt16,
    tx_count_24h UInt16,
    total_tx_count UInt32,
    is_first_tx UInt8,
    
    -- Location features
    location_mismatch_home UInt8,
    location_mismatch_last UInt8,
    distance_from_last_tx Float64,
    distance_from_last_tx_log Float64,
    travel_speed Float64,
    travel_speed_log Float64,
    is_impossible_travel UInt8,
    location_frequency Float64,
    is_new_location UInt8,
    
    -- Merchant features
    merchant_risk UInt8,
    merchant_frequency Float64,
    is_new_merchant UInt8,
    
    -- Prediction
    fraud_probability Float64,
    
    -- Metadata
    processed_at DateTime DEFAULT now()
)
ENGINE = MergeTree()
ORDER BY (timestamp, user_id)
PARTITION BY toYYYYMM(timestamp);
