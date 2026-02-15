from fastapi import FastAPI
from pydantic import BaseModel
import xgboost as xgb
import json
import uvicorn
import time


app = FastAPI()

# Load model at startup
model = xgb.Booster()
model.load_model('fraud_model.json')

# Load feature columns and metadata
with open('model_metadata.json', 'r') as f:
    metadata = json.load(f)
    feature_columns = metadata['feature_columns']
    optimal_threshold = metadata.get('optimal_threshold', 0.5)

print(f"Model loaded with {len(feature_columns)} features")
print(f"Optimal threshold: {optimal_threshold}")


class Transaction(BaseModel):
    amount: float
    amount_log: float
    amount_zscore: float
    amount_ratio_to_avg: float
    is_round_amount: int
    is_very_high: int
    hour: int
    day_of_week: int
    is_weekend: int
    is_night: int
    hours_since_last_tx: float
    hours_since_last_tx_log: float
    is_rapid_tx: int
    tx_count_1h: int
    tx_count_24h: int
    total_tx_count: int
    is_first_tx: int
    location_mismatch_home: int
    location_mismatch_last: int
    distance_from_last_tx: float
    distance_from_last_tx_log: float
    travel_speed: float
    travel_speed_log: float
    is_impossible_travel: int
    location_frequency: float
    is_new_location: int
    merchant_risk: int
    merchant_frequency: float
    is_new_merchant: int


@app.post("/predict")
async def predict(tx: Transaction):
    start = time.perf_counter()
    
    features = [
        tx.amount, tx.amount_log, tx.amount_zscore, tx.amount_ratio_to_avg,
        tx.is_round_amount, tx.is_very_high,
        tx.hour, tx.day_of_week, tx.is_weekend, tx.is_night,
        tx.hours_since_last_tx, tx.hours_since_last_tx_log, tx.is_rapid_tx,
        tx.tx_count_1h, tx.tx_count_24h, tx.total_tx_count, tx.is_first_tx,
        tx.location_mismatch_home, tx.location_mismatch_last,
        tx.distance_from_last_tx, tx.distance_from_last_tx_log,
        tx.travel_speed, tx.travel_speed_log, tx.is_impossible_travel,
        tx.location_frequency, tx.is_new_location,
        tx.merchant_risk, tx.merchant_frequency, tx.is_new_merchant
    ]
    
    # Create DMatrix for XGBoost
    dmatrix = xgb.DMatrix([features], feature_names=feature_columns)
    
    # Get prediction (returns probability for class 1)
    probability = float(model.predict(dmatrix)[0])
    prediction = 1 if probability >= optimal_threshold else 0
    
    
    return {
        "probability": probability,
        "prediction": prediction,
        "threshold": optimal_threshold,
    }


@app.get("/health")
async def health():
    return {"status": "healthy", "model_loaded": True}


if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=8000)
