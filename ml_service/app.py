from fastapi import FastAPI
from pydantic import BaseModel
import random

app = FastAPI()

class Features(BaseModel):
    amount: float
    avg_amount: float
    distance_from_last_tx: float
    hours_since_last_tx: float
    location_mismatch: int
    is_first_tx: int

@app.post("/predict")
def predict(f: Features):
    prob = min(1.0, max(0.0,
        0.3 * (f.amount / (f.avg_amount + 1)) +
        0.3 * (f.distance_from_last_tx / 100) +
        0.2 * f.location_mismatch +
        0.2 * (1 / (f.hours_since_last_tx + 1))
    ))

    return {"probability": float(prob)}

