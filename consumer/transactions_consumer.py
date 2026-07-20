import json
import math
from datetime import datetime
from fastapi import FastAPI
from pydantic import BaseModel

app = FastAPI()

# ---------------- State ----------------
user_state = {}

# ---------------- Utils ----------------
def haversine(lat1, lon1, lat2, lon2):
    R = 6371  # km
    phi1, phi2 = math.radians(lat1), math.radians(lat2)
    dphi = math.radians(lat2 - lat1)
    dlambda = math.radians(lon2 - lon1)

    a = math.sin(dphi / 2)**2 + \
        math.cos(phi1) * math.cos(phi2) * math.sin(dlambda / 2)**2
    return 2 * R * math.atan2(math.sqrt(a), math.sqrt(1 - a))


class Tx(BaseModel):
    user_id: int
    lat: float
    lon: float
    timestamp: str


@app.post("/predict")
def predict(tx: Tx):

    user_id = tx.user_id
    curr_time = datetime.fromisoformat(tx.timestamp)

    # ---------- First transaction ----------
    if user_id not in user_state:
        user_state[user_id] = {
            "lat": tx.lat,
            "lon": tx.lon,
            "time": curr_time
        }

        features = {
            "distance_from_last_tx": 0.0,
            "distance_from_last_tx_log": 0.0,
            "travel_speed": 0.0,
            "travel_speed_log": 0.0,
            "is_impossible_travel": 0,
            "hours_since_last_tx": 720.0  # cold start
        }

        print(features)
        return {"fraud": 0}

    # ---------- Fetch previous state ----------
    prev = user_state[user_id]

    hours = max(
        (curr_time - prev["time"]).total_seconds() / 3600,
        1e-3
    )

    distance = haversine(
        prev["lat"], prev["lon"],
        tx.lat, tx.lon
    )

    speed = distance / hours

    # ---------- Flags ----------
    is_impossible = int(speed > 900)  # km/h (jet-level)

    features = {
        "distance_from_last_tx": distance,
        "distance_from_last_tx_log": math.log1p(distance),
        "travel_speed": speed,
        "travel_speed_log": math.log1p(speed),
        "is_impossible_travel": is_impossible,
        "hours_since_last_tx": hours
    }

    # ---------- UPDATE STATE AFTER COMPUTE ----------
    user_state[user_id] = {
        "lat": tx.lat,
        "lon": tx.lon,
        "time": curr_time
    }

    print(features)

    # your model.predict(features) here
    return {"fraud": int(is_impossible)}

