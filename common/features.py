import math
from datetime import datetime

# These constants and the feature formulas below intentionally mirror
# Syn_dataset/dataset_generator.py exactly, so live features match what the
# model was trained on (avoids train/serve skew).
EPS = 1e-8
MAX_HOURS_CAP = 720.0
MAX_DISTANCE_CAP = 20000.0
RAPID_TX_HOURS = 0.083
IMPOSSIBLE_TRAVEL_KMH = 800
HIGH_RISK_MERCHANTS = {"Casino", "Foreign_Site", "Crypto", "Wire_Transfer"}
MEDIUM_RISK_MERCHANTS = {"Luxury_Store", "Electronics_Hub", "Jewelry"}

FEATURE_COLUMNS = [
    "amount", "amount_log", "amount_zscore", "amount_ratio_to_avg",
    "is_round_amount", "is_very_high",
    "hour", "day_of_week", "is_weekend", "is_night",
    "hours_since_last_tx", "hours_since_last_tx_log", "is_rapid_tx",
    "tx_count_1h", "tx_count_24h", "total_tx_count", "is_first_tx",
    "location_mismatch_home", "location_mismatch_last",
    "distance_from_last_tx", "distance_from_last_tx_log",
    "travel_speed", "travel_speed_log", "is_impossible_travel",
    "location_frequency", "is_new_location",
    "merchant_risk", "merchant_frequency", "is_new_merchant",
]


def haversine(lat1, lon1, lat2, lon2):
    R = 6371  # km
    phi1, phi2 = math.radians(lat1), math.radians(lat2)
    dphi = math.radians(lat2 - lat1)
    dlambda = math.radians(lon2 - lon1)
    a = math.sin(dphi / 2) ** 2 + math.cos(phi1) * math.cos(phi2) * math.sin(dlambda / 2) ** 2
    return 2 * R * math.atan2(math.sqrt(a), math.sqrt(1 - a))


def merchant_risk_level(merchant):
    if merchant in HIGH_RISK_MERCHANTS:
        return 3
    if merchant in MEDIUM_RISK_MERCHANTS:
        return 2
    return 1


def compute_features(r, tx):
    """
    Recomputes the same 29 features as Syn_dataset/dataset_generator.py's
    build_features(), but backed by Redis instead of an in-process dict.
    All state reads below reflect history strictly BEFORE this transaction
    (no data leakage); state is updated at the very end, after features
    are computed.
    """
    user_id = tx["user_id"]
    amount = tx["amount"]
    merchant = tx["merchant"]
    location = tx["location"]
    lat, lon = tx["lat"], tx["lon"]
    curr_time = datetime.fromisoformat(tx["timestamp"])
    epoch = curr_time.timestamp()

    state_key = f"user:{user_id}:state"
    loc_key = f"user:{user_id}:loc_counts"
    merch_key = f"user:{user_id}:merch_counts"
    times_key = f"user:{user_id}:tx_times"

    state = r.hgetall(state_key)
    tx_count = int(state.get("tx_count", 0))
    total_amount = float(state.get("total_amount", 0.0))
    total_amount_sq = float(state.get("total_amount_sq", 0.0))
    last_lat = state.get("last_lat")
    last_lon = state.get("last_lon")
    last_ts = state.get("last_ts")
    last_location = state.get("last_location")
    home_location = state.get("home_location") or location

    # ---------- Amount features ----------
    amount_log = math.log1p(amount)
    if tx_count > 5:
        avg = total_amount / tx_count
        variance = (total_amount_sq / tx_count) - avg ** 2
        std = max(variance, 0) ** 0.5
        amount_zscore = (amount - avg) / max(std, 1.0)
        amount_ratio_to_avg = amount / (avg + EPS)
    else:
        amount_zscore = 0.0
        amount_ratio_to_avg = 1.0

    is_round_amount = int(amount % 100 == 0 and amount >= 100)
    is_very_high = int(amount > 5000)

    # ---------- Time features ----------
    hour = curr_time.hour
    day_of_week = curr_time.weekday()
    is_weekend = int(day_of_week >= 5)
    is_night = int(hour >= 23 or hour <= 5)

    if last_ts is not None:
        # Floored at 0 as a defensive guard against out-of-order arrivals
        # (e.g. clock skew between producer instances) -- log1p() would
        # otherwise raise on a negative delta.
        hours_since_raw = max((epoch - float(last_ts)) / 3600, 0.0)
        hours_since_last_tx = min(hours_since_raw, MAX_HOURS_CAP)
        hours_since_last_tx_log = math.log1p(hours_since_raw)
        is_rapid_tx = int(hours_since_raw < RAPID_TX_HOURS)
    else:
        hours_since_raw = MAX_HOURS_CAP
        hours_since_last_tx = MAX_HOURS_CAP
        hours_since_last_tx_log = math.log1p(MAX_HOURS_CAP)
        is_rapid_tx = 0

    # ---------- Velocity features ----------
    tx_count_1h = r.zcount(times_key, epoch - 3600, epoch)
    tx_count_24h = r.zcount(times_key, epoch - 86400, epoch)
    total_tx_count = tx_count
    is_first_tx = int(tx_count == 0)

    # ---------- Location / travel features ----------
    location_mismatch_home = int(location != home_location)
    location_mismatch_last = int(location != last_location)

    if last_lat is not None:
        distance_km = haversine(float(last_lat), float(last_lon), lat, lon)
        distance_from_last_tx = min(distance_km, MAX_DISTANCE_CAP)
        distance_from_last_tx_log = math.log1p(distance_km)

        if hours_since_last_tx > 0:
            speed_kmh = distance_km / hours_since_last_tx
            travel_speed = min(speed_kmh, 10000)
            travel_speed_log = math.log1p(speed_kmh)
            is_impossible_travel = int(speed_kmh > IMPOSSIBLE_TRAVEL_KMH)
        else:
            travel_speed = 0.0
            travel_speed_log = 0.0
            is_impossible_travel = 0
    else:
        distance_from_last_tx = 0.0
        distance_from_last_tx_log = 0.0
        travel_speed = 0.0
        travel_speed_log = 0.0
        is_impossible_travel = 0

    total_txs = max(tx_count, 1)
    loc_count = int(r.hget(loc_key, location) or 0)
    location_frequency = loc_count / total_txs
    is_new_location = int(loc_count == 0)

    # ---------- Merchant features ----------
    merchant_risk = merchant_risk_level(merchant)
    merch_count = int(r.hget(merch_key, merchant) or 0)
    merchant_frequency = merch_count / total_txs
    is_new_merchant = int(merch_count == 0)

    features = {
        "amount": amount,
        "amount_log": amount_log,
        "amount_zscore": amount_zscore,
        "amount_ratio_to_avg": amount_ratio_to_avg,
        "is_round_amount": is_round_amount,
        "is_very_high": is_very_high,
        "hour": hour,
        "day_of_week": day_of_week,
        "is_weekend": is_weekend,
        "is_night": is_night,
        "hours_since_last_tx": hours_since_last_tx,
        "hours_since_last_tx_log": hours_since_last_tx_log,
        "is_rapid_tx": is_rapid_tx,
        "tx_count_1h": tx_count_1h,
        "tx_count_24h": tx_count_24h,
        "total_tx_count": total_tx_count,
        "is_first_tx": is_first_tx,
        "location_mismatch_home": location_mismatch_home,
        "location_mismatch_last": location_mismatch_last,
        "distance_from_last_tx": distance_from_last_tx,
        "distance_from_last_tx_log": distance_from_last_tx_log,
        "travel_speed": travel_speed,
        "travel_speed_log": travel_speed_log,
        "is_impossible_travel": is_impossible_travel,
        "location_frequency": location_frequency,
        "is_new_location": is_new_location,
        "merchant_risk": merchant_risk,
        "merchant_frequency": merchant_frequency,
        "is_new_merchant": is_new_merchant,
    }

    # ---------- Update state AFTER feature extraction ----------
    r.zadd(times_key, {f"{epoch}:{user_id}": epoch})
    r.zremrangebyscore(times_key, "-inf", epoch - 86400)
    r.expire(times_key, 86400)

    r.hincrby(loc_key, location, 1)
    r.hincrby(merch_key, merchant, 1)
    updated_loc_counts = r.hgetall(loc_key)
    new_home_location = max(updated_loc_counts, key=lambda k: int(updated_loc_counts[k]))

    r.hset(state_key, mapping={
        "tx_count": tx_count + 1,
        "total_amount": total_amount + amount,
        "total_amount_sq": total_amount_sq + amount ** 2,
        "last_lat": lat,
        "last_lon": lon,
        "last_ts": epoch,
        "last_location": location,
        "last_merchant": merchant,
        "home_location": new_home_location,
    })

    return features
