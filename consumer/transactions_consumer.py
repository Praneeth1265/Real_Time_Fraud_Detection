import json
import asyncio
import math
from datetime import datetime, timedelta
import redis.asyncio as redis
import httpx
import clickhouse_connect
from aiokafka import AIOKafkaConsumer

# ---------------- Config ----------------
KAFKA_BROKER = "kafka:9092"
TOPIC = "transactions"
GROUP_ID = "fraud_detection_group"

REDIS_HOST = "redis"
REDIS_PORT = 6379

CLICKHOUSE_HOST = "clickhouse"
CLICKHOUSE_DB = "fraud"

ML_SERVICE_URL = "http://ml_service:8000/predict"

# Feature engineering constants
EPS = 1e-8
MAX_HOURS_CAP = 720.0
MAX_DISTANCE_CAP = 20000.0
GLOBAL_AVG_AMOUNT = 500.0

SEM = asyncio.Semaphore(50)

# ---------------------------------------


def haversine(lat1, lon1, lat2, lon2):
    R = 6371
    phi1 = math.radians(lat1)
    phi2 = math.radians(lat2)
    dphi = math.radians(lat2 - lat1)
    dlambda = math.radians(lon2 - lon1)
    a = math.sin(dphi / 2) ** 2 + math.cos(phi1) * math.cos(phi2) * math.sin(dlambda / 2) ** 2
    return 2 * R * math.atan2(math.sqrt(a), math.sqrt(1 - a))


async def get_user_stats(r, user_id):
    """Fetch all user aggregations from Redis"""
    user_key = f"user:{user_id}:stats"
    
    data = await r.hgetall(user_key)
    
    if not data:
        return None
    
    # Get time-windowed transaction counts
    tx_key = f"user:{user_id}:tx_times"
    now = datetime.utcnow().timestamp()
    
    tx_count_1h = await r.zcount(tx_key, now - 3600, now)
    tx_count_24h = await r.zcount(tx_key, now - 86400, now)
    
    return {
        'total_amount': float(data.get("total_amount", 0)),
        'total_amount_sq': float(data.get("total_amount_sq", 0)),
        'tx_count': int(data.get("tx_count", 0)),
        'last_lat': float(data["last_lat"]) if "last_lat" in data else None,
        'last_lon': float(data["last_lon"]) if "last_lon" in data else None,
        'last_location': data.get("last_location"),
        'last_timestamp': data.get("last_timestamp"),
        'last_merchant': data.get("last_merchant"),
        'home_location': data.get("home_location"),
        'merchant_counts': json.loads(data.get("merchant_counts", "{}")),
        'location_counts': json.loads(data.get("location_counts", "{}")),
        'tx_count_1h': tx_count_1h,
        'tx_count_24h': tx_count_24h
    }


async def update_user_stats(r, user_id, amount, merchant, location, lat, lon, timestamp):
    """Update all user aggregations in Redis"""
    user_key = f"user:{user_id}:stats"
    
    current = await get_user_stats(r, user_id)
    
    if current is None:
        avg_amount = amount
        std_amount = 0
        home_location = location
        merchant_counts = {merchant: 1}
        location_counts = {location: 1}
        tx_count = 0
        total_amount = 0
        total_amount_sq = 0
    else:
        tx_count = current['tx_count']
        total_amount = current['total_amount']
        total_amount_sq = current['total_amount_sq']
        
        # Running average and std
        new_total = total_amount + amount
        new_count = tx_count + 1
        avg_amount = new_total / new_count
        
        new_total_sq = total_amount_sq + amount ** 2
        variance = (new_total_sq / new_count) - avg_amount ** 2
        std_amount = math.sqrt(max(variance, 0))
        
        merchant_counts = current['merchant_counts']
        merchant_counts[merchant] = merchant_counts.get(merchant, 0) + 1
        
        location_counts = current['location_counts']
        location_counts[location] = location_counts.get(location, 0) + 1
        
        home_location = max(location_counts, key=location_counts.get)
    
    # Update hash
    pipe = r.pipeline()
    pipe.hset(user_key, mapping={
        "total_amount": total_amount + amount,
        "total_amount_sq": total_amount_sq + amount ** 2,
        "tx_count": tx_count + 1,
        "avg_amount": avg_amount,
        "std_amount": std_amount,
        "last_lat": lat,
        "last_lon": lon,
        "last_location": location,
        "last_merchant": merchant,
        "last_timestamp": timestamp.isoformat(),
        "home_location": home_location,
        "merchant_counts": json.dumps(merchant_counts),
        "location_counts": json.dumps(location_counts)
    })
    
    # Add to time-series for velocity tracking
    tx_key = f"user:{user_id}:tx_times"
    pipe.zadd(tx_key, {timestamp.isoformat(): timestamp.timestamp()})
    
    # Clean up old entries (keep only 24 hours)
    cutoff = (timestamp - timedelta(hours=24)).timestamp()
    pipe.zremrangebyscore(tx_key, '-inf', cutoff)
    
    await pipe.execute()


def build_ml_features(tx, user_stats):
    """Build all 29 ML features"""
    
    amount = tx["amount"]
    merchant = tx["merchant"]
    location = tx["location"]
    lat = tx["lat"]
    lon = tx["lon"]
    timestamp = tx["timestamp"]
    
    features = {}
    
    # ============ AMOUNT FEATURES ============
    features['amount'] = amount
    features['amount_log'] = math.log1p(amount)
    
    if user_stats and user_stats['tx_count'] > 5:
        avg = user_stats['total_amount'] / user_stats['tx_count']
        std = math.sqrt(
            (user_stats['total_amount_sq'] / user_stats['tx_count']) - avg ** 2
        )
        std = max(std, 1.0)
        features['amount_zscore'] = (amount - avg) / std
        features['amount_ratio_to_avg'] = amount / (avg + EPS)
    else:
        features['amount_zscore'] = 0
        features['amount_ratio_to_avg'] = amount / (GLOBAL_AVG_AMOUNT + EPS)
    
    features['is_round_amount'] = 1 if (amount % 100 == 0 and amount >= 100) else 0
    features['is_very_high'] = 1 if amount > 5000 else 0
    
    # ============ TIME FEATURES ============
    features['hour'] = timestamp.hour
    features['day_of_week'] = timestamp.weekday()
    features['is_weekend'] = 1 if timestamp.weekday() >= 5 else 0
    features['is_night'] = 1 if (timestamp.hour >= 23 or timestamp.hour <= 5) else 0
    
    if user_stats and user_stats['last_timestamp']:
        last_ts = datetime.fromisoformat(user_stats['last_timestamp'])
        hours_since = (timestamp - last_ts).total_seconds() / 3600
        features['hours_since_last_tx'] = min(hours_since, MAX_HOURS_CAP)
        features['hours_since_last_tx_log'] = math.log1p(hours_since)
        features['is_rapid_tx'] = 1 if hours_since < 0.083 else 0
    else:
        features['hours_since_last_tx'] = MAX_HOURS_CAP
        features['hours_since_last_tx_log'] = math.log1p(MAX_HOURS_CAP)
        features['is_rapid_tx'] = 0
    
    # ============ VELOCITY FEATURES ============
    features['tx_count_1h'] = user_stats['tx_count_1h'] if user_stats else 0
    features['tx_count_24h'] = user_stats['tx_count_24h'] if user_stats else 0
    features['total_tx_count'] = user_stats['tx_count'] if user_stats else 0
    features['is_first_tx'] = 1 if (not user_stats or user_stats['tx_count'] == 0) else 0
    
    # ============ LOCATION FEATURES ============
    if user_stats:
        features['location_mismatch_home'] = 1 if location != user_stats.get('home_location') else 0
        features['location_mismatch_last'] = 1 if location != user_stats.get('last_location') else 0
        
        if user_stats['last_lat'] is not None:
            distance_km = haversine(
                user_stats['last_lat'],
                user_stats['last_lon'],
                lat,
                lon
            )
            features['distance_from_last_tx'] = min(distance_km, MAX_DISTANCE_CAP)
            features['distance_from_last_tx_log'] = math.log1p(distance_km)
            
            if features['hours_since_last_tx'] > 0:
                speed_kmh = distance_km / features['hours_since_last_tx']
                features['travel_speed'] = min(speed_kmh, 10000)
                features['travel_speed_log'] = math.log1p(speed_kmh)
                features['is_impossible_travel'] = 1 if speed_kmh > 800 else 0
            else:
                features['travel_speed'] = 0
                features['travel_speed_log'] = 0
                features['is_impossible_travel'] = 0
        else:
            features['distance_from_last_tx'] = 0
            features['distance_from_last_tx_log'] = 0
            features['travel_speed'] = 0
            features['travel_speed_log'] = 0
            features['is_impossible_travel'] = 0
        
        location_counts = user_stats.get('location_counts', {})
        total_txs = max(user_stats['tx_count'], 1)
        features['location_frequency'] = location_counts.get(location, 0) / total_txs
        features['is_new_location'] = 1 if location not in location_counts else 0
    else:
        features['location_mismatch_home'] = 0
        features['location_mismatch_last'] = 0
        features['distance_from_last_tx'] = 0
        features['distance_from_last_tx_log'] = 0
        features['travel_speed'] = 0
        features['travel_speed_log'] = 0
        features['is_impossible_travel'] = 0
        features['location_frequency'] = 0
        features['is_new_location'] = 1
    
    # ============ MERCHANT FEATURES ============
    high_risk = ["Casino", "Foreign_Site", "Crypto", "Wire_Transfer"]
    medium_risk = ["Luxury_Store", "Electronics_Hub", "Jewelry"]
    
    if merchant in high_risk:
        features['merchant_risk'] = 3
    elif merchant in medium_risk:
        features['merchant_risk'] = 2
    else:
        features['merchant_risk'] = 1
    
    if user_stats:
        merchant_counts = user_stats.get('merchant_counts', {})
        total_txs = max(user_stats['tx_count'], 1)
        features['merchant_frequency'] = merchant_counts.get(merchant, 0) / total_txs
        features['is_new_merchant'] = 1 if merchant not in merchant_counts else 0
    else:
        features['merchant_frequency'] = 0
        features['is_new_merchant'] = 1
    
    return features


async def process_transaction(msg, r, ch, http_client):
    try:
        tx = json.loads(msg.value.decode())
        
        user_id = tx["user_id"]
        amount = tx["amount"]
        merchant = tx["merchant"]
        location = tx["location"]
        lat = tx["lat"]
        lon = tx["lon"]
        timestamp = datetime.fromisoformat(tx["timestamp"])
        
        tx["timestamp"] = timestamp
        
        # Fetch user stats from Redis
        user_stats = await get_user_stats(r, user_id)
        
        # Build ML features
        features = build_ml_features(tx, user_stats)
        
        # Call ML Service
        try:
            resp = await http_client.post(
                ML_SERVICE_URL,
                json=features,
                timeout=1.0
            )
            result = resp.json()
            probability = float(result.get("probability", 0.0))
        except Exception as e:
            print(f"ML service error: {repr(e)}")
            probability = 0.0
        
        # Alert if fraud
        if probability > 0.7:
            print(f"[FRAUD] User {user_id} | Prob: {probability:.3f} | "
                  f"${amount:.2f} | {merchant} | Speed: {features['travel_speed']:.0f}km/h")
        
        # Update Redis AFTER feature extraction (no leakage)
        await update_user_stats(r, user_id, amount, merchant, location, lat, lon, timestamp)
        
        # Sink to ClickHouse (all features)
        ch.insert(
            "transactions",
            [[
                user_id, amount, merchant, location, lat, lon, timestamp,
                features['amount_log'], features['amount_zscore'], features['amount_ratio_to_avg'],
                features['is_round_amount'], features['is_very_high'],
                features['hour'], features['day_of_week'], features['is_weekend'], features['is_night'],
                features['hours_since_last_tx'], features['hours_since_last_tx_log'], features['is_rapid_tx'],
                features['tx_count_1h'], features['tx_count_24h'], features['total_tx_count'], features['is_first_tx'],
                features['location_mismatch_home'], features['location_mismatch_last'],
                features['distance_from_last_tx'], features['distance_from_last_tx_log'],
                features['travel_speed'], features['travel_speed_log'], features['is_impossible_travel'],
                features['location_frequency'], features['is_new_location'],
                features['merchant_risk'], features['merchant_frequency'], features['is_new_merchant'],
                probability
            ]],
            column_names=[
                "user_id", "amount", "merchant", "location", "lat", "lon", "timestamp",
                "amount_log", "amount_zscore", "amount_ratio_to_avg",
                "is_round_amount", "is_very_high",
                "hour", "day_of_week", "is_weekend", "is_night",
                "hours_since_last_tx", "hours_since_last_tx_log", "is_rapid_tx",
                "tx_count_1h", "tx_count_24h", "total_tx_count", "is_first_tx",
                "location_mismatch_home", "location_mismatch_last",
                "distance_from_last_tx", "distance_from_last_tx_log",
                "travel_speed", "travel_speed_log", "is_impossible_travel",
                "location_frequency", "is_new_location",
                "merchant_risk", "merchant_frequency", "is_new_merchant",
                "fraud_probability"
            ]
        )
        
    except Exception as e:
        print(f"Processing error: {e}")
        import traceback
        traceback.print_exc()


async def main():
    # Wait for Redis
    while True:
        try:
            r = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, decode_responses=True)
            await r.ping()
            print("Connected to Redis")
            break
        except Exception as e:
            print(f"Waiting for Redis... {e}")
            await asyncio.sleep(5)
    
    # Wait for ClickHouse
    while True:
        try:
            ch = clickhouse_connect.get_client(
                host=CLICKHOUSE_HOST,
                port=8123,
                database=CLICKHOUSE_DB
            )
            ch.command("SELECT 1")
            print("Connected to ClickHouse")
            break
        except Exception as e:
            print(f"Waiting for ClickHouse... {e}")
            await asyncio.sleep(5)
    
    # Wait for Kafka
    while True:
        try:
            consumer = AIOKafkaConsumer(
                TOPIC,
                bootstrap_servers=KAFKA_BROKER,
                group_id=GROUP_ID,
                auto_offset_reset="earliest"
            )
            await consumer.start()
            print("Connected to Kafka")
            break
        except Exception as e:
            print(f"Waiting for Kafka... {e}")
            await asyncio.sleep(5)
    
    print("Listening for transactions...")
    
    limits = httpx.Limits(max_connections=200, max_keepalive_connections=50)
    timeout = httpx.Timeout(connect=1.0, read=1.0, write=1.0, pool=2.0)

    async with httpx.AsyncClient(limits=limits, timeout=timeout) as http_client:
        try:
            async def guarded_process(msg):
                async with SEM:
                    await process_transaction(msg, r, ch, http_client)

            async for msg in consumer:
                asyncio.create_task(guarded_process(msg))

        finally:
            await consumer.stop()


if __name__ == "__main__":
    asyncio.run(main())
