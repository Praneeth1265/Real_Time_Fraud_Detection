import json
import asyncio
import math
from datetime import datetime
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
# ---------------------------------------


# -------- Distance Formula (Haversine) --------
def haversine(lat1, lon1, lat2, lon2):
    R = 6371  # Earth radius in km

    phi1 = math.radians(lat1)
    phi2 = math.radians(lat2)
    dphi = math.radians(lat2 - lat1)
    dlambda = math.radians(lon2 - lon1)

    a = math.sin(dphi/2)**2 + \
        math.cos(phi1) * math.cos(phi2) * math.sin(dlambda/2)**2

    return 2 * R * math.atan2(math.sqrt(a), math.sqrt(1 - a))
# ----------------------------------------------

async def process_transaction(msg, r, ch, http_client):
    try:
        tx = json.loads(msg.value.decode())

        user_id = tx["user_id"]
        amount = tx["amount"]
        location = tx["location"]
        lat = tx["lat"]
        lon = tx["lon"]
        ts = datetime.fromisoformat(tx["timestamp"])

        user_key = f"user:{user_id}"

        # -------- Fetch historical state --------
        data = await r.hgetall(user_key)

        total_amount = float(data.get("total_amount", 0))
        tx_count = int(data.get("tx_count", 0))

        last_lat = data.get("last_lat")
        last_lon = data.get("last_lon")
        last_ts = data.get("last_timestamp")
        last_location = data.get("last_location")

        # -------- Feature engineering --------
        MAX_HOURS_CAP = 168.0       # 1 week
        MAX_DISTANCE_CAP = 2000.0   # km
        GLOBAL_AVG_AMOUNT = 500.0   # baseline
        EPS = 1e-6

	# ---------- FIRST TX FLAG ----------
        is_first_tx = 1 if tx_count == 0 else 0

	# ---------- AVERAGE (NO LEAKAGE) ----------
        avg_amount = (
            total_amount / tx_count
            if tx_count > 0
            else GLOBAL_AVG_AMOUNT
        )

	# ---------- LOCATION MISMATCH ----------
        location_mismatch = int(
            last_location is not None and last_location != location
        )

	# ---------- TIME DELTA ----------
        if last_ts:
            delta = (ts - datetime.fromisoformat(last_ts)).total_seconds() / 3600
            hours_since_last_tx = min(max(delta, 1/3600), MAX_HOURS_CAP)
        else:
            hours_since_last_tx = MAX_HOURS_CAP / 2  # neutral

	# ---------- DISTANCE ----------
        if last_lat and last_lon:
            distance_from_last_tx = haversine(
		float(last_lat), float(last_lon), lat, lon
	    )
        else:
            distance_from_last_tx = MAX_DISTANCE_CAP / 2  # neutral

        distance_from_last_tx = min(distance_from_last_tx, MAX_DISTANCE_CAP)

        new_total = total_amount + amount
        new_count = tx_count + 1

        # -------- Update Redis state --------
        pipe = r.pipeline()
        pipe.hset(user_key, mapping={
            "total_amount": new_total,
            "tx_count": new_count,
            "last_lat": lat,
            "last_lon": lon,
            "last_location": location,
            "last_timestamp": ts.isoformat()
        })
        await pipe.execute()

        # -------- Build ML features --------
        features = {
	    "amount": amount,
	    "avg_amount": amount / (avg_amount + EPS),
	    "hours_since_last_tx": hours_since_last_tx / MAX_HOURS_CAP,
	    "distance_from_last_tx": math.log1p(distance_from_last_tx) / math.log1p(MAX_DISTANCE_CAP),
	    "location_mismatch": location_mismatch,
	    "is_first_tx": is_first_tx
	}


        # -------- Call ML Service (ASYNC, NON-BLOCKING) --------
        try:
            resp = await http_client.post(
                ML_SERVICE_URL,
                json=features,
                timeout=1.0
            )
            result = resp.json()
            probability = float(result.get("probability", 0.0))
        except Exception as e:
            print("ML service error TYPE:", type(e))
            print("ML service error REPR:", repr(e))
            probability = 0.0   # safe fallback

        # -------- Decision --------
        if probability > 0.7:
            print(f"[ALARM] User {user_id} | Prob {probability:.2f} | "
                  f"Dist {distance_from_last_tx:.1f} km | "
                  f"Δt {hours_since_last_tx:.2f} h")
        else:
            print(f"[OK] User {user_id} | Prob {probability:.2f}")

        # -------- Sink to ClickHouse --------
        ch.insert(
            "transactions",
            [[
                user_id,
                amount,
                location,
                lat,
                lon,
                ts,
                avg_amount,
                distance_from_last_tx,
                hours_since_last_tx,
                location_mismatch,
                is_first_tx,
                probability
            ]],
            column_names=[
                "user_id", "amount", "location", "lat", "lon", "timestamp",
                "avg_amount", "distance_from_last_tx", "hours_since_last_tx",
                "location_mismatch","is_first_tx", "fraud_probability"
            ]
        )

    except Exception as e:
        print("Processing error:", e)


# ---------------- MAIN LOOP ----------------
async def main():
    # -------- Wait for Redis --------
    while True:
        try:
            r = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, decode_responses=True)
            await r.ping()
            print("Connected to Redis")
            break
        except Exception as e:
            print("Waiting for Redis...", e)
            await asyncio.sleep(5)

    # -------- Wait for ClickHouse --------
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
            print("Waiting for ClickHouse...", e)
            await asyncio.sleep(5)

    # -------- Wait for Kafka --------
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
            print("Waiting for Kafka...", e)
            await asyncio.sleep(5)

    print("Listening for transactions...")

    async with httpx.AsyncClient() as http_client:
        try:
            async for msg in consumer:
                # concurrent processing
                asyncio.create_task(
                    process_transaction(msg, r, ch, http_client)
                )
        finally:
            await consumer.stop()



if __name__ == "__main__":
    asyncio.run(main())

