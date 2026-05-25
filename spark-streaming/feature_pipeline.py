"""
Real-Time Feature Engineering Pipeline for Spark Streaming
==========================================================
Computes streaming features from raw transaction events for
fraud detection model inference.

Features computed:
- Transaction amount statistics (log, z-score)
- Time-based features (hour, night flag)
- Behavioral features (rolling avg, velocity)
- Geographic anomaly detection
"""

import math
from datetime import datetime


def haversine_distance(lat1, lon1, lat2, lon2):
    """
    Calculate the great-circle distance between two points
    on Earth using the Haversine formula.
    Returns distance in kilometers.
    """
    R = 6371  # Earth's radius in km
    phi1, phi2 = math.radians(lat1), math.radians(lat2)
    dphi = math.radians(lat2 - lat1)
    dlambda = math.radians(lon2 - lon1)

    a = (math.sin(dphi / 2) ** 2 +
         math.cos(phi1) * math.cos(phi2) * math.sin(dlambda / 2) ** 2)
    c = 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a))
    return R * c


def compute_streaming_features(txn: dict, cardholder_history: dict = None) -> dict:
    """
    Compute features for a single transaction event in the streaming context.

    Parameters
    ----------
    txn : dict
        Raw transaction event from Kafka.
    cardholder_history : dict or None
        Accumulated history for the cardholder, containing:
        - recent_amounts: list of recent transaction amounts
        - recent_timestamps: list of recent transaction timestamps
        - last_lat, last_lon: coordinates of the last transaction

    Returns
    -------
    dict : Feature dictionary ready for model inference.
    """
    features = {}

    # --- Amount Features ---
    amount = txn.get("amount", 0.0)
    features["amount"] = amount
    features["log_amount"] = math.log1p(amount)
    features["is_high_value"] = 1 if amount > 500 else 0
    features["is_very_high_value"] = 1 if amount > 2000 else 0

    # --- Time Features ---
    ts_ms = txn.get("timestamp", 0)
    dt = datetime.fromtimestamp(ts_ms / 1000) if ts_ms > 0 else datetime.now()
    features["hour_of_day"] = dt.hour
    features["day_of_week"] = dt.weekday()
    features["is_night"] = 1 if (dt.hour >= 22 or dt.hour <= 5) else 0
    features["is_weekend"] = 1 if dt.weekday() >= 5 else 0

    # --- Channel / Card Features ---
    channel = txn.get("channel", "UNKNOWN")
    features["is_online"] = 1 if channel in ("ONLINE", "MOBILE") else 0
    features["is_international"] = 1 if txn.get("is_international", False) else 0

    # --- MCC Risk Score ---
    # High-risk merchant categories get a higher score
    high_risk_mccs = {5944, 5732, 6012, 6051, 5399, 5816}  # jewelry, electronics, wire, crypto, etc.
    features["mcc_risk"] = 1 if txn.get("mcc", 0) in high_risk_mccs else 0

    # --- Behavioral Features (require cardholder history) ---
    if cardholder_history and len(cardholder_history.get("recent_amounts", [])) > 0:
        recent_amounts = cardholder_history["recent_amounts"]
        recent_ts = cardholder_history["recent_timestamps"]

        # Rolling average amount
        avg_amount = sum(recent_amounts) / len(recent_amounts)
        features["rolling_avg_amount"] = avg_amount

        # Amount z-score relative to cardholder's recent history
        if len(recent_amounts) >= 3:
            import statistics
            std_amt = statistics.stdev(recent_amounts) if len(recent_amounts) > 1 else 1.0
            features["amount_zscore"] = (amount - avg_amount) / max(std_amt, 0.01)
        else:
            features["amount_zscore"] = 0.0

        # Transaction velocity (count in last hour)
        one_hour_ago = ts_ms - 3600000
        recent_count = sum(1 for t in recent_ts if t > one_hour_ago)
        features["txn_count_1h"] = recent_count
        features["rapid_txn"] = 1 if recent_count > 5 else 0

        # Time since last transaction (seconds)
        if recent_ts:
            last_ts = max(recent_ts)
            features["seconds_since_last"] = max(0, (ts_ms - last_ts) / 1000)
        else:
            features["seconds_since_last"] = 0

        # Geographic distance from last known location
        last_lat = cardholder_history.get("last_lat")
        last_lon = cardholder_history.get("last_lon")
        curr_lat = txn.get("latitude", 0)
        curr_lon = txn.get("longitude", 0)

        if last_lat is not None and last_lon is not None:
            geo_dist = haversine_distance(last_lat, last_lon, curr_lat, curr_lon)
            features["geo_distance_km"] = geo_dist
            features["geo_anomaly"] = 1 if geo_dist > 500 else 0  # >500km is suspicious
        else:
            features["geo_distance_km"] = 0.0
            features["geo_anomaly"] = 0
    else:
        # No history available (cold start)
        features["rolling_avg_amount"] = amount
        features["amount_zscore"] = 0.0
        features["txn_count_1h"] = 0
        features["rapid_txn"] = 0
        features["seconds_since_last"] = 0
        features["geo_distance_km"] = 0.0
        features["geo_anomaly"] = 0

    return features


def update_cardholder_history(history: dict, txn: dict, max_window: int = 100) -> dict:
    """
    Update the cardholder's rolling history with a new transaction.
    Keeps at most `max_window` recent entries to bound memory usage.
    """
    if history is None:
        history = {"recent_amounts": [], "recent_timestamps": []}

    history["recent_amounts"].append(txn.get("amount", 0.0))
    history["recent_timestamps"].append(txn.get("timestamp", 0))
    history["last_lat"] = txn.get("latitude", 0)
    history["last_lon"] = txn.get("longitude", 0)

    # Trim to window size
    if len(history["recent_amounts"]) > max_window:
        history["recent_amounts"] = history["recent_amounts"][-max_window:]
        history["recent_timestamps"] = history["recent_timestamps"][-max_window:]

    return history
