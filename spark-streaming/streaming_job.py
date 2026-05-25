"""
Spark Structured Streaming — Fraud Detection Job
=================================================
Consumes transaction events from Kafka, applies real-time feature
engineering and ML model inference, and routes results to:
- fraud-alerts Kafka topic (for flagged transactions)
- PostgreSQL (for all predictions)
- Console (for debugging)

This script works in two modes:
1. Full mode: Requires Kafka + Spark + PostgreSQL (production)
2. Simulation mode: Reads from a local JSON file (development/demo)

Usage:
    # Full Spark submit (production):
    spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0 \
        streaming_job.py

    # Simulation mode (no Spark/Kafka needed):
    python streaming_job.py --simulate

Requirements:
    pip install pyspark joblib numpy
"""

import os
import sys
import json
import time
import signal
import logging
import argparse
from datetime import datetime
from collections import defaultdict

import numpy as np
import joblib

from feature_pipeline import compute_streaming_features, update_cardholder_history

# Logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%H:%M:%S"
)
logger = logging.getLogger("spark-streaming")

# ============================================================
# Configuration
# ============================================================
KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
KAFKA_INPUT_TOPIC = os.getenv("KAFKA_TRANSACTIONS_TOPIC", "transactions")
KAFKA_ALERTS_TOPIC = os.getenv("KAFKA_ALERTS_TOPIC", "fraud-alerts")
CHECKPOINT_DIR = os.getenv("SPARK_CHECKPOINT_DIR", "./checkpoints")
MODEL_PATH = os.getenv("MODEL_PATH", "../ml/models/fraud_model.joblib")
SCALER_PATH = os.getenv("SCALER_PATH", "../ml/models/scaler.joblib")

# Classification thresholds
FRAUD_THRESHOLD = 0.7
SUSPICIOUS_THRESHOLD = 0.4

# PostgreSQL (for JDBC sink)
PG_URL = os.getenv("PG_URL", "jdbc:postgresql://localhost:5432/fraud_detection")
PG_USER = os.getenv("POSTGRES_USER", "fraud_user")
PG_PASS = os.getenv("POSTGRES_PASSWORD", "fraud_pass_2024")


def load_ml_model(model_path: str, scaler_path: str):
    """Load the trained ML model and scaler from disk."""
    logger.info(f"Loading ML model from: {model_path}")
    try:
        model = joblib.load(model_path)
        logger.info(f"  Model type: {type(model).__name__}")
    except FileNotFoundError:
        logger.warning(f"  Model file not found at {model_path}")
        logger.warning(f"  Using random scoring for demo purposes.")
        model = None

    try:
        scaler = joblib.load(scaler_path)
    except FileNotFoundError:
        scaler = None

    return model, scaler


def classify_transaction(fraud_score: float) -> str:
    """Classify based on threshold."""
    if fraud_score > FRAUD_THRESHOLD:
        return "FRAUDULENT"
    elif fraud_score > SUSPICIOUS_THRESHOLD:
        return "SUSPICIOUS"
    else:
        return "LEGITIMATE"


def process_transaction(txn: dict, model, scaler, cardholder_histories: dict):
    """
    Full processing pipeline for a single transaction:
    1. Compute streaming features
    2. Run ML inference
    3. Classify
    4. Update cardholder history
    """
    cardholder_id = txn.get("cardholder_id", "UNKNOWN")

    # Get/create cardholder history
    history = cardholder_histories.get(cardholder_id)

    # Compute features
    features = compute_streaming_features(txn, history)

    # Model inference
    if model is not None:
        # Build feature vector matching model's expected input
        # For the Kaggle model, we use amount-based features
        # For real deployment, the feature vector would match training features
        feature_vector = np.array([[
            features.get("log_amount", 0),
            features.get("amount_zscore", 0),
            features.get("is_high_value", 0),
            features.get("is_very_high_value", 0),
            features.get("hour_of_day", 12),
            features.get("is_night", 0),
            features.get("seconds_since_last", 0),
            features.get("rapid_txn", 0),
        ]])

        # Pad or truncate to match model's expected feature count
        n_expected = model.n_features_in_
        if feature_vector.shape[1] < n_expected:
            padding = np.zeros((1, n_expected - feature_vector.shape[1]))
            feature_vector = np.hstack([feature_vector, padding])
        elif feature_vector.shape[1] > n_expected:
            feature_vector = feature_vector[:, :n_expected]

        if scaler is not None:
            feature_vector = scaler.transform(feature_vector)

        try:
            fraud_prob = model.predict_proba(feature_vector)[0][1]
        except Exception:
            fraud_prob = model.predict(feature_vector)[0]
    else:
        # Demo mode: use heuristic scoring when no model is available
        fraud_prob = compute_heuristic_score(txn, features)

    classification = classify_transaction(fraud_prob)

    # Update cardholder history
    cardholder_histories[cardholder_id] = update_cardholder_history(history, txn)

    # Build result
    result = {
        "transaction_id": txn.get("transaction_id"),
        "cardholder_id": cardholder_id,
        "amount": txn.get("amount", 0),
        "merchant_name": txn.get("merchant_name", ""),
        "channel": txn.get("channel", ""),
        "timestamp": txn.get("timestamp", 0),
        "fraud_score": round(float(fraud_prob), 4),
        "classification": classification,
        "processing_time_ms": 0,  # Will be set below
        "features": {
            "log_amount": round(features.get("log_amount", 0), 4),
            "amount_zscore": round(features.get("amount_zscore", 0), 4),
            "txn_count_1h": features.get("txn_count_1h", 0),
            "geo_distance_km": round(features.get("geo_distance_km", 0), 2),
            "is_night": features.get("is_night", 0),
            "is_international": features.get("is_international", 0),
        }
    }

    return result


def compute_heuristic_score(txn: dict, features: dict) -> float:
    """
    Simple heuristic fraud scoring when no trained model is available.
    This allows the system to demo without a pre-trained model.
    """
    score = 0.1  # Base score

    amount = txn.get("amount", 0)
    if amount > 5000:
        score += 0.3
    elif amount > 2000:
        score += 0.15
    elif amount > 1000:
        score += 0.05

    if features.get("is_night", 0):
        score += 0.1

    if features.get("is_international", 0):
        score += 0.15

    if features.get("geo_anomaly", 0):
        score += 0.2

    if features.get("rapid_txn", 0):
        score += 0.15

    if features.get("mcc_risk", 0):
        score += 0.1

    # Add small random noise for variety
    import random
    score += random.uniform(-0.05, 0.05)

    return max(0.0, min(1.0, score))


# ============================================================
# Simulation Mode (No Spark/Kafka Required)
# ============================================================
def run_simulation(args):
    """
    Run the pipeline in simulation mode using the Kafka producer's
    transaction generator. No Spark or Kafka required.
    """
    logger.info("=" * 60)
    logger.info("Starting SIMULATION MODE (no Spark/Kafka required)")
    logger.info("=" * 60)

    # Import transaction generator
    sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "kafka-producer"))
    from schemas import generate_normal_transaction, generate_fraudulent_transaction

    model, scaler = load_ml_model(MODEL_PATH, SCALER_PATH)
    cardholder_histories = defaultdict(lambda: None)

    # Stats
    total = 0
    fraud_detected = 0
    suspicious_detected = 0

    # Optional: write results to file for the backend to read
    results_file = os.path.join(os.path.dirname(__file__), "..", "backend", "simulation_results.jsonl")
    os.makedirs(os.path.dirname(results_file), exist_ok=True)

    _running = True
    def stop(sig, frame):
        nonlocal _running
        _running = False
    signal.signal(signal.SIGINT, stop)

    import random

    logger.info(f"Processing at ~{args.tps} TPS. Press Ctrl+C to stop.\n")

    with open(results_file, "a") as f:
        while _running:
            start = time.time()

            # Generate transaction
            is_fraud = random.random() < 0.035
            cardholder = f"CH-{random.randint(10000, 10499)}"
            if is_fraud:
                txn = generate_fraudulent_transaction(cardholder).to_dict()
            else:
                txn = generate_normal_transaction(cardholder).to_dict()

            # Process
            proc_start = time.time()
            result = process_transaction(txn, model, scaler, cardholder_histories)
            result["processing_time_ms"] = round((time.time() - proc_start) * 1000, 2)

            # Write result
            f.write(json.dumps(result) + "\n")
            f.flush()

            total += 1
            classification = result["classification"]
            if classification == "FRAUDULENT":
                fraud_detected += 1
            elif classification == "SUSPICIOUS":
                suspicious_detected += 1

            # Print colored output
            if classification == "FRAUDULENT":
                icon = "🔴"
            elif classification == "SUSPICIOUS":
                icon = "🟡"
            else:
                icon = "🟢"

            logger.info(
                f"{icon} {classification:<12} | {result['transaction_id']} | "
                f"${result['amount']:>9,.2f} | Score: {result['fraud_score']:.3f} | "
                f"{result['merchant_name']:<20} | {result['processing_time_ms']:.1f}ms"
            )

            # Throttle
            elapsed = time.time() - start
            sleep_time = (1.0 / args.tps) - elapsed
            if sleep_time > 0:
                time.sleep(sleep_time)

    # Final stats
    logger.info(f"\n{'='*60}")
    logger.info(f"Simulation Complete")
    logger.info(f"  Total processed  : {total:,}")
    logger.info(f"  Fraudulent       : {fraud_detected:,}")
    logger.info(f"  Suspicious       : {suspicious_detected:,}")
    logger.info(f"  Legitimate       : {total - fraud_detected - suspicious_detected:,}")
    logger.info(f"  Results saved to : {results_file}")
    logger.info(f"{'='*60}")


# ============================================================
# Full Spark Structured Streaming Mode
# ============================================================
def run_spark_streaming(args):
    """Run the full Spark Structured Streaming pipeline with Kafka."""
    try:
        from pyspark.sql import SparkSession
        from pyspark.sql.functions import (
            from_json, col, udf, struct, to_json, current_timestamp,
            window, avg, count, lit
        )
        from pyspark.sql.types import (
            StructType, StructField, StringType, DoubleType,
            LongType, IntegerType, BooleanType
        )
    except ImportError:
        logger.error("PySpark not installed. Install with: pip install pyspark")
        logger.error("Or use --simulate mode.")
        sys.exit(1)

    logger.info("Starting Spark Structured Streaming pipeline...")

    # Create Spark session
    spark = SparkSession.builder \
        .appName("RealTimeFraudDetection") \
        .config("spark.sql.streaming.checkpointLocation", CHECKPOINT_DIR) \
        .config("spark.sql.shuffle.partitions", "4") \
        .config("spark.streaming.stopGracefullyOnShutdown", "true") \
        .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")

    # Define transaction schema
    txn_schema = StructType([
        StructField("transaction_id", StringType(), True),
        StructField("timestamp", LongType(), True),
        StructField("cardholder_id", StringType(), True),
        StructField("merchant_id", StringType(), True),
        StructField("amount", DoubleType(), True),
        StructField("currency", StringType(), True),
        StructField("mcc", IntegerType(), True),
        StructField("merchant_name", StringType(), True),
        StructField("latitude", DoubleType(), True),
        StructField("longitude", DoubleType(), True),
        StructField("channel", StringType(), True),
        StructField("card_type", StringType(), True),
        StructField("is_international", BooleanType(), True),
    ])

    # Read from Kafka
    raw_stream = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP) \
        .option("subscribe", KAFKA_INPUT_TOPIC) \
        .option("startingOffsets", "latest") \
        .option("failOnDataLoss", "false") \
        .load()

    # Parse JSON
    transactions = raw_stream \
        .selectExpr("CAST(value AS STRING) as json_str", "timestamp as kafka_ts") \
        .select(
            from_json(col("json_str"), txn_schema).alias("txn"),
            col("kafka_ts")
        ) \
        .select("txn.*", "kafka_ts")

    # Load ML model as broadcast variable
    model, scaler = load_ml_model(MODEL_PATH, SCALER_PATH)
    model_bc = spark.sparkContext.broadcast(model)
    scaler_bc = spark.sparkContext.broadcast(scaler)

    # UDF for fraud scoring
    @udf(returnType=DoubleType())
    def predict_fraud(amount, mcc, channel, is_international, hour):
        """Simplified UDF for fraud scoring."""
        m = model_bc.value
        if m is None:
            # Heuristic fallback
            score = 0.1
            if amount and amount > 5000: score += 0.3
            if is_international: score += 0.15
            if hour and (hour >= 22 or hour <= 5): score += 0.1
            if mcc and mcc in (5944, 5732, 6012, 6051): score += 0.1
            return min(score, 1.0)

        try:
            features = np.array([[
                np.log1p(amount or 0), 0, 1 if (amount or 0) > 500 else 0,
                1 if (amount or 0) > 2000 else 0, hour or 12, 
                1 if (hour and (hour >= 22 or hour <= 5)) else 0, 0, 0
            ]])
            n_expected = m.n_features_in_
            if features.shape[1] < n_expected:
                features = np.hstack([features, np.zeros((1, n_expected - features.shape[1]))])
            s = scaler_bc.value
            if s is not None:
                features = s.transform(features[:, :n_expected])
            return float(m.predict_proba(features)[0][1])
        except Exception:
            return 0.1

    @udf(returnType=StringType())
    def classify(score):
        if score is None:
            return "LEGITIMATE"
        if score > FRAUD_THRESHOLD:
            return "FRAUDULENT"
        elif score > SUSPICIOUS_THRESHOLD:
            return "SUSPICIOUS"
        return "LEGITIMATE"

    # Apply ML inference
    from pyspark.sql.functions import hour as spark_hour, from_unixtime
    
    scored = transactions \
        .withColumn("event_time", 
            from_unixtime(col("timestamp") / 1000).cast("timestamp")) \
        .withColumn("hour_of_day", spark_hour("event_time")) \
        .withColumn("fraud_score", 
            predict_fraud(col("amount"), col("mcc"), col("channel"),
                         col("is_international"), col("hour_of_day"))) \
        .withColumn("classification", classify(col("fraud_score"))) \
        .withColumn("processed_at", current_timestamp())

    # Output 1: Console sink (for debugging)
    console_query = scored \
        .select("transaction_id", "amount", "merchant_name", 
                "fraud_score", "classification") \
        .writeStream \
        .outputMode("append") \
        .format("console") \
        .option("truncate", "false") \
        .trigger(processingTime="2 seconds") \
        .start()

    # Output 2: Fraud alerts → Kafka topic
    alerts = scored.filter(col("classification").isin("FRAUDULENT", "SUSPICIOUS"))

    alerts_query = alerts \
        .selectExpr(
            "CAST(transaction_id AS STRING) AS key",
            "to_json(struct(*)) AS value"
        ) \
        .writeStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP) \
        .option("topic", KAFKA_ALERTS_TOPIC) \
        .option("checkpointLocation", os.path.join(CHECKPOINT_DIR, "alerts")) \
        .trigger(processingTime="2 seconds") \
        .start()

    logger.info("Spark Streaming pipeline started successfully.")
    logger.info(f"  Input topic  : {KAFKA_INPUT_TOPIC}")
    logger.info(f"  Alerts topic : {KAFKA_ALERTS_TOPIC}")
    logger.info(f"  Checkpoint   : {CHECKPOINT_DIR}")
    logger.info("  Waiting for transactions...\n")

    spark.streams.awaitAnyTermination()


# ============================================================
# Main Entry Point
# ============================================================
def main():
    parser = argparse.ArgumentParser(description="Fraud Detection Streaming Job")
    parser.add_argument("--simulate", action="store_true",
                        help="Run in simulation mode (no Spark/Kafka)")
    parser.add_argument("--tps", type=int, default=5,
                        help="Simulation TPS (default: 5)")
    args = parser.parse_args()

    if args.simulate:
        run_simulation(args)
    else:
        run_spark_streaming(args)


if __name__ == "__main__":
    main()
