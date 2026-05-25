"""
Real-Time Transaction Generator + Kafka Producer
=================================================
Simulates a stream of banking transactions (normal + fraudulent)
and publishes them to a Kafka topic in real time.

Features:
- Configurable TPS (transactions per second)
- Adjustable fraud injection rate
- Idempotent producer with exactly-once guarantees
- Graceful shutdown on Ctrl+C
- Standalone mode (no Kafka) for testing

Usage:
    # With Kafka running:
    python producer.py --tps 100 --fraud-rate 0.035

    # Standalone mode (prints to console, no Kafka required):
    python producer.py --standalone --tps 5

Requirements:
    pip install confluent-kafka pydantic
"""

import os
import sys
import json
import time
import signal
import logging
import argparse
from datetime import datetime

from schemas import generate_normal_transaction, generate_fraudulent_transaction

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%H:%M:%S"
)
logger = logging.getLogger("kafka-producer")

# Graceful shutdown
_running = True

def signal_handler(sig, frame):
    global _running
    logger.info("Received shutdown signal. Flushing remaining messages...")
    _running = False

signal.signal(signal.SIGINT, signal_handler)
signal.signal(signal.SIGTERM, signal_handler)


def create_kafka_producer(bootstrap_servers: str):
    """Create a Kafka producer with idempotent, exactly-once configuration."""
    try:
        from confluent_kafka import Producer
    except ImportError:
        logger.error("confluent-kafka not installed. Install with: pip install confluent-kafka")
        logger.error("Or use --standalone mode to run without Kafka.")
        sys.exit(1)

    conf = {
        "bootstrap.servers": bootstrap_servers,
        "acks": "all",                              # Wait for all ISR replicas
        "enable.idempotence": True,                  # Exactly-once delivery
        "max.in.flight.requests.per.connection": 5,  # Max pipelining
        "retries": 10,                               # Retry on transient errors
        "retry.backoff.ms": 100,
        "linger.ms": 5,                              # Batch for 5ms for throughput
        "batch.size": 65536,                         # 64KB batches
        "compression.type": "snappy",                # Compress for bandwidth
    }

    producer = Producer(conf)
    logger.info(f"Kafka producer created → {bootstrap_servers}")
    return producer


def delivery_callback(err, msg):
    """Called once per message to confirm delivery or report errors."""
    if err is not None:
        logger.error(f"Delivery FAILED for {msg.key()}: {err}")
    # Uncomment for verbose logging:
    # else:
    #     logger.debug(f"Delivered to {msg.topic()}[{msg.partition()}] @ offset {msg.offset()}")


def run_producer(args):
    """Main production loop."""
    global _running

    # Setup producer (or None for standalone)
    producer = None
    if not args.standalone:
        producer = create_kafka_producer(args.bootstrap_servers)

    topic = args.topic
    tps = args.tps
    fraud_rate = args.fraud_rate
    interval = 1.0 / tps if tps > 0 else 0.1

    # Statistics
    total_sent = 0
    fraud_sent = 0
    start_time = time.time()
    stats_interval = 5  # Print stats every N seconds
    last_stats = start_time

    logger.info(f"Starting transaction generator")
    logger.info(f"  Topic       : {topic}")
    logger.info(f"  Target TPS  : {tps}")
    logger.info(f"  Fraud rate  : {fraud_rate*100:.1f}%")
    logger.info(f"  Mode        : {'Standalone (console)' if args.standalone else 'Kafka'}")
    logger.info(f"  Press Ctrl+C to stop\n")

    # Pool of active cardholder IDs (simulates real cardholders)
    cardholder_pool = [f"CH-{10000 + i}" for i in range(500)]

    while _running:
        loop_start = time.time()

        # Decide if this transaction is fraudulent
        import random
        is_fraud = random.random() < fraud_rate
        cardholder = random.choice(cardholder_pool)

        if is_fraud:
            txn = generate_fraudulent_transaction(cardholder)
            fraud_sent += 1
        else:
            txn = generate_normal_transaction(cardholder)

        txn_dict = txn.to_dict()
        txn_json = json.dumps(txn_dict)

        if args.standalone:
            # Console mode: just print
            label = "🔴 FRAUD" if is_fraud else "🟢 LEGIT"
            logger.info(f"{label} | {txn.transaction_id} | "
                       f"${txn.amount:>9,.2f} | {txn.merchant_name:<20} | {txn.cardholder_id}")
        else:
            # Publish to Kafka
            producer.produce(
                topic=topic,
                key=txn.cardholder_id.encode("utf-8"),
                value=txn_json.encode("utf-8"),
                callback=delivery_callback
            )
            producer.poll(0)  # Trigger delivery callbacks

        total_sent += 1

        # Print statistics periodically
        now = time.time()
        if now - last_stats >= stats_interval:
            elapsed = now - start_time
            actual_tps = total_sent / elapsed if elapsed > 0 else 0
            fraud_pct = (fraud_sent / total_sent * 100) if total_sent > 0 else 0
            logger.info(
                f"📊 Stats | Sent: {total_sent:,} | "
                f"Fraud: {fraud_sent:,} ({fraud_pct:.1f}%) | "
                f"Actual TPS: {actual_tps:.1f} | "
                f"Elapsed: {elapsed:.0f}s"
            )
            last_stats = now

        # Throttle to target TPS
        elapsed_loop = time.time() - loop_start
        sleep_time = interval - elapsed_loop
        if sleep_time > 0:
            time.sleep(sleep_time)

    # Cleanup
    if producer:
        remaining = producer.flush(timeout=10)
        if remaining > 0:
            logger.warning(f"{remaining} messages were not delivered")

    elapsed = time.time() - start_time
    logger.info(f"\n{'='*50}")
    logger.info(f"Producer stopped.")
    logger.info(f"  Total sent   : {total_sent:,}")
    logger.info(f"  Fraudulent   : {fraud_sent:,} ({fraud_sent/max(total_sent,1)*100:.1f}%)")
    logger.info(f"  Duration     : {elapsed:.1f}s")
    logger.info(f"  Avg TPS      : {total_sent/max(elapsed,1):.1f}")
    logger.info(f"{'='*50}")


def main():
    parser = argparse.ArgumentParser(
        description="Real-time banking transaction generator + Kafka producer"
    )
    parser.add_argument("--bootstrap-servers", type=str,
                        default=os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092"),
                        help="Kafka bootstrap servers (default: localhost:9092)")
    parser.add_argument("--topic", type=str,
                        default=os.getenv("KAFKA_TRANSACTIONS_TOPIC", "transactions"),
                        help="Kafka topic name (default: transactions)")
    parser.add_argument("--tps", type=int, default=10,
                        help="Target transactions per second (default: 10)")
    parser.add_argument("--fraud-rate", type=float, default=0.035,
                        help="Fraction of transactions that are fraudulent (default: 0.035)")
    parser.add_argument("--standalone", action="store_true",
                        help="Run without Kafka (print to console)")
    args = parser.parse_args()

    run_producer(args)


if __name__ == "__main__":
    main()
