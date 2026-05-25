"""
FastAPI Backend — Real-Time Fraud Detection System
===================================================
Main application entry point with REST API and WebSocket support.

Usage:
    uvicorn main:app --reload --port 8000

API Docs:
    http://localhost:8000/docs  (Swagger UI)
    http://localhost:8000/redoc (ReDoc)
"""

import os
import json
import logging
import threading
from datetime import datetime
from contextlib import asynccontextmanager

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from database import init_db, engine, SessionLocal
from models import Transaction, Alert

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s — %(message)s",
    datefmt="%H:%M:%S"
)
logger = logging.getLogger("backend")


def ingest_simulation_results():
    """
    Background thread that reads the simulation results file
    and inserts new records into the database.
    Enables the dashboard to work without Kafka/Spark.
    """
    results_file = os.getenv(
        "SIMULATION_RESULTS",
        os.path.join(os.path.dirname(__file__), "simulation_results.jsonl")
    )

    if not os.path.exists(results_file):
        logger.info("No simulation results file found. Skipping background ingestion.")
        return

    logger.info(f"Starting background ingestion from: {results_file}")

    import time
    with open(results_file, "r") as f:
        # Seek to end — only process new entries
        f.seek(0, 2)

        while True:
            line = f.readline()
            if line:
                try:
                    data = json.loads(line.strip())
                    db = SessionLocal()
                    try:
                        txn = Transaction(
                            transaction_id=data.get("transaction_id", ""),
                            timestamp=datetime.fromtimestamp(data.get("timestamp", 0) / 1000),
                            cardholder_id=data.get("cardholder_id", ""),
                            merchant_name=data.get("merchant_name", ""),
                            amount=data.get("amount", 0),
                            channel=data.get("channel", ""),
                            fraud_score=data.get("fraud_score"),
                            classification=data.get("classification"),
                            processing_time_ms=data.get("processing_time_ms"),
                            features=data.get("features"),
                        )
                        db.add(txn)

                        # Create alert if fraud/suspicious
                        classification = data.get("classification", "")
                        if classification in ("FRAUDULENT", "SUSPICIOUS"):
                            fraud_score = data.get("fraud_score", 0)
                            if fraud_score > 0.8:
                                severity = "CRITICAL"
                            elif fraud_score > 0.7:
                                severity = "HIGH"
                            elif fraud_score > 0.5:
                                severity = "MEDIUM"
                            else:
                                severity = "LOW"

                            alert = Alert(
                                transaction_id=data.get("transaction_id", ""),
                                cardholder_id=data.get("cardholder_id", ""),
                                amount=data.get("amount", 0),
                                merchant_name=data.get("merchant_name", ""),
                                fraud_score=fraud_score,
                                classification=classification,
                                severity=severity,
                                channel=data.get("channel", ""),
                            )
                            db.add(alert)

                        db.commit()
                    except Exception as e:
                        db.rollback()
                        # Ignore duplicates silently
                        if "UNIQUE" not in str(e).upper():
                            logger.error(f"DB insert error: {e}")
                    finally:
                        db.close()
                except json.JSONDecodeError:
                    pass
            else:
                time.sleep(0.5)


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Application lifecycle: startup and shutdown."""
    # Startup
    logger.info("=" * 50)
    logger.info("🚀 Fraud Detection Backend Starting...")
    logger.info("=" * 50)

    init_db()

    # Start background ingestion thread
    ingestion_thread = threading.Thread(
        target=ingest_simulation_results, daemon=True
    )
    ingestion_thread.start()

    yield

    # Shutdown
    logger.info("Backend shutting down.")


# Create FastAPI app
app = FastAPI(
    title="Real-Time Fraud Detection API",
    description=(
        "REST API and WebSocket server for the Real-Time Fraud Detection System. "
        "Provides endpoints for transactions, alerts, analytics, and live updates."
    ),
    version="1.0.0",
    lifespan=lifespan,
)

# CORS — allow frontend to connect
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # Restrict in production
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Register routers
from routers.transactions import router as txn_router
from routers.alerts import router as alert_router
from routers.analytics import router as analytics_router
from routers.websocket import router as ws_router

app.include_router(txn_router)
app.include_router(alert_router)
app.include_router(analytics_router)
app.include_router(ws_router)


@app.get("/", tags=["Health"])
def root():
    """API root / health check."""
    return {
        "service": "Fraud Detection API",
        "version": "1.0.0",
        "status": "running",
        "docs": "/docs",
        "timestamp": datetime.utcnow().isoformat(),
    }


@app.get("/health", tags=["Health"])
def health_check():
    """Detailed health check."""
    from database import check_db_health
    db_ok = check_db_health()
    return {
        "status": "healthy" if db_ok else "degraded",
        "database": "connected" if db_ok else "disconnected",
        "timestamp": datetime.utcnow().isoformat(),
    }


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(
        "main:app",
        host="0.0.0.0",
        port=8000,
        reload=True,
        log_level="info",
    )
