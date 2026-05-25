"""
ORM Models
==========
SQLAlchemy models for transactions, predictions, and alerts.
"""

from datetime import datetime
from sqlalchemy import (
    Column, Integer, String, Float, Boolean, DateTime, Text, JSON,
    Index
)
from database import Base


class Transaction(Base):
    """Raw transaction record."""
    __tablename__ = "transactions"

    id = Column(Integer, primary_key=True, autoincrement=True)
    transaction_id = Column(String(64), unique=True, nullable=False, index=True)
    timestamp = Column(DateTime, default=datetime.utcnow, index=True)
    cardholder_id = Column(String(32), nullable=False, index=True)
    merchant_id = Column(String(32))
    merchant_name = Column(String(128))
    amount = Column(Float, nullable=False)
    currency = Column(String(8), default="USD")
    mcc = Column(Integer)
    latitude = Column(Float)
    longitude = Column(Float)
    channel = Column(String(16))
    card_type = Column(String(16))
    is_international = Column(Boolean, default=False)

    # ML prediction results
    fraud_score = Column(Float)
    classification = Column(String(16), index=True)  # LEGITIMATE, SUSPICIOUS, FRAUDULENT
    processing_time_ms = Column(Float)

    # Feature snapshot (for audit trail)
    features = Column(JSON)

    created_at = Column(DateTime, default=datetime.utcnow)

    __table_args__ = (
        Index("ix_txn_cardholder_time", "cardholder_id", "timestamp"),
        Index("ix_txn_classification_time", "classification", "timestamp"),
    )


class Alert(Base):
    """Fraud alert record."""
    __tablename__ = "alerts"

    id = Column(Integer, primary_key=True, autoincrement=True)
    transaction_id = Column(String(64), nullable=False, index=True)
    cardholder_id = Column(String(32), nullable=False)
    amount = Column(Float)
    merchant_name = Column(String(128))
    fraud_score = Column(Float)
    classification = Column(String(16))  # SUSPICIOUS or FRAUDULENT
    severity = Column(String(16))  # LOW, MEDIUM, HIGH, CRITICAL
    channel = Column(String(16))
    status = Column(String(16), default="OPEN")  # OPEN, REVIEWED, RESOLVED, FALSE_POSITIVE
    reviewed_by = Column(String(64))
    notes = Column(Text)
    created_at = Column(DateTime, default=datetime.utcnow, index=True)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)


class ModelMetadata(Base):
    """Tracks deployed ML model versions."""
    __tablename__ = "model_metadata"

    id = Column(Integer, primary_key=True, autoincrement=True)
    model_name = Column(String(64), nullable=False)
    version = Column(String(32), nullable=False)
    algorithm = Column(String(64))
    accuracy = Column(Float)
    precision = Column(Float)
    recall = Column(Float)
    f1_score = Column(Float)
    auprc = Column(Float)
    training_samples = Column(Integer)
    feature_count = Column(Integer)
    is_active = Column(Boolean, default=True)
    deployed_at = Column(DateTime, default=datetime.utcnow)
    notes = Column(Text)
