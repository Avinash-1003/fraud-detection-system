"""
Pydantic Schemas
================
Request/response schemas for the REST API.
"""

from pydantic import BaseModel
from typing import Optional, List
from datetime import datetime


class TransactionResponse(BaseModel):
    transaction_id: str
    timestamp: Optional[datetime] = None
    cardholder_id: str
    amount: float
    merchant_name: Optional[str] = None
    channel: Optional[str] = None
    fraud_score: Optional[float] = None
    classification: Optional[str] = None
    processing_time_ms: Optional[float] = None

    class Config:
        from_attributes = True


class AlertResponse(BaseModel):
    id: int
    transaction_id: str
    cardholder_id: str
    amount: float
    merchant_name: Optional[str] = None
    fraud_score: float
    classification: str
    severity: str
    status: str
    created_at: Optional[datetime] = None

    class Config:
        from_attributes = True


class DashboardMetrics(BaseModel):
    total_transactions: int
    total_fraud: int
    total_suspicious: int
    total_legitimate: int
    fraud_rate: float
    avg_fraud_score: float
    avg_processing_time_ms: float
    transactions_per_second: float
    active_alerts: int


class TrendPoint(BaseModel):
    timestamp: str
    fraud_count: int
    total_count: int
    fraud_rate: float


class ModelInfo(BaseModel):
    model_name: str
    version: str
    algorithm: str
    accuracy: Optional[float] = None
    precision: Optional[float] = None
    recall: Optional[float] = None
    f1_score: Optional[float] = None
    is_active: bool
    deployed_at: Optional[datetime] = None
