"""
Transaction API Router
======================
Endpoints for querying transaction history and predictions.
"""

from fastapi import APIRouter, Depends, Query
from sqlalchemy.orm import Session
from sqlalchemy import desc
from typing import Optional
from database import get_db
from models import Transaction
from schemas import TransactionResponse

router = APIRouter(prefix="/api/transactions", tags=["Transactions"])


@router.get("/", response_model=list[TransactionResponse])
def list_transactions(
    limit: int = Query(50, ge=1, le=500),
    offset: int = Query(0, ge=0),
    classification: Optional[str] = Query(None, description="Filter: LEGITIMATE, SUSPICIOUS, FRAUDULENT"),
    cardholder_id: Optional[str] = Query(None),
    db: Session = Depends(get_db),
):
    """Get paginated list of transactions with optional filters."""
    query = db.query(Transaction).order_by(desc(Transaction.created_at))

    if classification:
        query = query.filter(Transaction.classification == classification.upper())
    if cardholder_id:
        query = query.filter(Transaction.cardholder_id == cardholder_id)

    results = query.offset(offset).limit(limit).all()
    return results


@router.get("/recent", response_model=list[TransactionResponse])
def recent_transactions(
    limit: int = Query(20, ge=1, le=100),
    db: Session = Depends(get_db),
):
    """Get the most recent transactions (for live feed)."""
    results = (
        db.query(Transaction)
        .order_by(desc(Transaction.created_at))
        .limit(limit)
        .all()
    )
    return results


@router.get("/{transaction_id}", response_model=TransactionResponse)
def get_transaction(transaction_id: str, db: Session = Depends(get_db)):
    """Get a single transaction by ID."""
    txn = db.query(Transaction).filter(
        Transaction.transaction_id == transaction_id
    ).first()

    if not txn:
        from fastapi import HTTPException
        raise HTTPException(status_code=404, detail="Transaction not found")

    return txn


@router.get("/stats/count")
def transaction_counts(db: Session = Depends(get_db)):
    """Get transaction counts by classification."""
    from sqlalchemy import func

    results = (
        db.query(Transaction.classification, func.count(Transaction.id))
        .group_by(Transaction.classification)
        .all()
    )

    counts = {r[0]: r[1] for r in results if r[0]}
    return {
        "total": sum(counts.values()),
        "legitimate": counts.get("LEGITIMATE", 0),
        "suspicious": counts.get("SUSPICIOUS", 0),
        "fraudulent": counts.get("FRAUDULENT", 0),
    }
