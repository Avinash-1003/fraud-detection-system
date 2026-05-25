"""
Alert API Router
================
Endpoints for fraud alert management.
"""

from fastapi import APIRouter, Depends, Query, HTTPException
from sqlalchemy.orm import Session
from sqlalchemy import desc, func
from typing import Optional
from database import get_db
from models import Alert
from schemas import AlertResponse

router = APIRouter(prefix="/api/alerts", tags=["Alerts"])


@router.get("/", response_model=list[AlertResponse])
def list_alerts(
    limit: int = Query(50, ge=1, le=500),
    offset: int = Query(0, ge=0),
    severity: Optional[str] = Query(None, description="Filter: LOW, MEDIUM, HIGH, CRITICAL"),
    status: Optional[str] = Query(None, description="Filter: OPEN, REVIEWED, RESOLVED"),
    db: Session = Depends(get_db),
):
    """Get paginated list of fraud alerts."""
    query = db.query(Alert).order_by(desc(Alert.created_at))

    if severity:
        query = query.filter(Alert.severity == severity.upper())
    if status:
        query = query.filter(Alert.status == status.upper())

    return query.offset(offset).limit(limit).all()


@router.get("/stats")
def alert_stats(db: Session = Depends(get_db)):
    """Get alert statistics by severity and status."""
    severity_counts = dict(
        db.query(Alert.severity, func.count(Alert.id))
        .group_by(Alert.severity).all()
    )
    status_counts = dict(
        db.query(Alert.status, func.count(Alert.id))
        .group_by(Alert.status).all()
    )

    return {
        "total": sum(severity_counts.values()),
        "by_severity": {
            "critical": severity_counts.get("CRITICAL", 0),
            "high": severity_counts.get("HIGH", 0),
            "medium": severity_counts.get("MEDIUM", 0),
            "low": severity_counts.get("LOW", 0),
        },
        "by_status": {
            "open": status_counts.get("OPEN", 0),
            "reviewed": status_counts.get("REVIEWED", 0),
            "resolved": status_counts.get("RESOLVED", 0),
            "false_positive": status_counts.get("FALSE_POSITIVE", 0),
        }
    }


@router.put("/{alert_id}/status")
def update_alert_status(
    alert_id: int,
    status: str = Query(..., description="New status: REVIEWED, RESOLVED, FALSE_POSITIVE"),
    reviewed_by: Optional[str] = Query(None),
    notes: Optional[str] = Query(None),
    db: Session = Depends(get_db),
):
    """Update the status of a fraud alert (analyst workflow)."""
    alert = db.query(Alert).filter(Alert.id == alert_id).first()
    if not alert:
        raise HTTPException(status_code=404, detail="Alert not found")

    alert.status = status.upper()
    if reviewed_by:
        alert.reviewed_by = reviewed_by
    if notes:
        alert.notes = notes

    db.commit()
    return {"message": "Alert updated", "id": alert_id, "status": alert.status}
