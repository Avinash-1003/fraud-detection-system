"""
Database Connection Layer
=========================
SQLAlchemy async engine and session management.
Falls back to SQLite for development when PostgreSQL is unavailable.
"""

import os
import logging
from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker, declarative_base

logger = logging.getLogger("database")

Base = declarative_base()

# Use SQLite by default for easy development; PostgreSQL in production
DB_URL = os.getenv("DATABASE_URL", "sqlite:///./fraud_detection.db")

# Create engine
if "sqlite" in DB_URL:
    engine = create_engine(DB_URL, connect_args={"check_same_thread": False})
    logger.info("Using SQLite database (development mode)")
else:
    engine = create_engine(DB_URL, pool_size=10, max_overflow=20)
    logger.info(f"Using PostgreSQL: {DB_URL.split('@')[-1] if '@' in DB_URL else DB_URL}")

SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)


def get_db():
    """Dependency that provides a database session."""
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()


def init_db():
    """Create all tables."""
    from models import Transaction, Alert, ModelMetadata  # noqa
    Base.metadata.create_all(bind=engine)
    logger.info("Database tables created successfully.")


def check_db_health() -> bool:
    """Quick health check."""
    try:
        with engine.connect() as conn:
            conn.execute(text("SELECT 1"))
        return True
    except Exception as e:
        logger.error(f"Database health check failed: {e}")
        return False
