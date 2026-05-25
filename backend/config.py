"""
Application Configuration
=========================
Loads settings from environment variables with sensible defaults.
"""

import os
from dataclasses import dataclass


@dataclass
class Settings:
    # PostgreSQL
    POSTGRES_HOST: str = os.getenv("POSTGRES_HOST", "localhost")
    POSTGRES_PORT: int = int(os.getenv("POSTGRES_PORT", "5432"))
    POSTGRES_DB: str = os.getenv("POSTGRES_DB", "fraud_detection")
    POSTGRES_USER: str = os.getenv("POSTGRES_USER", "fraud_user")
    POSTGRES_PASSWORD: str = os.getenv("POSTGRES_PASSWORD", "fraud_pass_2024")

    # Kafka
    KAFKA_BOOTSTRAP_SERVERS: str = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
    KAFKA_ALERTS_TOPIC: str = os.getenv("KAFKA_ALERTS_TOPIC", "fraud-alerts")

    # Backend
    BACKEND_HOST: str = os.getenv("BACKEND_HOST", "0.0.0.0")
    BACKEND_PORT: int = int(os.getenv("BACKEND_PORT", "8000"))
    SECRET_KEY: str = os.getenv("SECRET_KEY", "dev-secret-key-change-in-prod")

    # Simulation data path
    SIMULATION_RESULTS: str = os.getenv(
        "SIMULATION_RESULTS",
        os.path.join(os.path.dirname(__file__), "simulation_results.jsonl")
    )

    @property
    def database_url(self) -> str:
        return (
            f"postgresql+asyncpg://{self.POSTGRES_USER}:{self.POSTGRES_PASSWORD}"
            f"@{self.POSTGRES_HOST}:{self.POSTGRES_PORT}/{self.POSTGRES_DB}"
        )

    @property
    def database_url_sync(self) -> str:
        return (
            f"postgresql://{self.POSTGRES_USER}:{self.POSTGRES_PASSWORD}"
            f"@{self.POSTGRES_HOST}:{self.POSTGRES_PORT}/{self.POSTGRES_DB}"
        )


settings = Settings()
