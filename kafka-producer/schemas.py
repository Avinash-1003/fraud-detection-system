"""
Transaction Schema Definitions
==============================
Pydantic models for transaction event validation.
Used by the Kafka producer and the backend API.
"""

from pydantic import BaseModel, Field
from typing import Optional
from datetime import datetime
import random
import string
import time
import uuid


class TransactionEvent(BaseModel):
    """Schema for a single financial transaction event."""
    transaction_id: str = Field(..., description="Unique transaction identifier")
    timestamp: int = Field(..., description="Unix timestamp in milliseconds")
    cardholder_id: str = Field(..., description="Unique cardholder identifier")
    merchant_id: str = Field(..., description="Merchant identifier")
    amount: float = Field(..., ge=0, description="Transaction amount in USD")
    currency: str = Field(default="USD", description="Currency code")
    mcc: int = Field(..., description="Merchant Category Code")
    merchant_name: str = Field(..., description="Merchant name")
    latitude: float = Field(..., ge=-90, le=90, description="Transaction latitude")
    longitude: float = Field(..., ge=-180, le=180, description="Transaction longitude")
    channel: str = Field(..., description="Transaction channel: POS, ONLINE, MOBILE, ATM")
    card_type: str = Field(default="DEBIT", description="CREDIT or DEBIT")
    is_international: bool = Field(default=False, description="Cross-border transaction flag")

    def to_dict(self) -> dict:
        return self.model_dump()


# --- Realistic Data Pools ---

MERCHANTS = [
    ("Amazon", 5411), ("Flipkart", 5411), ("BigBazaar", 5411),
    ("Swiggy", 5812), ("Zomato", 5812), ("Dominos", 5812),
    ("Shell Petrol", 5541), ("HP Fuel", 5541), ("IOCL", 5541),
    ("Apollo Pharmacy", 5912), ("MedPlus", 5912),
    ("IRCTC", 4112), ("MakeMyTrip", 4511), ("BookMyShow", 7832),
    ("Tanishq Jewellers", 5944), ("Kalyan Jewellers", 5944),
    ("Reliance Digital", 5732), ("Croma", 5732),
    ("DMart", 5411), ("Spencer's", 5411),
    ("Uber", 4121), ("Ola", 4121),
    ("Netflix", 5815), ("Spotify", 5815), ("Hotstar", 5815),
]

CITIES = [
    ("Bengaluru", 12.97, 77.59), ("Mumbai", 19.07, 72.87),
    ("Delhi", 28.61, 77.20), ("Chennai", 13.08, 80.27),
    ("Hyderabad", 17.38, 78.47), ("Pune", 18.52, 73.85),
    ("Kolkata", 22.57, 88.36), ("Ahmedabad", 23.02, 72.57),
    ("Jaipur", 26.91, 75.79), ("Lucknow", 26.85, 80.95),
]

# Suspicious foreign locations
SUSPICIOUS_CITIES = [
    ("Lagos", 6.45, 3.40), ("Vladivostok", 43.11, 131.87),
    ("Bucharest", 44.43, 26.10), ("Jakarta", -6.21, 106.85),
]

CHANNELS = ["POS", "ONLINE", "MOBILE", "ATM"]
CARD_TYPES = ["CREDIT", "DEBIT"]


def generate_normal_transaction(cardholder_id: str = None) -> TransactionEvent:
    """Generate a realistic, legitimate-looking transaction."""
    if not cardholder_id:
        cardholder_id = f"CH-{random.randint(10000, 99999)}"

    merchant_name, mcc = random.choice(MERCHANTS)
    city_name, lat, lon = random.choice(CITIES)

    # Normal amounts: mostly small, occasionally medium
    amount_pool = random.choices(
        [(1, 100), (100, 500), (500, 2000), (2000, 5000)],
        weights=[50, 30, 15, 5], k=1
    )[0]
    amount = round(random.uniform(*amount_pool), 2)

    # Add small geo-noise (within same city)
    lat += random.uniform(-0.05, 0.05)
    lon += random.uniform(-0.05, 0.05)

    return TransactionEvent(
        transaction_id=f"TXN-{uuid.uuid4().hex[:12].upper()}",
        timestamp=int(time.time() * 1000),
        cardholder_id=cardholder_id,
        merchant_id=f"MER-{random.randint(1000, 9999)}",
        amount=amount,
        currency="USD",
        mcc=mcc,
        merchant_name=merchant_name,
        latitude=round(lat, 6),
        longitude=round(lon, 6),
        channel=random.choice(CHANNELS),
        card_type=random.choice(CARD_TYPES),
        is_international=False,
    )


def generate_fraudulent_transaction(cardholder_id: str = None) -> TransactionEvent:
    """
    Generate a transaction that exhibits fraud-like patterns:
    - Unusually high amount
    - International / suspicious location
    - Late night timing
    - Electronics or jewelry merchant
    """
    if not cardholder_id:
        cardholder_id = f"CH-{random.randint(10000, 99999)}"

    # Fraudsters tend toward high-value categories
    fraud_merchants = [
        ("Electronics Store", 5732), ("Jewelry Shop", 5944),
        ("Wire Transfer", 6012), ("Crypto Exchange", 6051),
        ("Gift Cards", 5399), ("Online Gaming", 5816),
    ]
    merchant_name, mcc = random.choice(fraud_merchants)

    # Higher amounts
    amount = round(random.uniform(800, 15000), 2)

    # Often from suspicious locations
    if random.random() < 0.6:
        city_name, lat, lon = random.choice(SUSPICIOUS_CITIES)
        is_international = True
    else:
        city_name, lat, lon = random.choice(CITIES)
        is_international = False

    return TransactionEvent(
        transaction_id=f"TXN-{uuid.uuid4().hex[:12].upper()}",
        timestamp=int(time.time() * 1000),
        cardholder_id=cardholder_id,
        merchant_id=f"MER-{random.randint(1000, 9999)}",
        amount=amount,
        currency=random.choice(["USD", "EUR", "GBP"]),
        mcc=mcc,
        merchant_name=merchant_name,
        latitude=round(lat + random.uniform(-0.1, 0.1), 6),
        longitude=round(lon + random.uniform(-0.1, 0.1), 6),
        channel=random.choice(["ONLINE", "MOBILE"]),  # Fraud is mostly CNP
        card_type=random.choice(CARD_TYPES),
        is_international=is_international,
    )
