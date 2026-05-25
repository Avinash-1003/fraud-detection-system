"""
Feature Engineering Pipeline for Fraud Detection
=================================================
Shared feature transformations used by both the training script
and the real-time Spark streaming inference job.

Author: [Your Name]
"""

import numpy as np
import pandas as pd
from sklearn.preprocessing import StandardScaler


def create_time_features(df: pd.DataFrame) -> pd.DataFrame:
    """
    Extract time-based features from a timestamp column.
    If no timestamp column exists, creates synthetic time features
    from the 'Time' column (seconds since first transaction).
    """
    df = df.copy()

    if "Time" in df.columns:
        # Kaggle dataset: 'Time' is seconds elapsed since first txn
        df["hour_of_day"] = (df["Time"] / 3600).astype(int) % 24
        df["is_night"] = ((df["hour_of_day"] >= 22) | (df["hour_of_day"] <= 5)).astype(int)
        # Transaction velocity: time gap from previous transaction
        df["time_delta"] = df["Time"].diff().fillna(0)
        df["rapid_txn"] = (df["time_delta"] < 60).astype(int)  # < 1 min gap

    return df


def create_amount_features(df: pd.DataFrame) -> pd.DataFrame:
    """
    Derive amount-based features that help detect anomalous spending.
    """
    df = df.copy()

    if "Amount" in df.columns:
        # Log-transform to reduce skewness (amounts span $0 to $25K+)
        df["log_amount"] = np.log1p(df["Amount"])

        # Amount relative to overall statistics
        mean_amt = df["Amount"].mean()
        std_amt = df["Amount"].std()
        df["amount_zscore"] = (df["Amount"] - mean_amt) / (std_amt + 1e-8)

        # Binary flags for high-value transactions
        df["is_high_value"] = (df["Amount"] > 500).astype(int)
        df["is_very_high_value"] = (df["Amount"] > 2000).astype(int)

    return df


def preprocess_dataset(df: pd.DataFrame, fit_scaler: bool = True, scaler: StandardScaler = None):
    """
    Full preprocessing pipeline for the Kaggle Credit Card Fraud dataset.

    Parameters
    ----------
    df : pd.DataFrame
        Raw dataset with columns V1-V28, Time, Amount, Class.
    fit_scaler : bool
        If True, fit a new scaler on the data. If False, use the provided scaler.
    scaler : StandardScaler or None
        Pre-fitted scaler (used during inference).

    Returns
    -------
    X : np.ndarray — feature matrix
    y : np.ndarray — labels (0 = legitimate, 1 = fraud)
    scaler : StandardScaler — fitted scaler for reuse
    feature_names : list — ordered feature names
    """
    df = df.copy()

    # Create derived features
    df = create_time_features(df)
    df = create_amount_features(df)

    # Separate target
    y = df["Class"].values if "Class" in df.columns else None

    # Select features: PCA components V1-V28 + engineered features
    pca_cols = [f"V{i}" for i in range(1, 29)]
    engineered_cols = [
        "log_amount", "amount_zscore", "is_high_value", "is_very_high_value",
        "hour_of_day", "is_night", "time_delta", "rapid_txn"
    ]

    # Only include columns that exist in the dataframe
    feature_cols = [c for c in pca_cols + engineered_cols if c in df.columns]
    X = df[feature_cols].values
    feature_names = feature_cols

    # Handle any NaN/Inf values
    X = np.nan_to_num(X, nan=0.0, posinf=0.0, neginf=0.0)

    # Scale features
    if fit_scaler:
        scaler = StandardScaler()
        X = scaler.fit_transform(X)
    elif scaler is not None:
        X = scaler.transform(X)

    return X, y, scaler, feature_names


def get_feature_names() -> list:
    """Return the ordered list of feature names used by the model."""
    pca_cols = [f"V{i}" for i in range(1, 29)]
    engineered_cols = [
        "log_amount", "amount_zscore", "is_high_value", "is_very_high_value",
        "hour_of_day", "is_night", "time_delta", "rapid_txn"
    ]
    return pca_cols + engineered_cols
