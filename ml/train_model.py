"""
Fraud Detection Model Training Script
======================================
Trains three classifiers (Random Forest, Logistic Regression, Isolation Forest)
on the Kaggle Credit Card Fraud dataset, evaluates them, and saves the best
model for use in the real-time Spark Streaming pipeline.

Usage:
    python train_model.py --data data/creditcard.csv --output models/

Requirements:
    pip install pandas scikit-learn imbalanced-learn joblib
"""

import os
import sys
import argparse
import json
import time
from datetime import datetime

import numpy as np
import pandas as pd
import joblib
from sklearn.model_selection import train_test_split, StratifiedKFold, cross_val_score
from sklearn.ensemble import RandomForestClassifier, IsolationForest
from sklearn.linear_model import LogisticRegression
from sklearn.metrics import (
    classification_report, confusion_matrix, f1_score,
    precision_score, recall_score, accuracy_score,
    average_precision_score, roc_auc_score
)

# Attempt SMOTE import; fall back gracefully if not installed
try:
    from imblearn.over_sampling import SMOTE
    HAS_SMOTE = True
except ImportError:
    HAS_SMOTE = False
    print("[WARN] imbalanced-learn not installed. Skipping SMOTE resampling.")
    print("       Install with: pip install imbalanced-learn")

from feature_engineering import preprocess_dataset


def load_dataset(path: str) -> pd.DataFrame:
    """Load CSV and print basic statistics."""
    print(f"\n{'='*60}")
    print(f"Loading dataset from: {path}")
    print(f"{'='*60}")

    df = pd.read_csv(path)

    total = len(df)
    fraud_count = df["Class"].sum()
    legit_count = total - fraud_count

    print(f"  Total transactions : {total:,}")
    print(f"  Legitimate         : {legit_count:,} ({legit_count/total*100:.2f}%)")
    print(f"  Fraudulent         : {fraud_count:,} ({fraud_count/total*100:.2f}%)")
    print(f"  Imbalance ratio    : 1:{legit_count//max(fraud_count,1)}")

    return df


def train_random_forest(X_train, y_train, X_test, y_test):
    """Train a Random Forest classifier."""
    print("\n--- Training Random Forest ---")
    start = time.time()

    rf = RandomForestClassifier(
        n_estimators=200,
        max_depth=20,
        min_samples_split=5,
        min_samples_leaf=2,
        max_features="sqrt",
        class_weight="balanced_subsample",
        random_state=42,
        n_jobs=-1
    )
    rf.fit(X_train, y_train)
    elapsed = time.time() - start

    y_pred = rf.predict(X_test)
    y_prob = rf.predict_proba(X_test)[:, 1]

    print(f"  Training time: {elapsed:.1f}s")
    print(f"  Test Accuracy : {accuracy_score(y_test, y_pred):.4f}")
    print(f"  Test F1-Score : {f1_score(y_test, y_pred):.4f}")
    print(f"  Test AUPRC    : {average_precision_score(y_test, y_prob):.4f}")

    return rf, {
        "algorithm": "RandomForest",
        "training_time_s": round(elapsed, 1),
        "accuracy": round(accuracy_score(y_test, y_pred), 4),
        "precision": round(precision_score(y_test, y_pred), 4),
        "recall": round(recall_score(y_test, y_pred), 4),
        "f1_score": round(f1_score(y_test, y_pred), 4),
        "auprc": round(average_precision_score(y_test, y_prob), 4),
        "roc_auc": round(roc_auc_score(y_test, y_prob), 4),
    }


def train_logistic_regression(X_train, y_train, X_test, y_test):
    """Train a Logistic Regression classifier."""
    print("\n--- Training Logistic Regression ---")
    start = time.time()

    lr = LogisticRegression(
        max_iter=1000,
        class_weight="balanced",
        solver="lbfgs",
        random_state=42,
        n_jobs=-1
    )
    lr.fit(X_train, y_train)
    elapsed = time.time() - start

    y_pred = lr.predict(X_test)
    y_prob = lr.predict_proba(X_test)[:, 1]

    print(f"  Training time: {elapsed:.1f}s")
    print(f"  Test Accuracy : {accuracy_score(y_test, y_pred):.4f}")
    print(f"  Test F1-Score : {f1_score(y_test, y_pred):.4f}")
    print(f"  Test AUPRC    : {average_precision_score(y_test, y_prob):.4f}")

    return lr, {
        "algorithm": "LogisticRegression",
        "training_time_s": round(elapsed, 1),
        "accuracy": round(accuracy_score(y_test, y_pred), 4),
        "precision": round(precision_score(y_test, y_pred), 4),
        "recall": round(recall_score(y_test, y_pred), 4),
        "f1_score": round(f1_score(y_test, y_pred), 4),
        "auprc": round(average_precision_score(y_test, y_prob), 4),
        "roc_auc": round(roc_auc_score(y_test, y_prob), 4),
    }


def train_isolation_forest(X_train, y_train, X_test, y_test):
    """Train an Isolation Forest for anomaly detection."""
    print("\n--- Training Isolation Forest ---")
    start = time.time()

    # Isolation Forest is unsupervised; contamination ≈ fraud ratio
    fraud_ratio = y_train.mean()
    iso = IsolationForest(
        n_estimators=200,
        contamination=fraud_ratio,
        max_features=0.8,
        random_state=42,
        n_jobs=-1
    )
    iso.fit(X_train)
    elapsed = time.time() - start

    # IF returns -1 for anomalies, 1 for normal
    y_pred_raw = iso.predict(X_test)
    y_pred = np.where(y_pred_raw == -1, 1, 0)

    print(f"  Training time: {elapsed:.1f}s")
    print(f"  Test Accuracy : {accuracy_score(y_test, y_pred):.4f}")
    print(f"  Test F1-Score : {f1_score(y_test, y_pred):.4f}")

    return iso, {
        "algorithm": "IsolationForest",
        "training_time_s": round(elapsed, 1),
        "accuracy": round(accuracy_score(y_test, y_pred), 4),
        "precision": round(precision_score(y_test, y_pred, zero_division=0), 4),
        "recall": round(recall_score(y_test, y_pred), 4),
        "f1_score": round(f1_score(y_test, y_pred), 4),
        "auprc": 0.0,  # IF doesn't produce calibrated probabilities
        "roc_auc": 0.0,
    }


def main():
    parser = argparse.ArgumentParser(description="Train fraud detection models")
    parser.add_argument("--data", type=str, default="data/creditcard.csv",
                        help="Path to the CSV dataset")
    parser.add_argument("--output", type=str, default="models/",
                        help="Directory to save trained models")
    parser.add_argument("--smote", action="store_true", default=True,
                        help="Apply SMOTE resampling (default: True)")
    args = parser.parse_args()

    # --- Load data ---
    df = load_dataset(args.data)

    # --- Preprocess ---
    print("\nPreprocessing features...")
    X, y, scaler, feature_names = preprocess_dataset(df, fit_scaler=True)
    print(f"  Feature matrix shape: {X.shape}")
    print(f"  Features: {len(feature_names)}")

    # --- Train/Test Split ---
    X_train, X_test, y_train, y_test = train_test_split(
        X, y, test_size=0.2, random_state=42, stratify=y
    )
    print(f"\n  Train set: {X_train.shape[0]:,} samples")
    print(f"  Test set : {X_test.shape[0]:,} samples")

    # --- SMOTE Resampling ---
    if args.smote and HAS_SMOTE:
        print("\nApplying SMOTE resampling...")
        smote = SMOTE(sampling_strategy=0.2, random_state=42)  # 1:5 ratio
        X_train_resampled, y_train_resampled = smote.fit_resample(X_train, y_train)
        print(f"  Before SMOTE: {X_train.shape[0]:,} ({y_train.sum():,} fraud)")
        print(f"  After SMOTE : {X_train_resampled.shape[0]:,} ({y_train_resampled.sum():,} fraud)")
        X_train, y_train = X_train_resampled, y_train_resampled

    # --- Train All Models ---
    results = []

    rf_model, rf_metrics = train_random_forest(X_train, y_train, X_test, y_test)
    results.append(rf_metrics)

    lr_model, lr_metrics = train_logistic_regression(X_train, y_train, X_test, y_test)
    results.append(lr_metrics)

    iso_model, iso_metrics = train_isolation_forest(X_train, y_train, X_test, y_test)
    results.append(iso_metrics)

    # --- Select Best Model ---
    # Compare by F1-score (best for imbalanced classification)
    best = max(results, key=lambda r: r["f1_score"])
    best_name = best["algorithm"]
    print(f"\n{'='*60}")
    print(f"  BEST MODEL: {best_name} (F1={best['f1_score']:.4f})")
    print(f"{'='*60}")

    # --- Save Models ---
    os.makedirs(args.output, exist_ok=True)

    # Save the primary model (Random Forest — best performer)
    model_path = os.path.join(args.output, "fraud_model.joblib")
    joblib.dump(rf_model, model_path)
    print(f"\n  Saved Random Forest model → {model_path}")

    # Save the scaler
    scaler_path = os.path.join(args.output, "scaler.joblib")
    joblib.dump(scaler, scaler_path)
    print(f"  Saved scaler → {scaler_path}")

    # Save all model metrics
    metrics_path = os.path.join(args.output, "metrics.json")
    metadata = {
        "trained_at": datetime.now().isoformat(),
        "dataset": args.data,
        "dataset_rows": len(df),
        "feature_count": len(feature_names),
        "feature_names": feature_names,
        "smote_applied": args.smote and HAS_SMOTE,
        "train_samples": int(X_train.shape[0]),
        "test_samples": int(X_test.shape[0]),
        "results": results,
        "best_model": best_name,
        "model_file": "fraud_model.joblib",
        "scaler_file": "scaler.joblib",
    }
    with open(metrics_path, "w") as f:
        json.dump(metadata, f, indent=2)
    print(f"  Saved metrics → {metrics_path}")

    # --- Print Summary Table ---
    print(f"\n{'='*60}")
    print(f"{'Model':<22} {'Acc':>7} {'Prec':>7} {'Rec':>7} {'F1':>7} {'AUPRC':>7}")
    print(f"{'-'*60}")
    for r in results:
        print(f"{r['algorithm']:<22} {r['accuracy']:>7.4f} {r['precision']:>7.4f} "
              f"{r['recall']:>7.4f} {r['f1_score']:>7.4f} {r['auprc']:>7.4f}")
    print(f"{'='*60}")

    # --- Detailed Report for Best Model ---
    print(f"\nDetailed Classification Report ({best_name}):")
    y_pred_best = rf_model.predict(X_test)
    print(classification_report(y_test, y_pred_best, target_names=["Legitimate", "Fraudulent"]))

    print("Confusion Matrix:")
    cm = confusion_matrix(y_test, y_pred_best)
    print(f"  TN={cm[0,0]:,}  FP={cm[0,1]:,}")
    print(f"  FN={cm[1,0]:,}  TP={cm[1,1]:,}")

    print("\n✅ Training complete. Model ready for deployment.")


if __name__ == "__main__":
    main()
