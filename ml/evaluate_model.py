"""
Model Evaluation & Visualization Script
========================================
Loads a trained model and generates evaluation plots:
- Confusion matrix heatmap
- ROC curve
- Precision-Recall curve
- Feature importance bar chart

Usage:
    python evaluate_model.py --model models/fraud_model.joblib --data data/creditcard.csv
"""

import os
import argparse
import numpy as np
import pandas as pd
import joblib

from sklearn.metrics import (
    confusion_matrix, classification_report, roc_curve, auc,
    precision_recall_curve, average_precision_score, f1_score
)

from feature_engineering import preprocess_dataset

# Try matplotlib; if running headless, use Agg backend
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import matplotlib.ticker as ticker


def plot_confusion_matrix(y_test, y_pred, output_dir):
    """Generate and save a confusion matrix heatmap."""
    cm = confusion_matrix(y_test, y_pred)
    fig, ax = plt.subplots(figsize=(6, 5))

    im = ax.imshow(cm, interpolation="nearest", cmap="Blues")
    ax.figure.colorbar(im, ax=ax)

    labels = ["Legitimate", "Fraudulent"]
    ax.set(xticks=[0, 1], yticks=[0, 1],
           xticklabels=labels, yticklabels=labels,
           ylabel="Actual", xlabel="Predicted",
           title="Confusion Matrix")

    # Annotate cells with counts
    for i in range(2):
        for j in range(2):
            color = "white" if cm[i, j] > cm.max() / 2 else "black"
            ax.text(j, i, f"{cm[i,j]:,}", ha="center", va="center",
                    color=color, fontsize=14, fontweight="bold")

    plt.tight_layout()
    path = os.path.join(output_dir, "confusion_matrix.png")
    plt.savefig(path, dpi=150)
    plt.close()
    print(f"  Saved: {path}")


def plot_roc_curve(y_test, y_prob, output_dir):
    """Generate and save a ROC curve."""
    fpr, tpr, _ = roc_curve(y_test, y_prob)
    roc_auc = auc(fpr, tpr)

    fig, ax = plt.subplots(figsize=(6, 5))
    ax.plot(fpr, tpr, color="#2563eb", lw=2, label=f"ROC (AUC = {roc_auc:.3f})")
    ax.plot([0, 1], [0, 1], "k--", lw=1, alpha=0.5)
    ax.set(xlim=[0, 1], ylim=[0, 1.02],
           xlabel="False Positive Rate", ylabel="True Positive Rate",
           title="Receiver Operating Characteristic (ROC)")
    ax.legend(loc="lower right")
    ax.grid(alpha=0.3)

    plt.tight_layout()
    path = os.path.join(output_dir, "roc_curve.png")
    plt.savefig(path, dpi=150)
    plt.close()
    print(f"  Saved: {path}")


def plot_precision_recall(y_test, y_prob, output_dir):
    """Generate and save a Precision-Recall curve."""
    precision, recall, _ = precision_recall_curve(y_test, y_prob)
    ap = average_precision_score(y_test, y_prob)

    fig, ax = plt.subplots(figsize=(6, 5))
    ax.plot(recall, precision, color="#059669", lw=2, label=f"AP = {ap:.3f}")
    ax.set(xlim=[0, 1], ylim=[0, 1.02],
           xlabel="Recall", ylabel="Precision",
           title="Precision-Recall Curve")
    ax.legend(loc="upper right")
    ax.grid(alpha=0.3)

    plt.tight_layout()
    path = os.path.join(output_dir, "precision_recall_curve.png")
    plt.savefig(path, dpi=150)
    plt.close()
    print(f"  Saved: {path}")


def plot_feature_importance(model, feature_names, output_dir, top_n=15):
    """Plot the top-N most important features from a tree-based model."""
    if not hasattr(model, "feature_importances_"):
        print("  [SKIP] Model does not support feature_importances_")
        return

    importances = model.feature_importances_
    indices = np.argsort(importances)[::-1][:top_n]

    fig, ax = plt.subplots(figsize=(8, 5))
    ax.barh(range(top_n), importances[indices][::-1], color="#7c3aed")
    ax.set_yticks(range(top_n))
    ax.set_yticklabels([feature_names[i] for i in indices][::-1])
    ax.set_xlabel("Importance")
    ax.set_title(f"Top {top_n} Feature Importances (Random Forest)")
    ax.grid(axis="x", alpha=0.3)

    plt.tight_layout()
    path = os.path.join(output_dir, "feature_importance.png")
    plt.savefig(path, dpi=150)
    plt.close()
    print(f"  Saved: {path}")


def main():
    parser = argparse.ArgumentParser(description="Evaluate trained fraud model")
    parser.add_argument("--model", type=str, default="models/fraud_model.joblib")
    parser.add_argument("--scaler", type=str, default="models/scaler.joblib")
    parser.add_argument("--data", type=str, default="data/creditcard.csv")
    parser.add_argument("--output", type=str, default="models/plots/")
    args = parser.parse_args()

    os.makedirs(args.output, exist_ok=True)

    # Load model and scaler
    print(f"Loading model from {args.model}...")
    model = joblib.load(args.model)
    scaler = joblib.load(args.scaler)

    # Load and preprocess data
    df = pd.read_csv(args.data)
    X, y, _, feature_names = preprocess_dataset(df, fit_scaler=False, scaler=scaler)

    # Use last 20% as test set (same split as training)
    split_idx = int(len(X) * 0.8)
    X_test, y_test = X[split_idx:], y[split_idx:]

    # Predictions
    y_pred = model.predict(X_test)
    y_prob = model.predict_proba(X_test)[:, 1] if hasattr(model, "predict_proba") else None

    # Print report
    print("\n" + "=" * 60)
    print("Classification Report:")
    print("=" * 60)
    print(classification_report(y_test, y_pred, target_names=["Legitimate", "Fraudulent"]))

    # Generate plots
    print("\nGenerating evaluation plots...")
    plot_confusion_matrix(y_test, y_pred, args.output)
    if y_prob is not None:
        plot_roc_curve(y_test, y_prob, args.output)
        plot_precision_recall(y_test, y_prob, args.output)
    plot_feature_importance(model, feature_names, args.output)

    print("\n✅ Evaluation complete.")


if __name__ == "__main__":
    main()
