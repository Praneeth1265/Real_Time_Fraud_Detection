"""
Recalibrates the model's decision threshold for the *live* fraud prevalence
(the producer injects ~2% fraud), rather than the training-set prevalence
(~13%, implied by scale_pos_weight=6.658 in model_metadata.json).

The model itself is NOT retrained -- its ranking of transactions by risk
doesn't need to change, only the operating point (threshold) at which we
call a transaction "fraud". TPR/FPR are prevalence-invariant (computed
conditional on the true class), so we can reconstruct precision at any
target prevalence pi via Bayes' theorem:

    precision(pi) = (pi * TPR) / (pi * TPR + (1 - pi) * FPR)

Run inside a container with xgboost/pandas/scikit-learn installed (the host
Python env has none of these):

    docker build -t fraud-recalibrate -f scripts/Dockerfile.recalibrate .
    docker run --rm -v "$(pwd):/work" -w /work fraud-recalibrate \
        python scripts/recalibrate_threshold.py
"""
import json
import os
import zipfile

import numpy as np
import pandas as pd
import xgboost as xgb
from sklearn.metrics import roc_curve

REPO_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
ZIP_PATH = os.path.join(REPO_ROOT, "Syn_dataset.zip")
EXTRACT_DIR = os.path.join(REPO_ROOT, "Syn_dataset")
CSV_PATH = os.path.join(EXTRACT_DIR, "fraud_dataset_5l.csv")

MODEL_PATH = os.path.join(REPO_ROOT, "ml_service", "fraud_detection_model.json")
METADATA_PATH = os.path.join(REPO_ROOT, "ml_service", "model_metadata.json")

LIVE_PREVALENCE = float(os.environ.get("LIVE_PREVALENCE", "0.02"))  # matches producer's is_fraud rate
# A precision floor much above ~0.15 forces the threshold so high that recall
# collapses (empirically: floor=0.50 -> threshold=0.88 -> recall=4%, i.e. the
# model misses 96% of real fraud to keep alerts "clean"). In fraud detection
# a missed fraud case is normally far costlier than a false alarm, so a low
# floor that stays out of the way of the natural max-F1 operating point is
# the safer default -- raise it only if the false-alarm *volume* becomes the
# actual operational bottleneck.
PRECISION_FLOOR = float(os.environ.get("PRECISION_FLOOR", "0.10"))


def ensure_dataset_extracted():
    if not os.path.exists(CSV_PATH):
        print(f"Extracting {ZIP_PATH} ...", flush=True)
        with zipfile.ZipFile(ZIP_PATH) as zf:
            zf.extractall(REPO_ROOT)


def live_precision(prevalence, tpr, fpr):
    denom = prevalence * tpr + (1 - prevalence) * fpr
    return np.where(denom > 0, (prevalence * tpr) / np.where(denom > 0, denom, 1), 0.0)


def live_f1(precision, recall):
    denom = precision + recall
    return np.where(denom > 0, 2 * precision * recall / np.where(denom > 0, denom, 1), 0.0)


def main():
    ensure_dataset_extracted()

    with open(METADATA_PATH) as f:
        metadata = json.load(f)
    feature_columns = metadata["feature_columns"]
    old_threshold = metadata.get("optimal_threshold", 0.5)

    print(f"Loading model from {MODEL_PATH} ...", flush=True)
    model = xgb.Booster()
    model.load_model(MODEL_PATH)

    print(f"Loading dataset from {CSV_PATH} ...", flush=True)
    df = pd.read_csv(CSV_PATH)

    X = df[feature_columns]
    y = df["fraud_label"].values

    dmatrix = xgb.DMatrix(X, feature_names=feature_columns)
    probs = model.predict(dmatrix)

    fpr, tpr, thresholds = roc_curve(y, probs)
    # roc_curve's first threshold is +inf (a sentinel for "predict nothing
    # positive") -- drop it, it's not a usable operating point.
    fpr, tpr, thresholds = fpr[1:], tpr[1:], thresholds[1:]

    precision = live_precision(LIVE_PREVALENCE, tpr, fpr)
    f1 = live_f1(precision, tpr)

    def old_threshold_metrics():
        idx = int(np.argmin(np.abs(thresholds - old_threshold)))
        return thresholds[idx], precision[idx], tpr[idx], f1[idx]

    floor_mask = precision >= PRECISION_FLOOR
    if floor_mask.any():
        candidate_idx = np.where(floor_mask)[0]
        best_idx = candidate_idx[np.argmax(f1[candidate_idx])]
        selection_note = f"max F1 subject to precision >= {PRECISION_FLOOR}"
    else:
        best_idx = int(np.argmax(f1))
        selection_note = f"no threshold cleared the {PRECISION_FLOOR} precision floor; falling back to unconstrained max F1"

    new_threshold = float(thresholds[best_idx])
    new_precision = float(precision[best_idx])
    new_recall = float(tpr[best_idx])
    new_f1 = float(f1[best_idx])

    old_t, old_p, old_r, old_f = old_threshold_metrics()

    print("\n=== Threshold recalibration (live prevalence = {:.1%}) ===".format(LIVE_PREVALENCE))
    print(f"Selection rule: {selection_note}\n")
    print(f"{'':20s}{'threshold':>12s}{'precision':>12s}{'recall':>12s}{'f1':>12s}")
    print(f"{'old (0.51 F1-opt)':20s}{old_t:12.4f}{old_p:12.4f}{old_r:12.4f}{old_f:12.4f}")
    print(f"{'new (recalibrated)':20s}{new_threshold:12.4f}{new_precision:12.4f}{new_recall:12.4f}{new_f1:12.4f}")

    metadata["optimal_threshold"] = new_threshold
    metadata["performance_live_2pct"] = {
        "threshold": new_threshold,
        "precision": new_precision,
        "recall": new_recall,
        "f1": new_f1,
        "prevalence_assumed": LIVE_PREVALENCE,
        "precision_floor_used": PRECISION_FLOOR,
        "selection_rule": selection_note,
        "old_threshold_under_same_prevalence": {
            "threshold": float(old_t),
            "precision": float(old_p),
            "recall": float(old_r),
            "f1": float(old_f),
        },
    }

    with open(METADATA_PATH, "w") as f:
        json.dump(metadata, f, indent=4)

    print(f"\nWrote new optimal_threshold={new_threshold:.4f} to {METADATA_PATH}")


if __name__ == "__main__":
    main()
