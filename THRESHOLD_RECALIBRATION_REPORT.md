# Threshold Recalibration Report

> Answers one question: **why did live precision/recall (~84% recall, ~6% precision) look so different from the numbers in the notebook (~81% recall, ~30% precision), and what exactly was changed to fix it?**

**Short answer:** nothing about the trained model changed. One number — the decision threshold used to turn a probability into a "fraud" flag — was recalibrated because the training data's fraud rate (13%) doesn't match live traffic's fraud rate (2%), which is a base-rate mismatch, not a modeling error. No retraining was needed or performed.

---

## 1. The symptom

| | Recall | Precision |
|---|---|---|
| Notebook (`fraud-detection-xgboost.ipynb`, test set) | 80.65% | 29.52% |
| Live pipeline (observed) | ~84% | ~6% |

Recall roughly matched. Precision collapsed. That asymmetry is the clue.

## 2. Root cause: training data and live data have different fraud rates

**Proof — training/test data fraud rate**, from the notebook's own split cell (`fraud-detection-xgboost.ipynb`, cell `fde050e9`):

```
Training Set:   Fraud rate: 13.06%
Validation Set: Fraud rate: 13.18%
Test Set:       Fraud rate: 13.20%
```

Traced upstream to `Syn_dataset/dataset_generator.py:274`:
```python
# Inject 5% fraud
is_synthetic_fraud = random.random() < 0.05
```
This 5% gate, combined with the probabilistic fraud-score labeling and noise injection right after it (lines 296-305), is what produces the ~13% final `fraud_label` rate seen in the CSV.

**Proof — live traffic fraud rate**, from `producer/faker_producer.py:115`:
```python
is_fraud = random.random() < 0.02
```

So the model was thresholded on a validation set with **13% fraud**, then deployed against traffic with **2% fraud** — a 6.5x difference in how many legitimate transactions surround every fraud case.

## 3. Why this explains precision collapsing but not recall

- **Recall** = TP / (TP + FN) — computed only over actual fraud rows. It doesn't care how many legitimate transactions exist alongside them, so it transfers from a 13%-fraud test set to a 2%-fraud live stream almost unchanged. (80.65% notebook vs. ~84% live — consistent.)
- **Precision** = TP / (TP + FP) — FP count scales with how many legitimate transactions the model sees, which scales with how rare fraud actually is. The same true-positive-rate/false-positive-rate behavior produces a *much* worse precision when fraud is rarer.

This isn't a rule of thumb, it's exact — via Bayes' theorem:
```
precision(π) = (π · TPR) / (π · TPR + (1 - π) · FPR)
```
where π is the fraud prevalence, and TPR/FPR are the model's true/false positive rates at a given threshold (these *are* prevalence-invariant, since they're computed conditional on the true class).

**Proof this is the actual explanation, not just a plausible story** — reprojecting the *old* threshold (0.51)'s TPR/FPR from 13% down to the real 2% live prevalence gives:

```
precision(0.02) = 5.38%
recall(0.02)    = 81.6%
```

That's your reported ~84% recall / ~6% precision, reproduced almost exactly from the notebook's own model, with no retraining — just re-deriving what precision *should* be at the real-world base rate. (Full numbers in `ml_service/model_metadata.json` → `performance_live_2pct.old_threshold_under_same_prevalence`.)

## 4. What was actually changed

**One field**, via `scripts/recalibrate_threshold.py`. Full diff of `ml_service/model_metadata.json`:

```diff
-    "optimal_threshold": 0.5100000000000001,
+    "optimal_threshold": 0.7867072224617004,
     "scale_pos_weight": 6.658336098843591,
     ...
+    "performance_live_2pct": {
+        "threshold": 0.7867072224617004,
+        "precision": 0.13871203671871063,
+        "recall": 0.3207121172827264,
+        "f1": 0.19366256911480464,
+        "prevalence_assumed": 0.02,
+        "precision_floor_used": 0.1,
+        "selection_rule": "max F1 subject to precision >= 0.1",
+        "old_threshold_under_same_prevalence": {
+            "threshold": 0.5099958181381226,
+            "precision": 0.053800597164929874,
+            "recall": 0.8162346874952328,
+            "f1": 0.1009474314162472
+        }
+    }
```

**What did *not* change** (verifiable via `git status` / `git log`):
- `ml_service/fraud_detection_model.json` (the actual XGBoost weights) — no uncommitted diff, untouched since it was first committed (`56bd0fc`).
- Feature engineering, training hyperparameters, `scale_pos_weight`, the training data itself — all untouched.
- The model's ranking ability (AUC ≈ 0.84) — untouched, since AUC/TPR/FPR don't depend on the classification threshold at all.

The recalibration script (`scripts/recalibrate_threshold.py`) reloads the *existing* model, re-scores the held-out test set, builds the ROC curve, reprojects precision at each threshold to the real 2% prevalence via the formula above, then picks the threshold that maximizes F1 subject to a minimum precision floor (10%, configurable via `PRECISION_FLOOR`). It does not call `.fit()` anywhere — this is threshold selection, not training.

## 5. Metrics: before vs. after, at real 2% live prevalence

| Threshold | Precision | Recall | F1 | Note |
|---|---|---|---|---|
| 0.51 (old, notebook default) | 5.4% | 81.6% | 10.1% | What you were observing live |
| 0.7867 (recalibrated) | 13.9% | 32.1% | 19.4% | Current live setting |

This is a real trade-off, not free lunch: the recalibrated threshold roughly **doubles alert precision** (fewer wasted investigations) at the cost of **dropping recall from ~82% to ~32%** (missing more actual fraud). Both numbers above are honest, live-prevalence-correct estimates from the same underlying model — pick whichever operating point matches your risk tolerance. If you want recall back near 80%+, the precision floor would need to come down toward ~5%, which is mathematically what the old threshold already was doing.

## 6. Why retraining wasn't necessary (and wouldn't have fixed this)

The problem was never the model's ability to separate fraud from non-fraud (AUC 0.84, unaffected by any of this). It was that the threshold-selection step in the notebook (cell `2d53ddd0`, "target recall ≥ 0.80, then maximize precision") searched for precision on a 13%-fraud validation set and reported that number as if it would hold live. Retraining the same architecture on the same 13%-fraud data, then re-running the same threshold search, reproduces the identical mismatch. The fix is scoping the precision estimate to the real deployment prevalence — which is exactly what happened here, without touching model weights.

If you want the notebook itself to report honest, live-accurate numbers going forward (instead of relying on this separate recalibration script after the fact), the fix is to change cell `2d53ddd0`'s threshold search to apply the same Bayes reprojection to 2% prevalence before picking a threshold — not to retrain.
