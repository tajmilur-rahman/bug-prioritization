
import numpy as np

__all__ = ["brier_multiclass", "ece"]

def brier_multiclass(probs: np.ndarray, y_true: np.ndarray) -> float:
    N, C = probs.shape
    y1 = np.eye(C, dtype=float)[y_true]
    return float(np.mean(np.sum((probs - y1) ** 2, axis=1)))

def ece(probs: np.ndarray, y_true: np.ndarray, n_bins: int = 15) -> float:
    conf = probs.max(axis=1)
    y_pred = probs.argmax(axis=1)
    acc = (y_pred == y_true).astype(float)
    bins = np.linspace(0.0, 1.0, n_bins + 1)
    ece_val = 0.0
    for i in range(n_bins):
        mask = (conf > bins[i]) & (conf <= bins[i+1])
        if not np.any(mask):
            continue
        bin_acc = acc[mask].mean()
        bin_conf = conf[mask].mean()
        ece_val += (mask.mean()) * abs(bin_acc - bin_conf)
    return float(ece_val)

# -----------------------------------------------------------
# metric_hybrid.py — hybrid scoring for ranking ML models
# -----------------------------------------------------------

import numpy as np
from sklearn.metrics import (
    f1_score,
    balanced_accuracy_score,
    confusion_matrix
)


def macro_f1(y, p):
    return f1_score(y, p, average="macro")

def head_class_f1(y, p, rare_classes=None):
    if rare_classes is None:
        return macro_f1(y, p)
    f = {}
    for c in rare_classes:
        f[c] = f1_score((y == c), (p == c))
    return np.mean(list(f.values()))

def hybrid_score(y_true, probs, rare_classes=None):
    preds = probs.argmax(1)

    F1_macro = macro_f1(y_true, preds)
    BA = balanced_accuracy_score(y_true, preds)
    ECE_ = ece(probs, y_true)
    RareF1 = head_class_f1(y_true, preds, rare_classes)

    # combine (tuned for severity classification)
    score = (
        0.45 * F1_macro +
        0.25 * BA +
        0.20 * RareF1 +
        0.10 * (1 - ECE_)
    )
    return {
        "macro_f1": float(F1_macro),
        "balanced_accuracy": float(BA),
        "rare_f1": float(RareF1),
        "ece": float(ECE_),
        "hybrid": float(score),
    }
