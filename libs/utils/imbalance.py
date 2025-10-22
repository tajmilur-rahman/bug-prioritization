
from __future__ import annotations
import os
from collections import Counter
from typing import Dict, Tuple, Optional, Sequence

import numpy as np

__all__ = [
    "class_weights_from_counts",
    "choose_imbalance_strategy",
    "apply_smote_tomek_if_needed",
]

def class_weights_from_counts(
    y_train: Sequence,
    clip_min: float = None,
    clip_max: float = None,
    normalize_mean: bool = True,
) -> Dict[int, float]:
    counts = Counter(y_train)
    labels, freqs = zip(*sorted(counts.items()))
    N, C = sum(freqs), len(freqs)
    raw = np.array([N / (C * f) for f in freqs], dtype=float)
    if clip_min is None:
        clip_min = float(os.getenv("CLASS_WEIGHT_CLIP_MIN", 1.0))
    if clip_max is None:
        clip_max = float(os.getenv("CLASS_WEIGHT_CLIP_MAX", 5.0))
    clipped = np.clip(raw, clip_min, clip_max)
    if normalize_mean:
        clipped = clipped / clipped.mean()
    return dict(zip(labels, clipped.tolist()))

def _imbalance_ratio(y_train: Sequence) -> float:
    cnt = Counter(y_train)
    n_min = min(cnt.values())
    n_max = max(cnt.values())
    return float(n_max) / float(max(1, n_min))

def choose_imbalance_strategy(y_train: Sequence, algo: str) -> Tuple[str, dict]:
    """Return (strategy, params) based on env thresholds and algorithm.
    Strategies: 'class_weight', 'weighted_ce', 'focal', 'smote_tomek'
    """
    if os.getenv("IMBALANCE", "auto") != "auto":
        return os.environ["IMBALANCE"], {}

    IR_mild = float(os.getenv("AUTO_IMBAL_IR_MILD", 3))
    IR_mod  = float(os.getenv("AUTO_IMBAL_IR_MODERATE", 8))
    n_min_thresh = int(os.getenv("MINORITY_MIN_SAMPLES", 500))

    cnt = Counter(y_train)
    n_min = min(cnt.values())
    IR = _imbalance_ratio(y_train)

    if algo in ("xgboost", "lightgbm", "catboost"):
        if IR > IR_mod and n_min < n_min_thresh:
            return "smote_tomek", {}
        else:
            return "class_weight", {}

    elif algo == "mlp":
        if IR <= IR_mild:
            return "weighted_ce", {}
        elif IR <= IR_mod and n_min >= n_min_thresh:
            return "weighted_ce", {}
        else:
            gamma = float(os.getenv("FOCAL_GAMMA", 1.5))
            return "focal", {"gamma": gamma}

    return "class_weight", {}

def apply_smote_tomek_if_needed(
    X_train, y_train, enabled: bool, exclude_cols: Optional[Sequence[int]] = None
):
    """Apply SMOTE+Tomek on train only. Requires imblearn.
    - exclude_cols: optional list of column indices to exclude from interpolation (e.g., hashed actors).
    Returns X_res, y_res.
    """
    if not enabled:
        return X_train, y_train

    try:
        from imblearn.combine import SMOTETomek
    except Exception as e:
        raise RuntimeError("SMOTE-Tomek requires imblearn. Install with: pip install imbalanced-learn") from e

    import numpy as np
    X_arr = np.asarray(X_train)

    if exclude_cols:
        mask = np.ones(X_arr.shape[1], dtype=bool)
        mask[np.array(exclude_cols)] = False
        X_cont = X_arr[:, mask]
        X_keep = X_arr[:, ~mask]

        smt = SMOTETomek()
        Xc_res, yr = smt.fit_resample(X_cont, y_train)
        # Reattach protected columns by repeating rows to match new length
        reps = int(np.ceil(len(yr) / len(y_train)))
        X_keep_rep = np.tile(X_keep, (reps, 1))[:len(yr)]
        X_res = np.concatenate([Xc_res, X_keep_rep], axis=1)
        return X_res, yr
    else:
        smt = SMOTETomek()
        return smt.fit_resample(X_arr, y_train)
