"""
imbalance.py — Unified imbalance handling for MLP, XGB, LGBM, CatBoost.

Outputs (standardized model-agnostic schema):
  - strategy
  - class_weights
  - loss_name    (MLP only)
  - loss_params  (MLP only)
  - smote        (bool)
  - scale_pos_weight (optional for XGB)
  
Supports:
    • Class weights
    • Focal loss
    • Class-balanced focal loss
    • SMOTE–Tomek with protected feature columns
"""

from __future__ import annotations
import os
from collections import Counter
from typing import Dict, Tuple, Optional, Sequence

import numpy as np


__all__ = [
    "compute_class_weights",
    "choose_imbalance_strategy",
    "apply_smote_tomek",
]


# ============================================================
# 1. CLASS WEIGHTS
# ============================================================

def compute_class_weights(
    y_train: Sequence,
    clip_min: float = None,
    clip_max: float = None,
    normalize_mean: bool = True,
) -> Dict[int, float]:
    counts = Counter(y_train)
    labels, freqs = zip(*sorted(counts.items()))
    N, C = sum(freqs), len(freqs)

    # Inverse frequency weighting
    raw = np.array([N / (C * f) for f in freqs], dtype=float)

    # clipping
    if clip_min is None:
        clip_min = float(os.getenv("CLASS_WEIGHT_CLIP_MIN", 1.0))
    if clip_max is None:
        clip_max = float(os.getenv("CLASS_WEIGHT_CLIP_MAX", 12.0))  # optional improvement

    clipped = np.clip(raw, clip_min, clip_max)

    if normalize_mean:
        clipped = clipped / clipped.mean()

    return dict(zip(labels, clipped.tolist()))


# ---------------------------------------------------------------------
# 2. Imbalance Strategy Chooser
# ---------------------------------------------------------------------
def choose_imbalance_strategy(
    y_train: Sequence,
    algo: str,
) -> Dict:
    """
    Returns a unified specification used by trainers:

    {
      "strategy": "weighted_ce" | "focal" | "cb_focal" | "class_weight" | "smote_tomek",
      "loss_name": "...",
      "loss_params": { ... },
      "class_weights": { ... },
      "scale_pos_weight": float or None,
      "smote": bool
    }
    """

    # Manual override via .env
    manual = os.getenv("IMBALANCE", "auto").lower()
    if manual != "auto":
        return {
            "strategy": manual,
            "loss_name": manual,
            "loss_params": {},
            "class_weights": None,
            "scale_pos_weight": None,
            "smote": False,
        }

    cnt = Counter(y_train)
    labels, freqs = zip(*sorted(cnt.items()))
    IR = max(freqs) / max(1, min(freqs))

    IR_mild = float(os.getenv("AUTO_IMBAL_IR_MILD", 3))
    IR_mod  = float(os.getenv("AUTO_IMBAL_IR_MODERATE", 8))
    min_samples = int(os.getenv("MINORITY_MIN_SAMPLES", 400))

    # -------------------------------------------------------
    # TREE MODELS
    # -------------------------------------------------------
    if algo.lower() in ("xgboost", "lightgbm", "catboost"):

        class_weights = compute_class_weights(y_train)

        if IR > IR_mod and min(freqs) < min_samples:
            # recommend SMOTE
            return {
                "strategy": "smote_tomek",
                "loss_name": None,
                "loss_params": {},
                "class_weights": class_weights,
                "scale_pos_weight": None,
                "smote": True,
            }

        return {
            "strategy": "class_weight",
            "loss_name": None,
            "loss_params": {},
            "class_weights": class_weights,
            "scale_pos_weight": None,
            "smote": False,
        }

    # -------------------------------------------------------
    # MLP (PyTorch)
    # -------------------------------------------------------
    if algo.lower() == "mlp":

        class_weights = compute_class_weights(y_train)

        # Mild imbalance → Weighted CE
        if IR <= IR_mild:
            return {
                "strategy": "weighted_ce",
                "loss_name": "weighted_ce",
                "loss_params": {"class_weights": list(class_weights.values())},
                "class_weights": class_weights,
                "scale_pos_weight": None,
                "smote": False,
            }

        # Moderate → Focal Loss
        if IR <= IR_mod:
            gamma = float(os.getenv("FOCAL_GAMMA", 1.5))
            return {
                "strategy": "focal",
                "loss_name": "focal",
                "loss_params": {
                    "class_weights": list(class_weights.values()),
                    "gamma": gamma,
                },
                "class_weights": class_weights,
                "scale_pos_weight": None,
                "smote": False,
            }

        # Heavy imbalance → CB Focal Loss
        # (better for extreme S1-sparse distribution)
        return {
            "strategy": "cb_focal",
            "loss_name": "cb_focal",
            "loss_params": {
                "class_counts": list(freqs),      # correct ordering after remap
                "gamma": float(os.getenv("FOCAL_GAMMA", 1.5)),
                "beta": 0.999
            },
            "class_weights": class_weights,
            "scale_pos_weight": None,
            "smote": False,
        }

    # default fallback
    return {
        "strategy": "class_weight",
        "loss_name": "weighted_ce",
        "loss_params": {"class_weights": list(compute_class_weights(y_train).values())},
        "class_weights": compute_class_weights(y_train),
        "scale_pos_weight": None,
        "smote": False,
    }


# ============================================================
# 3. SMOTE–TOMEK with SAFE protected columns
# ============================================================

def apply_smote_tomek(
    X_train,
    y_train,
    schema,
    enabled: bool,
):
    """
    Block-aware SMOTE-Tomek:
      • Interpolates ONLY numeric + categorical PCA blocks.
      • All other blocks are protected (no interpolation).
    
    schema: from prep (schema.json)
    """
    if not enabled:
        return X_train, y_train

    try:
        from imblearn.combine import SMOTETomek
    except:
        raise RuntimeError("Install imbalanced-learn: pip install imbalanced-learn")

    X = np.asarray(X_train)

    # ---- Determine protected & smote blocks ----
    # PCA blocks, categorical PCA, sims (high dimensioal blocks) must NOT be synthesized by SMOTE.
    prot = []
    smote = []

    offset = 0

    for name in schema["used_blocks"]:
        width = schema["blocks"][name]

        if name in ("numeric", "categorical"):
            smote.extend(list(range(offset, offset + width)))
        else:
            prot.extend(list(range(offset, offset + width)))

        offset += width

    smote = np.array(smote)
    prot = np.array(prot)

    # If no safe SMOTE block exists → skip
    if smote.size == 0:
        return X_train, y_train

    X_sm = X[:, smote]
    X_prot = X[:, prot]

    sm = SMOTETomek()
    X_sm_res, y_res = sm.fit_resample(X_sm, y_train)

    # Repeat protected part
    reps = int(np.ceil(len(y_res) / len(y_train)))
    X_prot_rep = np.tile(X_prot, (reps, 1))[:len(y_res)]

    # Recombine blocks (order preserved)
    # We reconstruct X_res in original dimensional order
    D = X.shape[1]
    X_res = np.zeros((len(y_res), D), dtype=X.dtype)
    X_res[:, smote] = X_sm_res
    X_res[:, prot] = X_prot_rep

    return X_res, y_res

