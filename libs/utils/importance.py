"""
importance.py — Feature Importance for Hybrid MLP

Supports:
    - Permutation importance (fast global)
    - Integrated Gradients (global + local)
    - Block-level attribution (fusion-aware)
    - Uses block_schema.json (not schema.json)
"""

import os, json
import numpy as np
import torch
from torch.utils.data import DataLoader, TensorDataset
from copy import deepcopy

from captum.attr import IntegratedGradients


__all__ = [
    "get_block_schema",
    "slice_blocks",
    "permutation_importance",
    "integrated_gradients_local",
    "integrated_gradients_global",
    "block_importance",
    "compute_all_importance",
    "integrated_gradients_compare",
]


# ============================================================
# LOAD BLOCK SCHEMA
# ============================================================
def get_block_schema(schema):
    """
    schema["used_blocks"]: ordered list of block names
    schema["blocks"]: {block_name: width}
    """
    blocks = []
    offset = 0
    for name in schema["used_blocks"]:
        width = schema["blocks"][name]
        blocks.append({
            "name": name,
            "start": offset,
            "end": offset + width
        })
        offset += width
    return blocks
    
# ============================================================
# SLICE BLOCKS
# ============================================================

def slice_blocks(X: np.ndarray, schema):
    """
    Returns dict: name → X[:, start:end]
    """
    out = {}
    offset = 0
    for name in schema["used_blocks"]:
        width = schema["blocks"][name]
        start = offset
        end = offset + width
        out[name] = X[:, start:end]

    return out

# ============================================================
# INTERGRATED GRADIENT COMPARE
# ============================================================
def integrated_gradients_compare(
    model,
    X_sample,
    true_class=None,
    baseline=None,
    n_steps=50,
    device="cpu"
):
    """
    Returns:
        pred_class, true_class
        ig_pred: feature attributions for predicted class
        ig_true: feature attributions for true class (optional)
    """
    model.eval()
    xs = torch.tensor(X_sample[None, :], dtype=torch.float32).to(device)

    if baseline is None:
        baseline = torch.zeros_like(xs)

    # ---- predicted class ----
    with torch.no_grad():
        logits = model(xs)
        pred_class = logits.argmax(dim=1).item()

    ig = IntegratedGradients(model)

    # IG for predicted class
    ig_pred = ig.attribute(
        xs,
        baselines=baseline,
        n_steps=n_steps,
        target=pred_class
    ).detach().cpu().numpy().reshape(-1)

    # ---- IG for true class ----
    ig_true = None
    if true_class is not None:
        ig_true = ig.attribute(
            xs,
            baselines=baseline,
            n_steps=n_steps,
            target=int(true_class)
        ).detach().cpu().numpy().reshape(-1)

    return pred_class, true_class, ig_pred, ig_true


import matplotlib.pyplot as plt

def plot_compare_ig(feature_names, ig_pred, ig_true, pred_class, true_class):
    width = 0.35
    idx = np.arange(len(feature_names))

    plt.figure(figsize=(14,6))
    plt.title(f"Integrated Gradients — Predicted vs True Class\nPred={pred_class}, True={true_class}")

    plt.bar(idx - width/2, ig_pred, width, label=f"Predicted (class {pred_class})", alpha=0.7)
    if ig_true is not None:
        plt.bar(idx + width/2, ig_true, width, label=f"True (class {true_class})", alpha=0.7)

    plt.xticks(idx, feature_names, rotation=90)
    plt.legend()
    plt.tight_layout()
    plt.show()

# ============================================================
# FAST PERMUTATION IMPORTANCE
# ============================================================

# -----------------------------------------------------------
# perm_importance.py — FAST grouped permutation importance
# -----------------------------------------------------------

import numpy as np
import torch

@torch.no_grad()
def permutation_importance_grouped(
    model,
    X_val,
    y_val,
    schema,
    metric_fn,
    device="cuda",
    batch_size=2048,
    per_feature=False
):
    blocks = get_block_schema(schema)
    """
    blocks: list of dicts
        [{ "name": "text_pca", "start": 0, "end": 128 },
         { "name": "topic_pca", "start": 128, "end": 144 }, ...]

    Returns:
        block_scores: {block_name: score}
        feature_scores: np.ndarray (optional, only if per_feature=True)
    """

    model.eval()
    X_val = torch.tensor(X_val, dtype=torch.float32, device=device)
    y_val = torch.tensor(y_val, device=device)

    # baseline metric
    logits = model(X_val)
    baseline = metric_fn(logits, y_val)

    block_scores = {}
    full_feature_scores = np.zeros(X_val.shape[1], dtype=np.float32)

    for blk in blocks:
        s, e = blk["start"], blk["end"]
        width = e - s

        # fast permutation: permute same indices for entire block
        idx = torch.randperm(X_val.size(0), device=device)
        Xp = X_val.clone()
        Xp[:, s:e] = X_val[idx, s:e]

        logits_p = model(Xp)
        new_metric = metric_fn(logits_p, y_val)
        delta = float(baseline - new_metric)

        block_scores[blk["name"]] = delta

        if per_feature:
            # distribute group importance uniformly across features
            full_feature_scores[s:e] = delta / max(1, width)

    return block_scores, full_feature_scores if per_feature else block_scores


def permutation_importance(
    model,
    X: np.ndarray,
    y: np.ndarray,
    metric_fn,
    device="cpu",
    batch_size=4096
):
    """
    Fast implementation:
        - shuffle only once per feature
        - single forward pass per feature
    """

    model.eval()
    Xb = torch.tensor(X, dtype=torch.float32).to(device)
    yb = torch.tensor(y, dtype=torch.long).to(device)

    # Baseline
    with torch.no_grad():
        soft, _ = model(Xb)
        baseline = metric_fn(soft, yb)

    perm_scores = np.zeros(X.shape[1], dtype=np.float32)

    for j in range(X.shape[1]):
        X_perm = X.copy()
        np.random.shuffle(X_perm[:, j])   # in-place randomization

        Xp = torch.tensor(X_perm, dtype=torch.float32).to(device)
        with torch.no_grad():
            s2, _ = model(Xp)
            score = metric_fn(s2, yb)

        perm_scores[j] = baseline - score

    return perm_scores


# ============================================================
# INTEGRATED GRADIENTS (LOCAL)
# ============================================================

def integrated_gradients_local(
    model,
    X_sample: np.ndarray,
    baseline=None,
    n_steps=50,
    device="cpu",
):
    """
    X_sample: [1, D] torch float
    Returns: [D] attributions
    """
    model.eval()
    xs = torch.tensor(X_sample[None, :], dtype=torch.float32).to(device)

    if baseline is None:
        baseline = torch.zeros_like(xs)

    ig = IntegratedGradients(model.forward_softmax)

    at = ig.attribute(xs, baselines=baseline, n_steps=n_steps)
    return at.detach().cpu().numpy().reshape(-1)


# ============================================================
# GLOBAL IG (AVERAGE)
# ============================================================

def integrated_gradients_global(
    model,
    X: np.ndarray,
    n_samples=200,
    device="cpu",
):
    idx = np.random.choice(len(X), size=min(n_samples, len(X)), replace=False)

    scores = []

    for i in idx:
        scores.append(
            integrated_gradients_local(model, X[i], device=device)
        )

    return np.mean(np.abs(np.stack(scores, axis=0)), axis=0)


# ============================================================
# BLOCK-LEVEL IMPORTANCE
# ============================================================

def block_importance(perm_scores, block_schema):
    """
    Sum permutation importance over feature slices.
    """
    out = {}
    for blk in block_schema:
        s, e = blk["start"], blk["end"]
        out[blk["name"]] = float(perm_scores[s:e].sum())
    return out


# ============================================================
# MASTER WRAPPER
# ============================================================

def compute_all_importance(
    model,
    X,
    y,
    schema,
    metric_fn,
    device="cpu",
    save_dir="importance"
):
    os.makedirs(save_dir, exist_ok=True)

    block_schema = get_block_schema(schema)

    print("[importance] Permutation importance (Block-level)…")
    perm = permutation_importance_grouped(model, X, y, schema, metric_fn, device=device)
    np.save(os.path.join(save_dir, "perm_feature.npy"), perm)

    # print("[importance] Block-level…")
    # block_scores = block_importance(perm, block_schema)
    # json.dump(block_scores, open(os.path.join(save_dir, "block_importance.json"), "w"), indent=2)

    # print("[importance] Integrated gradients…")
    # ig = integrated_gradients_global(model, X, device=device)
    # np.save(os.path.join(save_dir, "ig_feature.npy"), ig)

    # print("[importance] DONE →", save_dir)

    return {"perm": perm, "ig": None}
