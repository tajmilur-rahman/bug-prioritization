"""
importance.py — Unified feature attribution for Hybrid MLP

Supports:
    ✓ Integrated Gradients (probability-based for ordinal models)
    ✓ Permutation importance (block-level + feature-level)
    ✓ Block-level attribution
    ✓ Local IG (single sample)
    ✓ Global IG (mean + variance)
    ✓ Per-class IG

All IG is computed on **class probabilities**, not threshold logits,
which fixes all Captum compatibility and interpretability problems.
"""

import os, json
import numpy as np
import torch
from torch import nn
from torch.utils.data import DataLoader, TensorDataset
from copy import deepcopy
import matplotlib.pyplot as plt

from captum.attr import IntegratedGradients


# ============================================================
# SCHEMA
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
# ORDINAL → PROBABILITY WRAPPER
# ============================================================

def ordinal_logits_to_probs(logits):
    """
    Convert ordinal logits (N, K-1) → probs (N, K)
    """
    s = torch.sigmoid(logits)
    N, Km1 = s.shape
    left = torch.cat([torch.ones(N, 1, device=logits.device), s], dim=1)
    right = torch.cat([s, torch.zeros(N, 1, device=logits.device)], dim=1)
    probs = (left - right).clamp_min(1e-8)
    return probs / probs.sum(dim=1, keepdim=True)


class ProbWrapper(nn.Module):
    """
    Wrapper so that IG can target probability outputs.

    Will return:
        probs = model(x) if softmax head
        ordinal_probs = ordinal_logits_to_probs(logits) otherwise
    """
    def __init__(self, model, ordinal=False):
        super().__init__()
        self.model = model
        self.ordinal = ordinal

    def forward(self, x):
        logits, _ = self.model(x)
        if self.ordinal:
            return ordinal_logits_to_probs(logits)
        return torch.softmax(logits, dim=1)


# ============================================================
# INTEGRATED GRADIENTS (per-sample)
# ============================================================

def integrated_gradients_compare(
    model,
    X_sample,
    true_class=None,
    baseline=None,
    n_steps=50,
    device="cpu",
    ordinal=True,
):
    """
    Compute IG attribution for:
        • predicted class
        • true class (optional)
    using probability wrapper.

    Returns:
        pred_class, true_class, ig_pred, ig_true
    """
    model.eval()

    xs = torch.tensor(X_sample[None, :], dtype=torch.float32).to(device)
    if baseline is None:
        baseline = torch.zeros_like(xs)

    # ---- wrap model for probs output ----
    wrapped = ProbWrapper(model, ordinal=ordinal)

    # ---- predicted class ----
    with torch.no_grad():
        probs = wrapped(xs).cpu().numpy()
    pred_class = int(np.argmax(probs, 1))

    if true_class is None:
        true_class = pred_class

    print(f"[IG] Predicted={pred_class}, True={true_class}")

    # ---- IG engine ----
    ig = IntegratedGradients(wrapped)

    # IG for predicted class
    ig_pred = ig.attribute(
        xs,
        baselines=baseline,
        target=pred_class,
        n_steps=n_steps
    ).detach().cpu().numpy().reshape(-1)

    # IG for true class
    ig_true = ig.attribute(
        xs,
        baselines=baseline,
        target=int(true_class),
        n_steps=n_steps
    ).detach().cpu().numpy().reshape(-1)

    return pred_class, true_class, ig_pred, ig_true


# ============================================================
# LOCAL IG (for probability wrapper)
# ============================================================

def integrated_gradients_local(model, X_sample, ordinal=True, baseline=None, n_steps=50, device="cpu", target_class=None):
    wrapped = ProbWrapper(model, ordinal=ordinal)
    wrapped.eval()

    xs = torch.tensor(X_sample[None, :], dtype=torch.float32).to(device)

    if baseline is None:
        baseline = torch.zeros_like(xs)
    else:
        baseline = torch.tensor(baseline[None, :], dtype=torch.float32).to(device)

    # If no target_class provided, use the predicted class as target
    if target_class is None:
        with torch.no_grad():
            probs = wrapped(xs)           # [1, C]
            target = probs.argmax(dim=1)  # tensor([k]) shape [1]
    else:
        # Allow either int or tensor
        if isinstance(target_class, int):
            target = torch.tensor([target_class], device=device)
        else:
            target = target_class.to(device)

    ig = IntegratedGradients(wrapped)
    at = ig.attribute(xs, baselines=baseline, n_steps=n_steps, target=target)
    return at.detach().cpu().numpy().reshape(-1)


# ============================================================
# GLOBAL IG
# ============================================================

def integrated_gradients_global(model, X, n_samples=200, ordinal=True, device="cpu"):
    """
    Returns global mean IG and variance IG.
    """
    idx = np.random.choice(len(X), size=min(n_samples, len(X)), replace=False)
    igs = []

    for i in idx:
        igs.append(integrated_gradients_local(model, X[i], ordinal=ordinal, device=device))

    igs = np.stack(igs, axis=0)
    return igs.mean(axis=0), igs.var(axis=0)


def per_class_global_ig(model, X, y, classes, ordinal=True, device="cpu", n_per_class=200):
    """
    Average IG per class.
    """
    out = {}
    for c in classes:
        idx = np.where(y == c)[0]
        if len(idx) == 0:
            continue
        take = np.random.choice(idx, size=min(n_per_class, len(idx)), replace=False)
        igs = []
        for i in take:
            igs.append(integrated_gradients_local(model, X[i], ordinal=ordinal, device=device))
        igs = np.stack(igs, axis=0)
        out[int(c)] = igs.mean(axis=0).tolist()
    return out


# ============================================================
# PERMUTATION IMPORTANCE (Block-level)
# ============================================================

@torch.no_grad()
def permutation_importance_grouped(
    model,
    X_val,
    y_val,
    schema,
    metric_fn,
    device="cuda",
    ordinal=True,
):
    """
    Returns:
        block_scores: {block_name: delta}
        feature_scores: np.ndarray
    """

    wrapped = ProbWrapper(model, ordinal=ordinal).to(device)
    wrapped.eval()

    X_val_t = torch.tensor(X_val, dtype=torch.float32, device=device)
    y_val_t = torch.tensor(y_val, dtype=torch.long, device=device)

    # ---- baseline ----
    base_logits = wrapped(X_val_t)
    baseline = metric_fn(base_logits, y_val_t)

    block_schema = get_block_schema(schema)

    block_scores = {}
    full_feature_scores = np.zeros(X_val.shape[1], dtype=np.float32)

    for blk in block_schema:
        s, e = blk["start"], blk["end"]
        width = e - s

        idx = torch.randperm(X_val_t.size(0), device=device)

        Xp = X_val_t.clone()
        Xp[:, s:e] = X_val_t[idx, s:e]

        logits_p = wrapped(Xp)
        new_metric = metric_fn(logits_p, y_val_t)

        delta = float(baseline - new_metric)
        block_scores[blk["name"]] = delta
        full_feature_scores[s:e] = delta / max(width, 1)

    return block_scores, full_feature_scores


# ============================================================
# BLOCK IMPORTANCE FROM FEATURE VECTOR
# ============================================================

def block_importance(feature_scores, block_schema):
    out = {}
    for blk in block_schema:
        s, e = blk["start"], blk["end"]
        out[blk["name"]] = float(feature_scores[s:e].sum())
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
    save_dir="importance",
    ordinal=True
):
    os.makedirs(save_dir, exist_ok=True)

    # ---- Permutation importance ----
    blk_scores, feat_scores = permutation_importance_grouped(
        model, X, y, schema, metric_fn, device=device, ordinal=ordinal
    )

    json.dump(blk_scores, open(os.path.join(save_dir, "perm_block.json"), "w"), indent=2)
    np.save(os.path.join(save_dir, "perm_feature.npy"), feat_scores)

    # ---- IG global ----
    mean_ig, var_ig = integrated_gradients_global(
        model, X, ordinal=ordinal, device=device
    )
    np.save(os.path.join(save_dir, "ig_mean.npy"), mean_ig)
    np.save(os.path.join(save_dir, "ig_var.npy"), var_ig)

    # ---- block IG ----
    block_schema = get_block_schema(schema)
    ig_block = block_importance(mean_ig, block_schema)
    json.dump(ig_block, open(os.path.join(save_dir, "ig_block.json"), "w"), indent=2)

    print("[importance] DONE →", save_dir)

    return {
        "perm_block": blk_scores,
        "perm_feature": feat_scores,
        "ig_mean": mean_ig,
        "ig_var": var_ig,
        "ig_block": ig_block
    }


