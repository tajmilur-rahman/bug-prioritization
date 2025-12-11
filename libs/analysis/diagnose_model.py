"""
diagnose_model.py — Integrated Gradients Diagnostics for Severity/Priority Classifier

This script:
    ✓ Loads trained MLP model
    ✓ Loads validation dataset
    ✓ Loads block schema
    ✓ Computes IG for predicted vs true class
    ✓ Produces visualization plots
    ✓ Prints diagnostic insights
"""

import os
from pathlib import Path
import json
import numpy as np
import torch
import pandas as pd
import matplotlib.pyplot as plt

import sys
ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from libs.utils.importance import (
    integrated_gradients_compare,
    get_block_schema
)

from libs.models.io_config import (
    load_yaml,
    load_prep_artifacts,
    read_arrow_matrix,
)

from libs.diagnostics.batch_ig import run_batch_ig
from libs.diagnostics.error_clusters import cluster_misclassifications, summarize_cluster
from libs.diagnostics.ig_html_dashboard import make_ig_bar_chart, make_block_level_chart


# ---------------------------------------------------------
# CONFIG — EDIT THESE PATHS TO MATCH THE RUN
# ---------------------------------------------------------
PREP_ID      = "pca225_tv0.95_sc103261f6_nt95_fmB1"
RUN          = "clf_mlp_B1_20251209_115337"
MODEL_ROOT   = f"artifacts/models/severity/{PREP_ID}/{RUN}"
MODEL_PATH   = f"artifacts/models/severity/{PREP_ID}/{RUN}/best_model.pt"
SCHEMA_PATH  = f"artifacts/prep/{PREP_ID}/schema.json"
VAL_DATA_X   = f"artifacts/prep/{PREP_ID}/X_val.arrow"
VAL_DATA_Y   = f"artifacts/prep/{PREP_ID}/y_val.npy"
DEVICE       = "cuda" if torch.cuda.is_available() else "cpu"


# ---------------------------------------------------------
# PLOT HELPERS
# ---------------------------------------------------------

def plot_compare_ig(feature_names, ig_pred, ig_true, pred_class, true_class):
    width = 0.4
    idx = np.arange(len(feature_names))

    plt.figure(figsize=(16, 6))
    plt.title(f"Integrated Gradients: Predicted vs True Class\nPred={pred_class}, True={true_class}")

    plt.bar(idx - width/2, ig_pred, width, alpha=0.7, label=f"Predicted (class {pred_class})")
    plt.bar(idx + width/2, ig_true, width, alpha=0.7, label=f"True (class {true_class})")

    plt.xticks(idx, feature_names, rotation=90)
    plt.legend()
    plt.tight_layout()
    plt.show()


def plot_block_importance(block_schema, ig_pred, ig_true):
    block_names = [blk["name"] for blk in block_schema]

    block_pred = []
    block_true = []

    for blk in block_schema:
        s, e = blk["start"], blk["end"]
        block_pred.append(np.sum(np.abs(ig_pred[s:e])))
        block_true.append(np.sum(np.abs(ig_true[s:e])))

    x = np.arange(len(block_names))
    width = 0.35

    plt.figure(figsize=(12, 5))
    plt.title("Block-Level IG Contribution (Pred vs True)")
    plt.bar(x - width/2, block_pred, width, label="Predicted Class IG")
    plt.bar(x + width/2, block_true, width, label="True Class IG")
    plt.xticks(x, block_names, rotation=45)
    plt.legend()
    plt.tight_layout()
    plt.show()

# ---------------------------------------------------------
# LOAD DATA & MODEL
# ---------------------------------------------------------

print("[diagnose] Loading model…")
model = torch.load(MODEL_PATH, map_location=DEVICE)
model.eval()
model.to(DEVICE)

print("[diagnose] Loading validation data…")
X_val = read_arrow_matrix(VAL_DATA_X)
yva = np.load(VAL_DATA_Y, allow_pickle=True)

# ---------------------------------------------------------
# Index mapping
# ---------------------------------------------------------
classes = sorted(pd.unique(pd.Series(yva)))
cls_to_idx = {c: i for i, c in enumerate(classes)}
y_val   = torch.tensor([cls_to_idx[c] for c in yva], dtype=torch.long)

# ---------------------------------------------------------
# Convert input matrices to tensors
# ---------------------------------------------------------
X_val = torch.tensor(X_val, dtype=torch.float32)

print("[diagnose] Loading schema…")
schema = json.load(open(SCHEMA_PATH))
block_schema = get_block_schema(schema)


# ---------------------------------------------------------
# Build feature names per block
# ---------------------------------------------------------
feature_names = []
for blk in block_schema:
    width = blk["end"] - blk["start"]
    for j in range(width):
        feature_names.append(f"{blk['name']}_{j}")


# ---------------------------------------------------------
# Pick one or more samples for IG
# ---------------------------------------------------------

print("[diagnose] Selecting sample to analyze…")
sample_idx = np.random.randint(0, len(X_val))
x_sample = X_val[sample_idx]
y_true   = int(y_val[sample_idx])

print(f"[diagnose] Sample index: {sample_idx}, True class = {y_true}")


# ---------------------------------------------------------
# Compute Integrated Gradients
# ---------------------------------------------------------

pred, true, ig_pred, ig_true = integrated_gradients_compare(
    model,
    x_sample,
    true_class=y_true,
    device=DEVICE
)

print(f"[diagnose] Model predicted class = {pred}, True class = {true}")


# ---------------------------------------------------------
# Visualizations
# ---------------------------------------------------------

plot_compare_ig(feature_names, ig_pred, ig_true, pred, true)
plot_block_importance(block_schema, ig_pred, ig_true)


# ---------------------------------------------------------
# DIAGNOSTIC MESSAGES
# ---------------------------------------------------------

print("\n==================== DIAGNOSTIC SUMMARY ====================")

# 1. Check if IG is noisy
if np.mean(np.abs(ig_pred)) < 1e-4:
    print("[!] IG is extremely small ⇒ model underfitting or gradients collapsed.")

# 2. Check if model relies heavily on text PCA only
text_blocks = [blk for blk in block_schema if "text" in blk["name"].lower()]
if text_blocks:
    s, e = text_blocks[0]["start"], text_blocks[0]["end"]
    frac = np.sum(np.abs(ig_pred[s:e])) / np.sum(np.abs(ig_pred))
    if frac > 0.7:
        print("[!] Model depends too much on TEXT block ⇒ metadata/topic features unused.")

# 3. Check metadata block contribution
meta_blocks = [blk for blk in block_schema if "meta" in blk["name"].lower()]
for blk in meta_blocks:
    s, e = blk["start"], blk["end"]
    if np.sum(np.abs(ig_pred[s:e])) < 1e-3:
        print(f"[!] Metadata block '{blk['name']}' has almost 0 IG contribution.")

# 4. Compare predicted vs true importance
diff = np.mean(np.abs(ig_pred - ig_true))
print(f"[diagnose] IG difference between predicted and true class = {diff:.4f}")

if diff < 1e-2:
    print("[!] IG for predicted vs true class is nearly identical ⇒ model is confused between severity classes.")
elif diff > 0.5:
    print("[✓] Model differentiates features for predicted vs true class clearly.")

print("==============================================================\n")


records = run_batch_ig(model, X_val, y_val, limit=500, only_errors=True, device=DEVICE)

clusters = cluster_misclassifications(records)

for key, cluster in clusters.items():
    print(f"\n---- Error cluster {key} (count={len(cluster)}) ----")
    summary = summarize_cluster(cluster, block_schema)
    print(summary)

out_dir = Path(MODEL_ROOT) / "diagnostics" 
out_dir.mkdir(parents=True, exist_ok=True)
make_ig_bar_chart(feature_names, ig_pred, ig_true, pred, true,
                  out_html=out_dir / "ig_sample.html")

make_block_level_chart(block_schema, ig_pred, ig_true,
                       out_html= out_dir / "ig_blocks.html")

