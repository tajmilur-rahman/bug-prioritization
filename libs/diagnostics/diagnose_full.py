"""
Full Diagnostics Pipeline:
    ✓ Integrated Gradients (per-sample)
    ✓ Block-level attribution
    ✓ Topic purity analysis
    ✓ Topic IG influence
    ✓ Error clusters
    ✓ Global IG statistics
    ✓ HTML dashboard export
"""

import json
import numpy as np
import torch
from pathlib import Path
import pandas as pd
import sys
ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from libs.utils.importance import integrated_gradients_compare, get_block_schema
from libs.diagnostics.batch_ig import run_batch_ig
from libs.diagnostics.block_summary import blockwise_ig, compare_blocks
from libs.diagnostics.error_clusters import cluster_misclassifications, summarize_cluster
from libs.diagnostics.global_analysis import (
    global_feature_ig, global_block_ig, per_class_ig
)
from libs.diagnostics.topic_purity import (
    compute_topic_label_distribution,
    topic_purity,
    topic_entropy,
    topic_kl_uniform,
    global_mutual_information,
    compute_topic_level_ig
)
from libs.diagnostics.topic_plots import (
    plot_topic_purity,
    plot_topic_entropy,
    plot_topic_confusion_heatmap
)
from libs.diagnostics.ig_html_dashboard import (
    make_ig_bar_chart,
    make_block_level_chart
)
from libs.diagnostics.report import write_diagnostic_report

from libs.models.io_config import read_arrow_matrix

from libs.utils.pd_utils import (
    normalize_indices,
    assign_positional,
    safe_merge,
)


# ---------------------------------------------------------
# CONFIG
# ---------------------------------------------------------
PREP_ID = "pca225_tv0.95_sc103261f6_nt95_fmB1"
RUN = "clf_mlp_B1_20251209_115337"
MODEL_ROOT = f"artifacts/models/severity/{PREP_ID}/{RUN}"

MODEL_PATH = f"artifacts/models/severity/{PREP_ID}/{RUN}/best_model.pt"
X_PATH     = f"artifacts/prep/{PREP_ID}/X_val.arrow"
Y_PATH     = f"artifacts/prep/{PREP_ID}/y_val.npy"
ID_PATH    = f"artifacts/prep/{PREP_ID}/val_ids.npy"
SCHEMA_PATH= f"artifacts/prep/{PREP_ID}/schema.json"

DEVICE = "cuda" if torch.cuda.is_available() else "cpu"

# ---------------------------------------------------------
# LOAD
# ---------------------------------------------------------
model = torch.load(MODEL_PATH, map_location=DEVICE).to(DEVICE)
model.eval()

X_val = torch.tensor(read_arrow_matrix(X_PATH), dtype=torch.float32)
y_arr = np.load(Y_PATH, allow_pickle=True)

classes = sorted(pd.unique(pd.Series(y_arr)))
cls_to_idx = {c:i for i,c in enumerate(classes)}
y_val = torch.tensor([cls_to_idx[c] for c in y_arr])

schema = json.load(open(SCHEMA_PATH))
block_schema = get_block_schema(schema)

# Load topic ids of X_val - join with bugs_enriched.parquet on bug id
enriched_bugs = pd.read_parquet("data/bugs_enriched.parquet")
val_ids = pd.DataFrame(np.load(ID_PATH, allow_pickle=True), columns=["id"])
topic_ids = safe_merge(val_ids, enriched_bugs, on="id", how="left")["topic_B_clean"]


# ---------------------------------------------------------
# IG over dataset
# ---------------------------------------------------------
records = run_batch_ig(
    model, X_val, y_val,
    limit=500,
    only_errors=False,
    device=DEVICE
)

# ---------------------------------------------------------
# TOPIC PURITY
# ---------------------------------------------------------
dist = compute_topic_label_distribution(topic_ids, y_val.tolist())
purity = topic_purity(dist)
ent = topic_entropy(dist)
kl = topic_kl_uniform(dist)
mi = global_mutual_information(topic_ids, y_val.tolist())


# ---------------------------------------------------------
# TOPIC IG INFLUENCE
# ---------------------------------------------------------
# find topic block span
topic_block = [(blk["start"], blk["end"]) for blk in block_schema if "topic" in blk["name"].lower()]
if topic_block:
    topic_span = topic_block[0]
    topic_ig_vec = compute_topic_level_ig(records, topic_span)
else:
    topic_ig_vec = None


# ---------------------------------------------------------
# ERROR CLUSTERS
# ---------------------------------------------------------
clusters = cluster_misclassifications(records)
cluster_summaries = {
    key: summarize_cluster(cl, block_schema)
    for key, cl in clusters.items()
}


# ---------------------------------------------------------
# GLOBAL IG stats
# ---------------------------------------------------------
global_feat = global_feature_ig(records)
global_blk  = global_block_ig(records, block_schema)
per_class  = per_class_ig(records, block_schema)


# ---------------------------------------------------------
# REPORT
# ---------------------------------------------------------
report = {
    "topic_mutual_information": mi,
    "topic_purity": purity,
    "topic_entropy": ent,
    "topic_kl_uniform": kl,
    "block_global_ig": global_blk,
    "cluster_summaries": cluster_summaries,
    "global_feature_ig_top10": np.argsort(-global_feat)[:10].tolist()
}

out_dir = Path(MODEL_ROOT) / "diagnostics"
out_dir.mkdir(parents=True, exist_ok=True)

write_diagnostic_report(
    out_dir / "diagnostic_report.html",
    pred_true_stats={"num_samples": len(records)},
    block_summary=global_blk,
    clusters=cluster_summaries,
    global_stats=report
)

print("[✓] Diagnostic Report Written.")
