"""
full_diagnostics.py — One-stop diagnostics engine for Hybrid MLP

Produces a complete diagnostics report including:
    ✓ Integrated Gradients (global, variance, per-class)
    ✓ Block-level IG
    ✓ Permutation importance (block + feature)
    ✓ Topic purity + entropy + MI
    ✓ Topic-level IG influence
    ✓ Misclassification clusters
    ✓ Visualizations & HTML report
"""

import os
import json
from pathlib import Path
import numpy as np
import pandas as pd
import torch
import sys
ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

# -------------------------------
# Utilities from your repository
# -------------------------------
from libs.utils.importance import (
    ordinal_logits_to_probs,
    integrated_gradients_compare,
    integrated_gradients_global,
    per_class_global_ig,
    permutation_importance_grouped,
    block_importance,
    get_block_schema,
)
from libs.models.io_config import read_arrow_matrix

from libs.diagnostics.topic_purity import (
    compute_topic_label_distribution,
    topic_purity,
    topic_entropy,
    topic_kl_uniform,
    global_mutual_information,
    compute_topic_level_ig,
)

from libs.diagnostics.batch_ig import run_batch_ig

from libs.diagnostics.error_clusters import (
    cluster_misclassifications,
    summarize_cluster,
)

from libs.diagnostics.topic_plots import (
    plot_topic_purity,
    plot_topic_entropy,
    plot_topic_confusion_heatmap,
)

from libs.diagnostics.ig_html_dashboard import (
    make_ig_bar_chart,
    make_block_level_chart
)

from libs.diagnostics.feature_semantics import (
    resolve_index,
    build_feature_semantics,
)
from libs.diagnostics.plotly_dashboards import (
    plotly_feature_importance,
    plotly_block_importance,
    plotly_topic_purity
)

from sklearn.metrics import f1_score
import matplotlib.pyplot as plt

ordinal = True
# ================================================================
# MAIN ENTRY
# ================================================================
def run_full_diagnostics(model, prep_id, run_id, device="cpu", limit_ig=500, ordinal=False):
    """
    Runs the FULL diagnostics suite and saves everything into:

        artifacts/models/<label>/<prep_id>/<run_id>/diagnostics/

    Args:
        model      — loaded PyTorch model
        prep_id    — e.g., pca225_tv0.95_sc103261f6_nt95_fmB1
        run_id     — e.g., clf_mlp_B1_20251209_115337
        device     — "cuda" or "cpu"
        limit_ig   — number of samples for batch IG
    """

    # ------------------------------------------------------------
    # 0. Paths and loading data
    # ------------------------------------------------------------
    root = Path(f"artifacts/models/severity/{prep_id}/{run_id}")
    diag_dir = root / "diagnostics"
    diag_dir.mkdir(parents=True, exist_ok=True)

    schema_path = f"artifacts/prep/{prep_id}/schema.json"
    X_path = f"artifacts/prep/{prep_id}/X_val.arrow"
    y_path = f"artifacts/prep/{prep_id}/y_val.npy"
    id_path = f"artifacts/prep/{prep_id}/val_ids.npy"

    schema = json.load(open(schema_path))
    block_schema = get_block_schema(schema)

    X_val = read_arrow_matrix(X_path).astype("float32")
    y_arr = np.load(y_path, allow_pickle=True)
    ids = np.load(id_path, allow_pickle=True)

    # numeric labels
    classes = sorted(pd.unique(pd.Series(y_arr)))
    cls_to_idx = {c:i for i,c in enumerate(classes)}
    y_val = np.array([cls_to_idx[c] for c in y_arr])

    # enrich topic ids
    enriched = pd.read_parquet("data/bugs_enriched.parquet")
    merged = pd.DataFrame({"id": ids})
    merged = merged.merge(enriched[["id", "topic_B_clean"]], on="id", how="left")
    topic_ids = merged["topic_B_clean"].astype(int).tolist()

    model = model.to(device)
    model.eval()


    # ============================================================
    # 1. PERMUTATION IMPORTANCE
    # ============================================================
    print("[diagnostics] Computing permutation importance...")
    blk_perm, feat_perm = permutation_importance_grouped(
        model,
        X_val,
        y_val,
        schema=schema,
        metric_fn=lambda probs, y: f1_score(
            y.cpu().numpy(), probs.cpu().numpy().argmax(1),
            average="macro"
        ),
        device=device,
        ordinal=ordinal,
    )

    json.dump(blk_perm, open(diag_dir/"perm_block.json", "w"), indent=2)
    np.save(diag_dir/"perm_feature.npy", feat_perm)


    # ============================================================
    # 2. GLOBAL IG (mean + variance)
    # ============================================================
    print("[diagnostics] Computing global IG mean + variance...")

    ig_mean, ig_var = integrated_gradients_global(
        model, X_val, n_samples=min(limit_ig, len(X_val)),
        ordinal=ordinal, device=device
    )

    np.save(diag_dir/"ig_mean.npy", ig_mean)
    np.save(diag_dir/"ig_var.npy", ig_var)

    # resolve feature ig feature index to readable names
    topk = np.argsort(-ig_mean)[:20]
    resolved = [resolve_index(int(i), schema) for i in topk]

    json.dump(resolved, open(diag_dir/"ig_top20_semantic.json", "w"), indent=2)

    # ---------------------------------------------------------
    # IG over dataset
    # ---------------------------------------------------------
    records = run_batch_ig(
        model, X_val, y_val,
        limit=500,
        only_errors=False,
        device=device
    )

    # Block IG aggregated from mean IG
    ig_block = block_importance(ig_mean, block_schema)
    json.dump(ig_block, open(diag_dir/"ig_block.json", "w"), indent=2)


    # ============================================================
    # 3. PER-CLASS IG
    # ============================================================
    print("[diagnostics] Per-class IG...")
    class_ig = per_class_global_ig(
        model, X_val, y_val, classes=list(range(len(classes))),
        ordinal=ordinal, device=device
    )
    json.dump(class_ig, open(diag_dir/"ig_per_class.json","w"), indent=2)

    # ============================================================
    # 4. TOPIC PURITY + ENTROPY + MI
    # ============================================================
    print("[diagnostics] Topic purity metrics...")
    dist = compute_topic_label_distribution(topic_ids, y_val.tolist())
    purity = topic_purity(dist)
    entropy = topic_entropy(dist)
    kl = topic_kl_uniform(dist)
    mi = global_mutual_information(topic_ids, y_val.tolist())

    json.dump({
        "purity": purity,
        "entropy": entropy,
        "kl": kl,
        "mutual_information": mi,
    }, open(diag_dir/"topic_stats.json","w"), indent=2)


    # Topic IG influence
    t_block = [(b["start"], b["end"]) for b in block_schema if "topic" in b["name"].lower()]
    print("t_block: ", t_block)
    topic_ig = None
    if t_block:
        topic_ig = compute_topic_level_ig(records, t_block[0])
        np.save(diag_dir/"topic_ig.npy", topic_ig)


    # ============================================================
    # 5. MISCLASSIFICATION CLUSTERS
    # ============================================================
    print("[diagnostics] Clustering misclassifications...")
    # with torch.no_grad():
    #     logits, _ = model(torch.tensor(X_val).to(device))
    #     probs = ordinal_logits_to_probs(logits) if ordinal else torch.softmax(logits, dim=1)
    #     preds = probs.cpu().numpy().argmax(axis=1)

    errors = cluster_misclassifications(records=records)
    
    cluster_summaries = {
        k: summarize_cluster(v, block_schema)
        for k,v in errors.items()
    }
    json.dump(cluster_summaries, open(diag_dir/"error_clusters.json","w"), indent=2)


    # ============================================================
    # 6. VISUALIZATIONS
    # ============================================================
    print("[diagnostics] Plotting figures...")

    # # IG ranking
    # make_ig_bar_chart(ig_mean, diag_dir/"ig_ranked.png")

    # # Block IG
    # make_block_level_chart(ig_block, diag_dir/"ig_blocks.png")

    # Topic purity
    # plot_topic_purity(dist, save_path=diag_dir/"topic_purity.png")

    # # Topic entropy
    # plot_topic_entropy(dist, save_path=diag_dir/"topic_entropy.png")

    # # Topic confusion heatmap
    # plot_topic_confusion_heatmap(dist, save_path=diag_dir/"topic_confusion.png")


    # Interactive plotly dashboards
    feature_names = build_feature_semantics(schema)

    plotly_feature_importance(
        ig_mean,
        feature_names,
        diag_dir/"ig_feature_importance.html"
    )

    plotly_block_importance(
        ig_block,
        diag_dir/"ig_block_importance.html"
    )

    plotly_topic_purity(
        purity,
        diag_dir/"topic_purity.html"
    )


    # ============================================================
    # 7. HTML SUMMARY
    # ============================================================
    html = f"""
    <html>
    <head>
        <title>Diagnostics Report – {run_id}</title>
        <style>body {{ font-family: Arial; margin: 24px; }}</style>
    </head>
    <body>
        <h1>Diagnostics Report</h1>
        <h3>Prep ID: {prep_id}<br>Run ID: {run_id}</h3>

        <h2>Permutation Importance</h2>
        <img src="ig_blocks.png" width="600">

        <h2>Global IG Rankings</h2>
        <img src="ig_ranked.png" width="600">

        <h2>Topic Diagnostics</h2>
        <p>Mutual Information: {mi:.4f}</p>
        <img src="topic_purity.png" width="450">
        <img src="topic_entropy.png" width="450">
        <img src="topic_confusion.png" width="600">

        <h2>Error Clusters</h2>
        <pre>{json.dumps(cluster_summaries, indent=2)}</pre>

        <h2>Block IG</h2>
        <pre>{json.dumps(ig_block, indent=2)}</pre>

        <h2>Per-Class IG</h2>
        <pre>{json.dumps(class_ig, indent=2)}</pre>

    </body>
    </html>
    """
    (diag_dir/"diagnostic_report.html").write_text(html, encoding="utf-8")

    print(f"\n[✓] Diagnostics completed → {diag_dir}")
    return diag_dir


