import os
import json
import argparse
import numpy as np
import pandas as pd
from pathlib import Path
from datetime import datetime

# ------------------------------------------------------------
# Repo root resolution (same trick as train_mlp)
# ------------------------------------------------------------
import sys
ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from dotenv import load_dotenv
load_dotenv(dotenv_path=".env", override=False)

# ------------------------------------------------------------
# Repo-libs
# ------------------------------------------------------------
from libs.models.io_config import (
    load_yaml,
    load_prep_artifacts,
    read_arrow_matrix,
    build_test_features_from_csv
)
from libs.utils.imbalance import (
    class_weights_from_counts,
    choose_imbalance_strategy,
    apply_smote_tomek
)
from libs.utils.reporting import (
    make_run_dir,
    evaluate_and_save,
    save_input_data_information
)

from libs.utils.importance import (
    permutation_importance_grouped,
)

import shap
from sklearn.metrics import f1_score


# =====================================================================
# PART 1 — Load PREP artifacts
# =====================================================================

def load_data(prep_root: str, prep_id: str):
    art = load_prep_artifacts(prep_root, prep_id)

    X_train = read_arrow_matrix(art["X_train"])
    X_val   = read_arrow_matrix(art["X_val"])
    y_train = np.load(art["y_train"])
    y_val   = np.load(art["y_val"])

    return art, X_train, y_train, X_val, y_val


# =====================================================================
# PART 2 — Class/label handling
# =====================================================================

def remap_labels(y_train, y_val):
    classes = sorted(pd.unique(pd.concat([
        pd.Series(y_train), pd.Series(y_val)
    ], ignore_index=True)))

    cls_to_idx = {c: i for i, c in enumerate(classes)}
    ytr = np.array([cls_to_idx[c] for c in y_train], dtype=np.int32)
    yva = np.array([cls_to_idx[c] for c in y_val], dtype=np.int32)

    return classes, cls_to_idx, ytr, yva


# =====================================================================
# PART 3 — XGB Parameter Builder
# =====================================================================

def build_xgb_params(args):
    """
    Mirror configs from .env (same pattern as MLP production).
    """
    use_gpu = args.gpu and str(args.tree_method) == "gpu_hist"

    return dict(
        n_estimators=args.n_estimators,
        max_depth=args.max_depth,
        learning_rate=args.learning_rate,
        subsample=args.subsample,
        colsample_bytree=args.colsample_bytree,
        tree_method="gpu_hist" if use_gpu else args.tree_method,
        grow_policy=args.grow_policy,
        max_bin=args.max_bin,
        n_jobs=args.threads,
        random_state=int(os.getenv("RANDOM_SEED", "42")),
        objective="multi:softprob",
        eval_metric="mlogloss"
    )


# =====================================================================
# PART 4 — SHAP, Permutation & Block Importance
# =====================================================================

def compute_shap_importance(model, X_val, outdir):
    outdir.mkdir(parents=True, exist_ok=True)
    explainer = shap.TreeExplainer(model)
    shap_vals = explainer.shap_values(X_val)

    mean_abs = np.mean([np.abs(c).mean(axis=0) for c in shap_vals], axis=0)
    np.save(outdir / "shap_mean_abs.npy", mean_abs)
    return mean_abs


def compute_permutation_importance(model, X_val, y_val, schema, metric_fn, outdir):
    outdir.mkdir(parents=True, exist_ok=True)
    
    #importances = np.zeros(X_val.shape[1])
    importances = permutation_importance_grouped(model, X_val, y_val, schema, metric_fn, "cpu")

    np.save(outdir / "permutation_importance.npy", importances)
    return importances


def compute_block_importance(perm, schema, outdir):
    outdir.mkdir(parents=True, exist_ok=True)
    blk_importance = {}
    for blk in schema.get("blocks", []):
        s, e = blk["start"], blk["end"]
        blk_importance[blk["name"]] = float(perm[s:e].sum())

    (outdir / "block_importance.json").write_text(
        json.dumps(blk_importance, indent=2), encoding="utf-8"
    )
    return blk_importance


# =====================================================================
# PART 5 — Main Orchestration
# =====================================================================

def main():
    ap = argparse.ArgumentParser()

    ap.add_argument("--features-config", default="configs/features.yaml")
    ap.add_argument("--prep-root", default=os.getenv("PREP_CACHE_ROOT", "artifacts/prep"))
    ap.add_argument("--prep-id", required=True)

    # XGB hyperparameters
    ap.add_argument("--n-estimators", type=int, default=int(os.getenv("XGB_N_EST", "1200")))
    ap.add_argument("--max-depth",    type=int, default=int(os.getenv("XGB_MAX_DEPTH", "8")))
    ap.add_argument("--learning-rate",type=float, default=float(os.getenv("XGB_LR", "0.05")))
    ap.add_argument("--subsample",    type=float, default=float(os.getenv("XGB_SUBSAMPLE", "0.8")))
    ap.add_argument("--colsample-bytree", type=float, default=float(os.getenv("XGB_COLSAMPLE", "0.8")))
    ap.add_argument("--max-bin",      type=int, default=int(os.getenv("XGB_MAX_BIN", "256")))
    ap.add_argument("--threads",      type=int, default=int(os.getenv("XGB_THREADS", "16")))
    ap.add_argument("--tree-method",  default=os.getenv("XGB_TREE_METHOD", "hist"))
    ap.add_argument("--grow-policy",  default=os.getenv("XGB_GROW_POLICY", "lossguide"))
    ap.add_argument("--gpu", action="store_true", default=os.getenv("XGB_GPU", "false") == "true")

    ap.add_argument("--compute-importance", action="store_true")
    ap.add_argument("--test-csv", default=None)

    args = ap.parse_args()

    # Load data
    art, X_train, y_train_raw, X_val, y_val_raw = load_data(args.prep_root, args.prep_id)
    schema = json.load(open(art["schema"])) if art.get("schema") else {}

    # Make run_dir
    run_dir = make_run_dir("XGB", artifacts_root=os.getenv("ARTIFACTS_DIR", "artifacts"))
    save_input_data_information(run_dir, y_train_raw, y_val_raw, args.prep_id)

    # Label remapping
    classes, cls_to_idx, y_train, y_val = remap_labels(y_train_raw, y_val_raw)

    # Imbalance strategy
    strategy, _ = choose_imbalance_strategy(y_train, algo="xgboost")
    sample_weight = None
    if strategy == "class_weight":
        cw = class_weights_from_counts(y_train)
        sample_weight = np.array([cw[c] for c in y_train], dtype=float)
    elif strategy == "smote_tomek":
        X_train, y_train = apply_smote_tomek(X_train, y_train, schema, enabled=True)

    # Train model
    import xgboost as xgb
    params = build_xgb_params(args)
    model = xgb.XGBClassifier(**params)

    model.fit(
        X_train,
        y_train,
        sample_weight=sample_weight,
        eval_set=[(X_val, y_val)],
        verbose=False
    )

    # Evaluate
    y_pred = model.predict(X_val)
    y_prob = model.predict_proba(X_val)

    evaluate_and_save(
        run_dir,
        y_val,
        y_pred,
        labels=np.array([int(v) for k, v in cls_to_idx.items()], dtype=np.int32),
        label_names=np.array([str(k) for k, v in cls_to_idx.items()], dtype=str),
        probs=y_prob,
        model_tag="XGB"
    )

    # Save Booster
    model.save_model(run_dir / "xgb_model.json")

    # Feature Importance
    if args.compute_importance:
        imp_dir = run_dir / "importance"
        imp_dir.mkdir(parents=True, exist_ok=True)

        shap_imp = compute_shap_importance(model, X_val, imp_dir)
        perm_imp = compute_permutation_importance(
            model, X_val, y_val,
            metric_fn=lambda a, b: f1_score(a, b, average="macro"),
            outdir=imp_dir
        )
        #compute_block_importance(perm_imp, schema, imp_dir)

    print(f"[XGB] Completed. Artifacts saved to: {run_dir}")


if __name__ == "__main__":
    main()
