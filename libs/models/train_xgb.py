
import os, json, argparse, numpy as np
import pandas as pd
import sys
from pathlib import Path
ROOT = Path(__file__).resolve().parents[2]  # repo root (adjust if layout changes)
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))
from dotenv import load_dotenv
load_dotenv(dotenv_path=".env", override=False)
from libs.utils.imbalance import class_weights_from_counts, choose_imbalance_strategy, apply_smote_tomek_if_needed
from libs.models.io_config import load_yaml, getenv_bool, load_prep_artifacts, read_arrow_matrix, build_test_features_from_csv
from libs.utils.reporting import make_run_dir, evaluate_and_save

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--features-config", default="configs/features.yaml")
    ap.add_argument("--prep-root", default=os.getenv("PREP_CACHE_ROOT","artifacts/prep"))
    ap.add_argument("--prep-id", required=True)
    ap.add_argument("--fusion", default=os.getenv("FUSION","A1"))
    ap.add_argument("--threads", type=int, default=int(os.getenv("XGB_THREADS","16")))
    ap.add_argument("--oversample", choices=["none","smote_tomek"], default="none")
    ap.add_argument("--test-csv", default=None, help="Optional: test CSV with no topics; will be transformed using prep artifacts.")

    ap.add_argument("--n-estimators", type=int,   default=int(os.getenv("XGB_N_EST","1200")))
    ap.add_argument("--max-depth",    type=int,   default=int(os.getenv("XGB_MAX_DEPTH","8")))
    ap.add_argument("--learning-rate",type=float, default=float(os.getenv("XGB_LR","0.05")))
    ap.add_argument("--subsample",    type=float, default=float(os.getenv("XGB_SUBSAMPLE","0.8")))
    ap.add_argument("--colsample-bytree", type=float, default=float(os.getenv("XGB_COLSAMPLE","0.8")))
    ap.add_argument("--max-bin",      type=int,   default=int(os.getenv("XGB_MAX_BIN","256")))
    ap.add_argument("--tree-method",  default=os.getenv("XGB_TREE_METHOD","hist"))
    ap.add_argument("--grow-policy",  default=os.getenv("XGB_GROW_POLICY","lossguide"))
    ap.add_argument("--gpu",          type=lambda s: str(s).lower() in ("1","true","yes"),
                default=os.getenv("XGB_GPU","false").lower()=="true")

    args = ap.parse_args()

    cfg = load_yaml(args.features_config)
    art = load_prep_artifacts(args.prep_root, args.prep_id)

    # Load matrices
    X_train = read_arrow_matrix(art["X_train"])
    X_val = read_arrow_matrix(art["X_val"])
    ytr = np.load(art["y_train"])
    yva = np.load(art["y_val"])

    # Formalize ytr, yva to y_train, y_val
    classes = sorted(pd.unique(pd.concat([pd.Series(ytr), pd.Series(yva)], ignore_index=True)))
    cls_to_idx = {c:i for i,c in enumerate(classes)}
    y_train = np.array([cls_to_idx[c] for c in ytr], dtype=np.int32)
    y_val = np.array([cls_to_idx[c] for c in yva], dtype=np.int32)

    # Imbalance
    strategy, _ = choose_imbalance_strategy(y_train, algo="xgboost")
    if args.oversample == "smote_tomek":
        strategy = "smote_tomek"

    sample_weight = None
    if strategy == "class_weight":
        cw = class_weights_from_counts(y_train)
        label2w = {lbl: w for lbl, w in cw.items()}
        sample_weight = np.array([label2w[y] for y in y_train], dtype=float)
    elif strategy == "smote_tomek":
        X_train, y_train = apply_smote_tomek_if_needed(X_train, y_train, enabled=True, exclude_cols=None)

    # Train
    import xgboost as xgb
    params = dict(
        n_estimators=args.n_estimators,
        max_depth=args.max_depth,
        learning_rate=args.learning_rate,
        subsample=args.subsample,
        colsample_bytree=args.colsample_bytree,
        tree_method=("gpu_hist" if args.gpu else args.tree_method),
        max_bin=args.max_bin,
        grow_policy=args.grow_policy,
        n_jobs=args.threads,
        random_state=int(os.getenv("RANDOM_SEED","42")),
        eval_metric="mlogloss",
    )
    model = xgb.XGBClassifier(**params)
    model.fit(X_train, y_train, sample_weight=sample_weight, eval_set=[(X_val, y_val)], verbose=False)

    # Save run artifacts under artifacts/clf_XGB_YYYYMMDD_HHMMSS
    run_dir = make_run_dir("XGB", artifacts_root=os.getenv("ARTIFACTS_DIR","artifacts"))
    y_val_pred = model.predict(X_val)
    try:
        y_val_prob = model.predict_proba(X_val)
    except Exception:
        y_val_prob = None

    _ = evaluate_and_save(run_dir, y_val, y_val_pred, 
                          np.array([int(v) for k,v in cls_to_idx.items()], dtype=np.int32), 
                          np.array([str(k) for k,v in cls_to_idx.items()], dtype=str),
                          y_val_prob, model_tag="XGB")
    (run_dir / "xgb.json").write_text(json.dumps(model.get_xgb_params(), indent=2), encoding="utf-8")
    print(f"[XGB] Wrote outputs to {run_dir}")

    # Optional: predict on cached X_test
    if art["X_test"] and art["y_test"]:
        X_test = read_arrow_matrix(art["X_test"]); y_test = np.load(art["y_test"])
        y_pred = model.predict(X_test)
        from sklearn.metrics import f1_score, classification_report
        print("[XGB] Cached Test macro-F1:", f1_score(y_test, y_pred, average="macro"))
        print(classification_report(y_test, y_pred))

    # Optional: predict on external test CSV
    if args.test_csv:
        schema = art["schema"] or {}
        text_cols = cfg["data"]["text_cols"]
        emb_model = cfg["embeddings"]["text"]["model"]
        max_len = cfg["embeddings"]["text"].get("truncate_tokens", 192)
        X_new, ids = build_test_features_from_csv(args.test_csv, schema, art["pca"], art["scaler"], art["topics_centroids"], text_cols, emb_model, max_len)
        y_hat = model.predict(X_new)
        out = Path("artifacts/predictions"); out.mkdir(parents=True, exist_ok=True)
        pd.DataFrame({"id": ids, "pred": y_hat}).to_csv(out / f"xgb_preds_{args.prep_id}.csv", index=False)
        print(f"[XGB] Wrote predictions to {out / f'xgb_preds_{args.prep_id}.csv'}")

if __name__ == "__main__":
    main()
