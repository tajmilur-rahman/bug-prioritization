import argparse, os, glob, json
import numpy as np, pandas as pd, xgboost as xgb, yaml
from sklearn.metrics import classification_report, confusion_matrix
import matplotlib.pyplot as plt

import sys
from pathlib import Path
ROOT = Path(__file__).resolve().parents[2]  # repo root (adjust if layout changes)
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))
from libs.data.build_features_xgb import build_xgb_features

def normalize_df(df):
    n0 = len(df)
    df = df.dropna(subset=['priority'])
    n1 = len(df)
    print(f"Dropped {n0-n1} rows with NA label")
    df.reset_index(drop=True)
    return df

def load_df(csv_path=None, parquet_glob=None, limit=None, cols=None):
    if csv_path:
        df = pd.read_csv(csv_path, usecols=cols) if cols else pd.read_csv(csv_path)
        df = normalize_df(df)
        return df if not limit else df.head(limit)
    if parquet_glob:
        files = sorted(glob.glob(parquet_glob)); parts = []
        for fp in files:
            dfp = pd.read_parquet(fp, columns=cols) if cols else pd.read_parquet(fp)
            parts.append(dfp)
        df = pd.concat(parts, ignore_index=True) if parts else pd.DataFrame()
        df = normalize_df(df)
        return df if not limit else df.head(limit)
    raise SystemExit("Provide either --train_csv or --train_parquet_glob")


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--train_csv"); ap.add_argument("--val_csv")
    ap.add_argument("--train_parquet_glob"); ap.add_argument("--val_parquet_glob")
    ap.add_argument("--features_cfg", default="configs/features.yaml")
    ap.add_argument("--topics_centroids", default="artifacts/topics/topic_centroids.npy")
    ap.add_argument("--topics_threshold", type=float, default=0.35)
    ap.add_argument("--out_dir", default="artifacts")
    ap.add_argument("--model_name", default="clf_XGB")
    ap.add_argument("--limit", type=int)
    ap.add_argument("--class_weight", action="store_true")
    args = ap.parse_args()
    cfg = yaml.safe_load(open(args.features_cfg))
    centroids = None
    if cfg.get("topics", {}).get("use", True) and cfg["topics"].get("source","centroid")!="transform":
        if not Path(args.topics_centroids).exists(): raise SystemExit(f"Centroids not found at {args.topics_centroids}")
        centroids = np.load(args.topics_centroids)
    train_df = load_df(args.train_csv, args.train_parquet_glob, limit=args.limit)
    val_df   = load_df(args.val_csv,   args.val_parquet_glob,   limit=args.limit)
    Xtr, ytr, _ = build_xgb_features(train_df, cfg, centroids=centroids, threshold=args.topics_threshold)
    Xva, yva, _ = build_xgb_features(val_df,   cfg, centroids=centroids, threshold=args.topics_threshold)
    
    classes = sorted(pd.unique(pd.concat([pd.Series(ytr), pd.Series(yva)], ignore_index=True)))
    print(f"Training size: {len(ytr)}")
    print(f"Evaluation size: {len(yva)}")
    print("Training labels distribution:\n", pd.Series(ytr).value_counts())
    print("Evaluation labels distribution:\n", pd.Series(yva).value_counts())
    print(classes)
    
    cls_to_idx = {c:i for i,c in enumerate(classes)}
    ytr_idx = np.array([cls_to_idx[c] for c in ytr], dtype=np.int32)
    yva_idx = np.array([cls_to_idx[c] for c in yva], dtype=np.int32)
    if args.class_weight:
        _, counts = np.unique(ytr_idx, return_counts=True); total = counts.sum()
        weights = total / (len(counts) * counts); class_weights = {i: float(w) for i,w in enumerate(weights)}
    else:
        class_weights = None
    params = dict(objective="multi:softprob", num_class=len(classes), max_depth=8, eta=0.1,
                  subsample=0.9, colsample_bytree=0.9, tree_method="hist", eval_metric="mlogloss")
    dtrain = xgb.DMatrix(Xtr, label=ytr_idx, weight=[class_weights[i] for i in ytr_idx] if class_weights else None)
    dval   = xgb.DMatrix(Xva, label=yva_idx)
    bst = xgb.train(params, dtrain, num_boost_round=500, evals=[(dtrain,"train"),(dval,"val")], early_stopping_rounds=50, verbose_eval=50)
    preds = bst.predict(dval).argmax(axis=1)
    outdir = Path(args.out_dir) / f"{args.model_name}_{pd.Timestamp.now().strftime('%Y%m%d_%H%M%S')}"
    outdir.mkdir(parents=True, exist_ok=True); bst.save_model(str(outdir/"xgb.json"))
    (outdir/"label_map.json").write_text(json.dumps({"classes": [str(c) for c in classes], "cls_to_idx": {str(k):int(v) for k,v in cls_to_idx.items()}}, indent=2))
    rep = classification_report(yva_idx, preds, target_names=[str(c) for c in classes], output_dict=True, zero_division=0)
    (outdir/"classification_report.json").write_text(json.dumps(rep, indent=2))
    from sklearn.metrics import confusion_matrix
    cm = confusion_matrix(yva_idx, preds).tolist()
    (outdir/"confusion_matrix.json").write_text(json.dumps({"labels": [str(c) for c in classes], "matrix": cm}, indent=2))
    print(f"[train_xgb] saved to {outdir}")

if __name__ == "__main__": main()
