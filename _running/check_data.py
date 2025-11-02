import argparse, os, glob, json
import numpy as np, pandas as pd, yaml
import matplotlib.pyplot as plt

import sys
from pathlib import Path
ROOT = Path(__file__).resolve().parents[2]  # repo root (adjust if layout changes)
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

import glob

def check_after_embedding(setpath):
    paths = sorted(glob.glob(f"data/processed/{setpath}/*.parquet"))
    tot = nulls = 0
    for p in paths:
        df = pd.read_parquet(p, columns=["priority"])
        tot += len(df)
        nulls += int(df["priority"].isna().sum())

    print(f"Total rows in {setpath}: {tot:,} | priority nulls: {nulls:,}")
    assert nulls == 0, "Some shards still contain null priority"

def normalize_df(df):
    n0 = len(df)
    df.dropna(subset=['priority'])
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
    ap.add_argument("--out_dir", default="artifacts")
    args = ap.parse_args()
 
    train_df = load_df(args.train_csv, args.train_parquet_glob)
    val_df   = load_df(args.val_csv,   args.val_parquet_glob)
    ytr = train_df['priority']
    yva = val_df['priority']

    classes = sorted(pd.unique(pd.concat([pd.Series(ytr), pd.Series(yva)], ignore_index=True)))
    cls_to_idx = {c:i for i,c in enumerate(classes)}
    print(cls_to_idx)
    
    print(train_df.columns)
    fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(12,6))
    grouped_data = pd.Series(ytr).value_counts()
    ax1.pie(grouped_data.values, labels=grouped_data.index, 
            autopct=lambda p: f'{p:.1f}%\n({(p/100)*sum(grouped_data.values):.0f})')
    ax1.set_title("Train Labels Distribution")
    print(grouped_data)
    grouped_data = pd.Series(yva).value_counts()
    ax2.pie(grouped_data.values, labels=grouped_data.index, 
            autopct=lambda p: f'{p:.1f}%\n\n({(p/100)*sum(grouped_data.values):.0f})')
    ax2.set_title("Evaluation Labels Distribution")
    plt.show()
    print(grouped_data)

    check_after_embedding("train_emb_shards")
    check_after_embedding("val_emb_shards")
    # check_after_embedding("test_emb_shards")
    

if __name__ == "__main__": main()