#!/usr/bin/env python3
import argparse, os, pandas as pd
from pathlib import Path

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--input", default="data/bugs_resolved.csv")
    ap.add_argument("--time_col", default="creation_time")
    ap.add_argument("--val_frac", type=float, default=0.1)
    ap.add_argument("--test_frac", type=float, default=0.1)
    ap.add_argument("--outdir", default="data/processed")
    args = ap.parse_args()

    df = pd.read_csv(args.input)
    if args.time_col not in df.columns:
        raise SystemExit(f"Missing time column: {args.time_col}")
    df["_ts"] = pd.to_datetime(df[args.time_col], errors="coerce", utc=True)
    df = df.sort_values("_ts")
    n = len(df)
    n_test = int(n * args.test_frac)
    n_val = int(n * args.val_frac)
    n_train = n - n_val - n_test

    train = df.iloc[:n_train].copy()
    val   = df.iloc[n_train:n_train+n_val].copy()
    test  = df.iloc[n_train+n_val:].copy()

    outdir = Path(args.outdir); outdir.mkdir(parents=True, exist_ok=True)
    train.to_csv(outdir/"train.csv", index=False)
    val.to_csv(outdir/"val.csv", index=False)
    test.to_csv(outdir/"test.csv", index=False)

    print(f"[splits] train={len(train)} val={len(val)} test={len(test)} -> {outdir}")

if __name__ == "__main__":
    main()
