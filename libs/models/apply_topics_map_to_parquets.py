
# Apply cleaned topics mapping to parquet files and write cleaned copies.
# Usage:
#   python models/apply_topics_map_to_parquets.py #       --input_glob 'data/train/*.parquet' #       --topics-clean-dir artifacts/topics_clean #       --out_dir data/train_clean #       --drop-policy keep        # {keep|drop_any_removed|drop_all_removed}
#
# Notes:
# - Reads artifacts/topics_clean/{topics_map.json, cleaned_centroids.npy}.
# - Adds columns: topic_id_A_clean, topic_id_B_clean
# - If --drop-policy != keep, removes rows mapped to -1 (see policy).
# - Writes a summary CSV with counts per clean topic id.
import os, glob, json, argparse
from pathlib import Path
import numpy as np
import pandas as pd

def apply_topics_map(series: pd.Series, topics_map: dict) -> pd.Series:
    # topics_map keys are strings in topics_cleanup output; convert ids to str
    return series.astype(int).map(lambda t: topics_map.get(str(int(t)), -1)).astype(int)

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--input_glob", required=True)
    ap.add_argument("--topics-clean-dir", default="artifacts/topics_clean")
    ap.add_argument("--out_dir", required=True)
    ap.add_argument("--drop-policy", choices=["keep","drop_any_removed","drop_all_removed","drop_b_removed"], default="keep",
                    help="keep: no drop; drop_any_removed: drop rows where A_clean==-1 or B_clean==-1; drop_all_removed: drop only rows with both clean ids==-1")
    args = ap.parse_args()

    tdir = Path(args.topics_clean_dir)
    topics_map = json.load(open(tdir / "topics_map.json","r",encoding="utf-8"))
    _ = np.load(tdir / "cleaned_centroids.npy")  # existence check

    out_dir = Path(args.out_dir); out_dir.mkdir(parents=True, exist_ok=True)

    files = sorted(glob.glob(args.input_glob))
    if not files:
        raise SystemExit(f"No files match: {args.input_glob}")

    summary_rows = []
    for fp in files:
        df = pd.read_parquet(fp)
        if "topic_id_A" in df.columns:
            df["topic_id_A_clean"] = apply_topics_map(df["topic_id_A"], topics_map)
        if "topic_id_B" in df.columns:
            df["topic_id_B_clean"] = apply_topics_map(df["topic_id_B"], topics_map)

        before = len(df)
        if args.drop_policy != "keep":
            a = df["topic_id_A_clean"] if "topic_id_A_clean" in df.columns else None
            b = df["topic_id_B_clean"] if "topic_id_B_clean" in df.columns else None
            if args.drop_policy == "drop_any_removed":
                mask = True
                if a is not None:
                    mask = mask & (a != -1)
                if b is not None:
                    mask = mask & (b != -1)
                df = df[mask]
            elif args.drop_policy == "drop_b_removed":
                mask = True
                if b is not None:
                    mask = mask & (b != -1)
                df = df[mask]
            elif args.drop_policy == "drop_all_removed":
                if a is not None and b is not None:
                    df = df[~((a == -1) & (b == -1))]
                elif a is not None:
                    df = df[a != -1]
                elif b is not None:
                    df = df[b != -1]

        after = len(df)
        # counts per clean topic (A first if present else B)
        if "topic_id_A_clean" in df.columns:
            counts = df["topic_id_A_clean"].value_counts().sort_index()
            for tid, cnt in counts.items():
                summary_rows.append({"file": fp, "topic_col": "A_clean", "topic_id": int(tid), "count": int(cnt)})
        if "topic_id_B_clean" in df.columns:
            counts = df["topic_id_B_clean"].value_counts().sort_index()
            for tid, cnt in counts.items():
                summary_rows.append({"file": fp, "topic_col": "B_clean", "topic_id": int(tid), "count": int(cnt)})

        out_fp = out_dir / (Path(fp).stem + "_clean.parquet")
        df.to_parquet(out_fp, index=False)
        print(f"[apply_topics_map] {fp} -> {out_fp} (rows: {before} -> {after})")

    if summary_rows:
        summary = pd.DataFrame(summary_rows)
        summary.to_csv(out_dir / "clean_topics_distribution.csv", index=False)
        print(f"[apply_topics_map] wrote summary: {out_dir / 'clean_topics_distribution.csv'}")

if __name__ == "__main__":
    main()
