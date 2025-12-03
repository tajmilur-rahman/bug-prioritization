
"""
merge_enrichment.py 

Merges:
  data/bugs_cleaned.csv
  data/embeddings/shards/*.parquet
  data/topics/topics.parquet

Output:
  data/bugs_enriched.parquet

All merges are ID-exact, no row drift, no duplicate IDs.
"""

import argparse, sys
import json
import glob
import pandas as pd
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from libs.utils.pd_utils import (
    normalize_indices,
    assign_positional,
    safe_merge,
)


# ----------------------------------------------------------------------
# UTILITIES
# ----------------------------------------------------------------------

def load_bugs_cleaned(path: Path) -> pd.DataFrame:
    """Load the cleaned Bugzilla dataset."""
    if not path.exists():
        raise SystemExit(f"Missing bugs_cleaned.csv → {path}")

    df = pd.read_csv(path)
    if "id" not in df.columns:
        raise SystemExit("bugs_cleaned.csv is missing required column: id")

    # Ensure id is string for consistent merges
    df["id"] = df["id"].astype(str)
    return df.reset_index(drop=True)


def load_embeddings_shards(emb_glob: str) -> pd.DataFrame:
    """Load all embedding shards: must contain id + embedding columns."""
    files = sorted(glob.glob(emb_glob))
    if not files:
        raise SystemExit(f"No embedding shards found under: {emb_glob}")

    parts = []
    for fp in files:
        part = pd.read_parquet(fp)
        # required columns
        if "id" not in part.columns:
            raise SystemExit(f"Missing 'id' column in embeddings shard: {fp}")
        if "embedding" not in part.columns:
            raise SystemExit(f"Missing 'embedding' column in embeddings shard: {fp}")

        part["id"] = part["id"].astype(str)
        parts.append(part[["id", "embedding"]])

    df = pd.concat(parts, ignore_index=True)
    df = df.drop_duplicates(subset=["id"], keep="first")
    return df.reset_index(drop=True)


def load_topics(topic_glob: str) -> pd.DataFrame:
    """Load topics assignments."""
    files = sorted(glob.glob(topic_glob))
    if not files:
        raise SystemExit(f"No topics shards found under: {topic_glob}")
    
    parts = []
    for fp in files:
        part = pd.read_parquet(fp)
        
        required = ["id", "topic_A_clean", "topic_B_clean"]
        for c in required:
            if c not in part.columns:
                raise SystemExit(f"Missing {c} column in topics shard: {fp}")

        part["id"] = part["id"].astype(str)
        parts.append(part[["id", "topic_A_clean", "topic_B_clean"]])

    df = pd.concat(parts, ignore_index=True)

    df["id"] = df["id"].astype(str)
    return df.reset_index(drop=True)


# ----------------------------------------------------------------------
# MERGE PIPELINE
# ----------------------------------------------------------------------

def run_merge(args):
    # Paths
    bugs_path = Path(args.bugs_cleaned)
    emb_glob  = args.emb_glob
    topics_glob = args.topics_glob
    out_fp    = Path(args.out_file)

    print("\n[merge] Loading cleaned bugs …")
    bugs = load_bugs_cleaned(bugs_path)

    print("[merge] Loading embeddings shards …")
    emb = load_embeddings_shards(emb_glob)

    print("[merge] Loading topics …")
    topics = load_topics(topics_glob)

    print(f"[merge] bugs rows      = {len(bugs)}")
    print(f"[merge] embeddings rows = {len(emb)}")
    print(f"[merge] topics rows     = {len(topics)}")

    # Normalize indices for safety
    bugs, emb, topics = normalize_indices(bugs, emb, topics)

    # ------------------------------------------------------------------
    # 1. Merge embeddings
    # ------------------------------------------------------------------
    print("[merge] Merging embeddings on id …")
    merged = safe_merge(
        bugs,
        emb,
        on="id",
        how="left",
        check_unique=True,
        allow_extra_rows=False,
    )

    # ------------------------------------------------------------------
    # 2. Merge topics
    # ------------------------------------------------------------------
    print("[merge] Merging topics on id …")
    merged = safe_merge(
        merged,
        topics,
        on="id",
        how="left",
        check_unique=True,
        allow_extra_rows=False,
    )

    # ------------------------------------------------------------------
    # Validation
    # ------------------------------------------------------------------
    if merged["embedding"].isna().sum() > 0:
        missing = int(merged["embedding"].isna().sum())
        raise SystemExit(f"ERROR: {missing} rows missing embeddings after merge.")

    if merged["topic_A_clean"].isna().sum() > 0:
        missing = int(merged["topic_A_clean"].isna().sum())
        print(f"[merge] WARNING: {missing} rows missing topics. These remain NaN.")

    # ------------------------------------------------------------------
    # Save enriched dataset
    # ------------------------------------------------------------------
    out_fp.parent.mkdir(parents=True, exist_ok=True)
    tmp = out_fp.with_suffix(".tmp")

    print(f"[merge] Writing atomic output → {out_fp}")
    merged.to_parquet(tmp, index=False)
    tmp.rename(out_fp)

    # ------------------------------------------------------------------
    # Metadata
    # ------------------------------------------------------------------
    meta = {
        "input_clean_rows": int(len(bugs)),
        "input_embeddings_rows": int(len(emb)),
        "input_topics_rows": int(len(topics)),
        "output_rows": int(len(merged)),
        "embedding_dim": len(merged["embedding"].iloc[0]),
        "missing_topic_A_clean": int(merged["topic_A_clean"].isna().sum()),
        "missing_topic_B_clean": int(merged["topic_B_clean"].isna().sum()),
    }
    (out_fp.parent / "enrichment_meta.json").write_text(
        json.dumps(meta, indent=2),
        encoding="utf-8"
    )

    print(f"[merge] Completed. Enriched dataset stored at:\n  {out_fp}")


# ----------------------------------------------------------------------
# CLI
# ----------------------------------------------------------------------

def main():
    ap = argparse.ArgumentParser()

    ap.add_argument("--bugs-cleaned",
                    default="data/bugs_cleaned.csv")
    ap.add_argument("--emb-glob",
                    default="data/embeddings/shards/*.parquet")
    ap.add_argument("--topics-glob",
                    default="data/topics/shards/*.topics.parquet")
    ap.add_argument("--out-file",
                    default="data/bugs_enriched.parquet")

    args = ap.parse_args()
    run_merge(args)


if __name__ == "__main__":
    main()
