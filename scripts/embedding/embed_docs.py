"""
embed_docs.py  

Responsibilities:
  ✓ Read raw input rows (parquet/csv/ndjson)
  ✓ Build text from summary + description
  ✓ Compute embeddings 
  ✓ Cache embeddings (SQLite)
  ✓ Output shards to: data/embeddings/shards/*.parquet
  
Output columns:
   - id (bug id)
   - embedding (list of float32)

NO labels, NO engineered features, NO categoricals.
"""

import argparse, os, sys, glob, json, sqlite3, time, hashlib, re
import pandas as pd
import numpy as np
import requests
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from libs.models.severity_scoring.bug_embedding_preprocessing import build_embedding_text
from scripts.embedding.embedding_common import run_embed


BUG_MAX_TOKENS = 190

# =========================
# TEXT UTILITIES
# =========================

def truncate_tokens(text, max_tokens):
    tokens = text.split()
    return " ".join(tokens[:max_tokens])


def build_bug_embedding_text(title, description):
    text = f"""
    Bug title: {title}
    Bug description: {description}
    """

    text_to_embed = build_embedding_text(text)

    return truncate_tokens(text_to_embed, BUG_MAX_TOKENS)

# ───────────────────────────────────────────────────────────────
# Input Reader
# ───────────────────────────────────────────────────────────────
def load_input(args):
    if args.input_parquet_glob:
        files = sorted(glob.glob(args.input_parquet_glob))
        if not files:
            raise SystemExit(f"No parquet matched: {args.input_parquet_glob}")
        return pd.concat([pd.read_parquet(f) for f in files], ignore_index=True)

    if args.input_csv:
        return pd.read_csv(args.input_csv)

    if args.input_ndjson:
        return pd.read_json(args.input_ndjson, lines=True)

    raise SystemExit("Must specify one input source: parquet | csv | ndjson")


def choose_text_cols(df, user_cols):
    if user_cols:
        return [c for c in user_cols if c in df.columns]
    # Most common Bugzilla structure
    cols = [c for c in ["summary", "description"] if c in df.columns]
    print("\nText cols: ", cols)
    if cols:
        return cols
    # fallback heuristic
    objects = [c for c in df.columns if df[c].dtype == object]
    return objects[:2]


# ───────────────────────────────────────────────────────────────
# Main Embedding Pipeline
# ───────────────────────────────────────────────────────────────
def run_embedding(args):
    df = load_input(args)
    df = df.reset_index(drop=True)

    print("\nKeep consistent text columns: [""summary", "description""]")
    if args.id_col not in df.columns:
        df["_row_id"] = np.arange(len(df))
        id_col = "_row_id"
    else:
        id_col = args.id_col


    out_dir = Path(args.out_dir) / "shards"
    out_dir.mkdir(parents=True, exist_ok=True)

    shard_size = args.shard_size
    N = len(df)
    print(f"[embed] Total input rows: {N}, shard_size={shard_size}")

    for start in range(0, N, shard_size):
        stop = min(start + shard_size, N)
        part = df.iloc[start:stop].copy()

        texts = [build_embedding_text(
            build_bug_embedding_text(getattr(row, "summary"), description=getattr(row, "description"))) 
            for row in part.itertuples()]
        embs = run_embed(texts)
        E = np.vstack(embs).astype(np.float32)

        # Build final shard DataFrame
        out = pd.DataFrame({
            "id": part[id_col].astype(str),
            #"summary": part["summary"] if "summary" in part.columns else "",
            #"description": part["description"] if "description" in part.columns else "",
            "embedding": list(map(list, E)),
        })

        shard_path = out_dir / f"emb_{start:08d}_{stop:08d}.parquet"
        out.to_parquet(shard_path, index=False)
        print(f"[embed] wrote shard {shard_path}, rows={len(out)}, new={len(need_idx)}, cached={len(cached)}")

    print(f"[embed] DONE → {out_dir}")


# ───────────────────────────────────────────────────────────────
# CLI
# ───────────────────────────────────────────────────────────────
def parse_args():
    ap = argparse.ArgumentParser()

    # Input
    ap.add_argument("--input_parquet_glob")
    ap.add_argument("--input_csv")
    ap.add_argument("--input_ndjson")
    ap.add_argument("--id_col", default="id")
    ap.add_argument("--text_cols", nargs="*")

    
    # Cache + Output
    ap.add_argument("--cache_db", default="artifacts/emb_cache/embeddings.sqlite")
    ap.add_argument("--out_dir", default="data/embeddings")
    ap.add_argument("--shard_size", type=int, default=50000)

    return ap.parse_args()


if __name__ == "__main__":
    run_embedding(parse_args())
