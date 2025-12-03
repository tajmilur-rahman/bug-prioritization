"""
embed_docs.py  (New Pipeline)

Responsibilities:
  ✓ Read raw input rows (parquet/csv/ndjson)
  ✓ Build text from summary + description
  ✓ Compute embeddings (local or HTTP service)
  ✓ Cache embeddings (SQLite)
  ✓ Output shards to: data/embeddings/shards/*.parquet
  
Output columns:
   - id
   - summary
   - description
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

# Try local embedding backend
try:
    from sentence_transformers import SentenceTransformer
    import torch
except Exception:
    SentenceTransformer = None
    torch = None


# ───────────────────────────────────────────────────────────────
# Utilities
# ───────────────────────────────────────────────────────────────
def sha1_text(s: str) -> str:
    return hashlib.sha1(s.encode("utf-8", errors="ignore")).hexdigest()

def build_text(row, text_cols):
    parts = []
    for c in text_cols:
        if c in row and pd.notna(row[c]):
            parts.append(str(row[c]))
    return "\n".join(parts).strip()


# ───────────────────────────────────────────────────────────────
# Embedding Cache (SQLite)
# ───────────────────────────────────────────────────────────────
class EmbCache:
    def __init__(self, db_path: Path):
        self.db_path = db_path
        db_path.parent.mkdir(parents=True, exist_ok=True)
        self.conn = sqlite3.connect(str(db_path))
        self.conn.execute("""
            CREATE TABLE IF NOT EXISTS cache(
                key TEXT PRIMARY KEY,
                dim INTEGER NOT NULL,
                vec BLOB NOT NULL
            )
        """)
        self.conn.execute("PRAGMA journal_mode=WAL;")
        self.conn.execute("PRAGMA synchronous=NORMAL;")
        self.conn.commit()

    def get_many(self, keys):
        if not keys:
            return {}
        q = f"SELECT key, dim, vec FROM cache WHERE key IN ({','.join(['?']*len(keys))})"
        rows = self.conn.execute(q, keys).fetchall()
        out = {}
        for k, dim, blob in rows:
            arr = np.frombuffer(blob, dtype=np.float32)
            if arr.size == dim:
                out[k] = arr
        return out

    def put_many(self, items):
        if not items:
            return
        cur = self.conn.cursor()
        for k, vec in items.items():
            cur.execute(
                "INSERT OR REPLACE INTO cache(key, dim, vec) VALUES (?,?,?)",
                (k, int(vec.size), vec.astype(np.float32).tobytes()),
            )
        self.conn.commit()


# ───────────────────────────────────────────────────────────────
# Embedding Backends
# ───────────────────────────────────────────────────────────────
class LocalEmbedder:
    def __init__(self, model_name: str, device="auto", max_len=190):
        if SentenceTransformer is None:
            raise RuntimeError("sentence-transformers not available.")
        if device == "auto":
            device = "cuda" if (torch is not None and torch.cuda.is_available()) else "cpu"
        self.model = SentenceTransformer(model_name, device=device)
        self.model.max_seq_length = max_len

    def embed(self, texts, batch_size=256):
        return np.array(
            self.model.encode(texts, batch_size=batch_size, normalize_embeddings=True),
            dtype=np.float32
        )


class HttpEmbedder:
    def __init__(self, endpoint: str, timeout=90.0):
        self.endpoint = endpoint.rstrip("/")
        self.timeout = timeout

    def embed(self, texts, batch_size=512):
        out = []
        for i in range(0, len(texts), batch_size):
            r = requests.post(
                f"{self.endpoint}/embed",
                json={"texts": texts[i:i+batch_size]},
                timeout=self.timeout
            )
            r.raise_for_status()
            out.extend(r.json()["embeddings"])
        return np.array(out, dtype=np.float32)


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

    text_cols = choose_text_cols(df, args.text_cols)
    if args.id_col not in df.columns:
        df["_row_id"] = np.arange(len(df))
        id_col = "_row_id"
    else:
        id_col = args.id_col

    # Backend selection
    if args.mode == "local":
        backend = LocalEmbedder(args.model_name, device=args.device, max_len=args.max_seq_len)
        batch_size = args.local_batch
    else:
        backend = HttpEmbedder(args.endpoint, timeout=args.http_timeout)
        batch_size = args.http_batch

    cache = EmbCache(Path(args.cache_db))

    out_dir = Path(args.out_dir) / "shards"
    out_dir.mkdir(parents=True, exist_ok=True)

    shard_size = args.shard_size
    N = len(df)
    print(f"[embed] Total input rows: {N}, shard_size={shard_size}")

    for start in range(0, N, shard_size):
        stop = min(start + shard_size, N)
        part = df.iloc[start:stop].copy()

        texts = [build_text(row, text_cols) for _, row in part.iterrows()]
        keys = [sha1_text(t) for t in texts]

        # Cache lookup
        cached = cache.get_many(keys)
        need_idx = [i for i, k in enumerate(keys) if k not in cached]

        # Embed missing
        new_embs = {}
        if need_idx:
            todo = [texts[i] for i in need_idx]
            for i0 in range(0, len(todo), batch_size):
                chunk = todo[i0:i0 + batch_size]
                vecs = backend.embed(chunk, batch_size=batch_size)
                for j, v in enumerate(vecs):
                    new_embs[keys[need_idx[i0+j]]] = v
            cache.put_many(new_embs)

        # Stitch embeddings in correct order
        embs = []
        for k in keys:
            vec = cached.get(k, new_embs.get(k))
            if vec is None:
                raise RuntimeError("Missing embedding unexpectedly.")
            embs.append(vec)
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

    # Backend
    ap.add_argument("--mode", choices=["local", "http"], default="local")
    ap.add_argument("--model_name", default="sentence-transformers/all-MiniLM-L6-v2")
    ap.add_argument("--device", default="auto")
    ap.add_argument("--max_seq_len", type=int, default=190)
    ap.add_argument("--endpoint", default="http://localhost:8001")
    ap.add_argument("--local_batch", type=int, default=256)
    ap.add_argument("--http_batch", type=int, default=512)
    ap.add_argument("--http_timeout", type=float, default=120.0)

    # Cache + Output
    ap.add_argument("--cache_db", default="artifacts/emb_cache/embeddings.sqlite")
    ap.add_argument("--out_dir", default="data/embeddings")
    ap.add_argument("--shard_size", type=int, default=50000)

    return ap.parse_args()


if __name__ == "__main__":
    run_embedding(parse_args())
