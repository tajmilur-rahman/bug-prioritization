import argparse, os, sys, glob, json, sqlite3, time, hashlib, re
from pathlib import Path
from typing import List, Dict, Any, Optional
import pandas as pd
import numpy as np

# Local embedding
try:
    from sentence_transformers import SentenceTransformer
    import torch
except Exception:
    SentenceTransformer = None
    torch = None

import requests

URL_RE = re.compile(r'https?://\S+')
FENCE_RE = re.compile(r'```')
STACKTRACE_RE = re.compile(r'(at\s+[A-Za-z0-9_.]+\(|^\s*#\d+\s+)', re.MULTILINE)

def sha1_text(s: str) -> str:
    return hashlib.sha1(s.encode('utf-8', errors='ignore')).hexdigest()

def default_text_builder(row: pd.Series, text_cols: List[str]) -> str:
    parts = [str(row[c]) for c in text_cols if c in row and pd.notna(row[c])]
    # Keep it short for Phase 2; truncation in the model tokenizer anyway.
    return " \n".join(parts)

def derive_numerics(summary: str, description: str) -> Dict[str, Any]:
    s = summary or ""
    d = description or ""
    text = f"{s}\n{d}".strip()

    return {
        "summary_len": len(s),
        "desc_len": len(d),
        "url_count": len(URL_RE.findall(text)),
        "code_fence_count": len(FENCE_RE.findall(text)),
        "has_stacktrace": 1 if STACKTRACE_RE.search(d) else 0,
    }

# ------------ Cache layer (SQLite) -------------
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

    def get_many(self, keys: List[str]) -> Dict[str, np.ndarray]:
        if not keys: return {}
        q = "SELECT key, dim, vec FROM cache WHERE key IN (%s)" % (",".join(["?"]*len(keys)))
        rows = self.conn.execute(q, keys).fetchall()
        out = {}
        for k, dim, blob in rows:
            arr = np.frombuffer(blob, dtype=np.float32)
            if arr.size != dim:
                continue
            out[k] = arr
        return out

    def put_many(self, items: Dict[str, np.ndarray]):
        if not items: return
        cur = self.conn.cursor()
        for k, vec in items.items():
            cur.execute("INSERT OR REPLACE INTO cache(key, dim, vec) VALUES (?,?,?)",
                        (k, int(vec.size), vec.astype(np.float32).tobytes()))
        self.conn.commit()

# ------------ Embedding backends -------------
class LocalEmbedder:
    def __init__(self, model_name: str, device: str = "auto", max_seq_len: int = 160):
        if SentenceTransformer is None:
            raise RuntimeError("sentence-transformers not available; install it or use --mode http")
        if device == "auto":
            device = "cuda" if (torch is not None and torch.cuda.is_available()) else "cpu"
        self.model = SentenceTransformer(model_name, device=device)
        self.model.max_seq_length = max_seq_len

    def embed(self, texts: List[str], batch_size: int = 256) -> np.ndarray:
        return np.array(self.model.encode(texts, batch_size=batch_size, normalize_embeddings=True), dtype=np.float32)

class HttpEmbedder:
    def __init__(self, endpoint: str, timeout: float = 60.0):
        self.endpoint = endpoint.rstrip("/")
        self.timeout = timeout

    def embed(self, texts: List[str], batch_size: int = 512) -> np.ndarray:
        # micro-batch to the service
        out = []
        for i in range(0, len(texts), batch_size):
            payload = {"texts": texts[i:i+batch_size]}
            r = requests.post(f"{self.endpoint}/embed", json=payload, timeout=self.timeout)
            r.raise_for_status()
            embs = r.json()["embeddings"]
            out.extend(embs)
        return np.array(out, dtype=np.float32)

# ------------ IO helpers -------------
def read_input(args) -> pd.DataFrame:
    cols = None  # let pandas infer, we’ll select later
    if args.input_parquet_glob:
        files = sorted(glob.glob(args.input_parquet_glob))
        if not files:
            raise SystemExit(f"No parquet matched: {args.input_parquet_glob}")
        parts = [pd.read_parquet(f) for f in files]
        df = pd.concat(parts, ignore_index=True)
    elif args.input_csv:
        df = pd.read_csv(args.input_csv)
    elif args.input_ndjson:
        # one object per line
        df = pd.read_json(args.input_ndjson, lines=True)
    else:
        raise SystemExit("Provide one of --input_parquet_glob | --input_csv | --input_ndjson")

    # Optional filters
    if args.filter:
        # Very simple pandas.query filter (be cautious with quotes)
        df = df.query(args.filter)

    if args.since:
        # Keep rows with creation_time or last_change_time >= since
        for tcol in ["creation_time", "last_change_time"]:
            if tcol in df.columns:
                df[tcol] = pd.to_datetime(df[tcol], errors="coerce", utc=True)
        mask = None
        for tcol in ["creation_time", "last_change_time"]:
            if tcol in df.columns:
                m = df[tcol] >= pd.to_datetime(args.since, utc=True)
                mask = m if mask is None else (mask | m)
        if mask is not None:
            df = df[mask]

    if args.row_offset:
        df = df.iloc[args.row_offset:]
    if args.row_limit:
        df = df.iloc[:args.row_limit]

    return df.reset_index(drop=True)

def choose_text_cols(df: pd.DataFrame, user_cols: Optional[List[str]]) -> List[str]:
    if user_cols: return [c for c in user_cols if c in df.columns]
    # sensible defaults
    candidates = [c for c in ["summary", "description"] if c in df.columns] # optional "whiteboard", "cf_qa_whiteboard"
    if not candidates:
        # fallback: pick two largest text-ish columns
        text_like = [c for c in df.columns if df[c].dtype == object]
        candidates = text_like[:2]
    return candidates

# ------------ Main pipeline -------------
def run(args):
    out_dir = Path(args.out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)
    shard_dir = out_dir / args.shard_name
    shard_dir.mkdir(parents=True, exist_ok=True)

    df = read_input(args)
    text_cols = choose_text_cols(df, args.text_cols)
    id_col = args.id_col if args.id_col in df.columns else None
    if id_col is None:
        # manufacture IDs if missing
        id_col = "_row_id"
        df[id_col] = np.arange(len(df))

    # passthrough categoricals and optional numeric-from cols
    pass_cols = [c for c in (args.pass_cols or []) if c in df.columns]
    numeric_from = [c for c in (args.numeric_from or []) if c in df.columns]

    # build backend
    if args.mode == "local":
        backend = LocalEmbedder(args.model_name, device=args.device, max_seq_len=args.max_seq_len)
        batch_size = args.local_batch
    else:
        backend = HttpEmbedder(args.endpoint, timeout=args.http_timeout)
        batch_size = args.http_batch

    # cache
    cache = EmbCache(Path(args.cache_db))

    # process in shards
    N = len(df)
    shard_size = args.shard_size
    written = 0

    for start in range(0, N, shard_size):
        stop = min(start + shard_size, N)
        part = df.iloc[start:stop].copy()

        # texts
        texts = []
        keys = []
        for _, row in part.iterrows():
            t = default_text_builder(row, text_cols)
            texts.append(t)
            keys.append(sha1_text(t))

        # probe cache
        cached = cache.get_many(keys)
        need_idx = [i for i,k in enumerate(keys) if k not in cached]
        # embed missing
        new_embs = {}
        if need_idx:
            to_embed = [texts[i] for i in need_idx]
            for i0 in range(0, len(to_embed), batch_size):
                chunk = to_embed[i0:i0+batch_size]
                vecs = backend.embed(chunk, batch_size=batch_size)
                for j, v in enumerate(vecs):
                    new_embs[keys[need_idx[i0+j]]] = v
            cache.put_many(new_embs)

        # stitch in order
        embs = []
        for k in keys:
            vec = cached.get(k, new_embs.get(k))
            if vec is None:
                raise RuntimeError("Missing embedding after cache+inference")
            embs.append(vec)
        E = np.vstack(embs).astype(np.float32)

        # derive numerics from text
        num_rows = [derive_numerics(
                        str(part.get("summary", pd.Series([""])).iloc[i]),
                        str(part.get("description", pd.Series([""])).iloc[i])
                    ) for i in range(len(part))]
        num_df = pd.DataFrame(num_rows)

        # relationships counts if present
        for col, out_name in [("duplicates", "dup_count"),
                              ("depends_on", "depends_on_count"),
                              ("blocks", "blocks_count")]:
            if col in part.columns:
                part[out_name] = part[col].apply(lambda x: (len(x) if isinstance(x, (list, tuple)) else 0))

        # assemble output
        out = pd.DataFrame({
            "id": part[id_col].astype(str),
            "summary": (part["summary"] if "summary" in part.columns else ""),
            "description": (part["description"] if "description" in part.columns else ""),
            "product": (part["product"] if "product" in part.columns else None),
            "component": (part["component"] if "component" in part.columns else None),
            "platform": (part["platform"] if "platform" in part.columns else None),
            "op_sys": (part["op_sys"] if "op_sys" in part.columns else None),
        })
        # label passthrough if exists
        if "priority" in part.columns:
            out["priority"] = part["priority"]

        # attach numerics
        out = pd.concat([out.reset_index(drop=True), num_df.reset_index(drop=True)], axis=1)

        # attach optional numeric_from columns (already numeric)
        for col in numeric_from:
            out[col] = part[col]

        # attach pass-through columns
        for col in pass_cols:
            out[col] = part[col]

        # embedding as a list column (Arrow-friendly)
        out["embedding"] = [E[i].tolist() for i in range(E.shape[0])]

        # write shard
        shard_name = f"part-{start:08d}-{stop:08d}.parquet"
        (shard_dir / shard_name).parent.mkdir(parents=True, exist_ok=True)
        out.to_parquet(shard_dir / shard_name, index=False)
        written += len(out)

        print(f"[embed] wrote {shard_dir/shard_name}  rows={len(out)}  cached={len(cached)}  new={len(need_idx)}")

    print(f"[embed] done. total rows written: {written} → {shard_dir}")

def parse_args():
    ap = argparse.ArgumentParser()
    # Inputs
    ap.add_argument("--input_parquet_glob")
    ap.add_argument("--input_csv")
    ap.add_argument("--input_ndjson")
    ap.add_argument("--id_col", default="id")
    ap.add_argument("--text_cols", nargs="*", help="e.g., --text_cols summary description")
    ap.add_argument("--pass_cols", nargs="*", help="columns to pass through as-is")
    ap.add_argument("--numeric_from", nargs="*", help="numeric columns already present to keep (e.g., votes, comment_count)")
    ap.add_argument("--filter", help="pandas.query string, e.g., 'is_open == True and product == \"Firefox\"'")
    ap.add_argument("--since", help="ISO datetime; keep rows with creation_time or last_change_time >= since")
    ap.add_argument("--row_limit", type=int)
    ap.add_argument("--row_offset", type=int, default=0)

    # Backend
    ap.add_argument("--mode", choices=["local", "http"], default="local")
    ap.add_argument("--model_name", default="sentence-transformers/all-MiniLM-L6-v2")
    ap.add_argument("--device", default="auto")  # auto/cpu/cuda
    ap.add_argument("--max_seq_len", type=int, default=160)
    ap.add_argument("--endpoint", default="http://localhost:8001")
    ap.add_argument("--local_batch", type=int, default=256)
    ap.add_argument("--http_batch", type=int, default=512)
    ap.add_argument("--http_timeout", type=float, default=120.0)

    # Caching + output
    ap.add_argument("--cache_db", default="artifacts/emb_cache/embeddings.sqlite")
    ap.add_argument("--out_dir", default="data/processed")
    ap.add_argument("--shard_name", default="train_emb_shards",
                    help="subfolder name under out_dir (e.g., train_emb_shards / val_emb_shards)")
    ap.add_argument("--shard_size", type=int, default=50000)
    return ap.parse_args()

if __name__ == "__main__":
    run(parse_args())
