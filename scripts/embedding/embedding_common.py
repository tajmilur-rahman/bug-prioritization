"""
embed_docs.py  

Requirements:
  pip install sentence-transformers numpy pandas tqdm

Responsibilities:
  ✓ Compute embeddings 
  ✓ Cache embeddings (SQLite)
  
"""

import argparse, os, sys, glob, json, sqlite3, time, hashlib, re
import pandas as pd
import numpy as np
from tqdm import tqdm
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
    
    def embed_text(self, text):
        return np.array(
            self.model.encode(text, normalize_embeddings=True),
            dtype=np.float32
        )



# ───────────────────────────────────────────────────────────────
# Main Embedding 
# ───────────────────────────────────────────────────────────────
def embed(texts):
    model_name = "sentence-transformers/all-MiniLM-L6-v2"
    backend = LocalEmbedder(model_name)
    cache_path = "artifacts/emb_cache/embeddings.sqlite"

    cache = EmbCache(Path(cache_path))

    keys = [sha1_text(t) for t in texts]

    # Cache lookup
    cached = cache.get_many(keys)
    need_idx = [i for i, k in enumerate(keys) if k not in cached]

    # Embed missing
    new_embs = {}
    if need_idx:
        todo = [texts[i] for i in need_idx]
        vecs = []

        for text in tqdm(todo):
            vecs.append(backend.embed_text(text))

        for j, v in enumerate(vecs):
            new_embs[keys[need_idx[j]]] = v

        cache.put_many(new_embs)

    # Stitch embeddings in correct order
    embs = []
    for k in keys:
        vec = cached.get(k, new_embs.get(k))
        if vec is None:
            raise RuntimeError("Missing embedding unexpectedly.")
        embs.append(vec)
    #E = np.vstack(embs).astype(np.float32)

    
    print(f"[embed] rows={len(embs)}, new={len(need_idx)}, cached={len(cached)}")

    return embs

def run_embed(texts, batch_size=256):
    embs = []

    for i0 in range(0, len(texts), batch_size):
        chunk = texts[i0:i0 + batch_size]
        vecs = embed(chunk)
        embs.extend(vecs)
            
    #E = np.vstack(embs).astype(np.float32)

    return embs


