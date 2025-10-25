
# Build prep matrices from existing parquet splits that already contain embeddings & topic ids.
# Usage:
#   python libs/models/prep_from_parquets.py --features-config configs/features.yaml #       --train_parquet_glob 'data/train_clean/*_clean.parquet' --val_parquet_glob 'ddata/val_clean/*_clean.parquet' #       --topics-clean-dir artifacts/topics_clean
import os, glob, json, argparse, numpy as np, pandas as pd
for v in ["OMP_NUM_THREADS", "MKL_NUM_THREADS", "OPENBLAS_NUM_THREADS",
          "VECLIB_MAXIMUM_THREADS", "NUMEXPR_NUM_THREADS"]:
    os.environ.setdefault(v, "1")

from pathlib import Path

import joblib
from sklearn.decomposition import PCA
from sklearn.feature_extraction import FeatureHasher
from sklearn.preprocessing import StandardScaler, OneHotEncoder
import pyarrow as pa, pyarrow.ipc as ipc

def normalize_df(df):
    n0 = len(df)
    df = df.dropna(subset=['priority'])
    n1 = len(df)
    print(f"Dropped {n0-n1} rows with NA label")
    df.reset_index(drop=True)
    return df

def load_yaml(path):
    import yaml
    with open(path, "r", encoding="utf-8") as f:
        return yaml.safe_load(f)

def read_table(csv_path=None, parquet_glob=None):
    if parquet_glob:
        parts = [pd.read_parquet(p) for p in glob.glob(parquet_glob)]
        if parts:
            return normalize_df(pd.concat(parts, axis=0, ignore_index=True))
    if csv_path:
        return normalize_df(pd.read_csv(csv_path))
    return None

def write_arrow_matrix(X: np.ndarray, out_path: Path):
    out_path = Path(out_path)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    arr = pa.array([list(row) for row in X], type=pa.list_(pa.float32()))
    table = pa.Table.from_arrays([arr], names=["X"])
    # Use OSFile with a *string* path
    abs_path = str(out_path.resolve())
    with pa.OSFile(abs_path, 'wb') as sink:
        with ipc.new_file(sink, table.schema) as writer:
            writer.write_table(table)

def compute_prep_id(pca_dim: int, pca_var: float, scaler_hash: str, n_topics_new: int):
    return f"pca{pca_dim}_v{pca_var:.2f}_sc{scaler_hash[:8]}_tn{n_topics_new}"

def apply_topics_map(series, topics_map):
    return series.astype(int).map(lambda t: topics_map.get(str(int(t)), -1))

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--features-config", default="configs/features.yaml")
    ap.add_argument("--prep-root", default=os.getenv("PREP_CACHE_ROOT","artifacts/prep"))
    ap.add_argument("--prep-id", default="")
    ap.add_argument("--train_parquet_glob", required=True)
    ap.add_argument("--val_parquet_glob", required=True)
    ap.add_argument("--test_parquet_glob")
    ap.add_argument("--topics-clean-dir", default="artifacts/topics_clean")
    args = ap.parse_args()

    cfg = load_yaml(args.features_config)
    label_col = cfg["data"]["label_col"]
    id_col = cfg["data"].get("id_col", "id")
    topics_dim = cfg["features"]["topics"].get("dim", 8)
    target_variance = cfg["prep"]["pca"]["target_variance"]
    max_dim = cfg["prep"]["pca"].get("max_dim", 256)
    numeric_cols = cfg.get("features",{}).get("numeric",{}).get("cols", [])
    cat_cfg = cfg.get("features",{}).get("categorical",{})
    cat_cols = cat_cfg.get("cols", [])
    actors_cfg = cfg.get("features",{}).get("actors",{})
    actors_cols = actors_cfg.get("cols", []) if actors_cfg.get("use", False) else []
    hashing_trick = (cat_cfg.get("hashing_trick", {}) or {})
    ht_enabled = bool(hashing_trick.get("enabled", False))
    ht_buckets = int(hashing_trick.get("buckets", 512))
    ht_topk = int(hashing_trick.get("topk_pass", 100))
    ht_fields = set(hashing_trick.get("fields", []) or cat_cols)
    ohe_small = bool(cat_cfg.get("ohe_small", True))

    # enforcing deterministic column order
    cat_cols = sorted(cat_cols)
    numeric_cols = sorted(numeric_cols)
    actors_cols = sorted(actors_cols)


    ablate = (cfg.get("regularization", {})
              .get("feature_ablation_switches", 
                   { "use_text": True, 
                    "use_topics": True,                    
                    "use_categorical": True, 
                    "use_actors": False,                    
                    "use_numeric": True,                
                    }))
    

    tdir = Path(args.topics_clean_dir)
    topics_map = json.load(open(tdir / "topics_map.json","r",encoding="utf-8"))
    C_clean = np.load(tdir / "cleaned_centroids.npy")
    n_topics_new = C_clean.shape[0]

    tr = read_table(parquet_glob=args.train_parquet_glob)
    va = read_table(parquet_glob=args.val_parquet_glob)
    te = read_table(parquet_glob=args.test_parquet_glob) if args.test_parquet_glob else None

    if tr is None or va is None:
        raise SystemExit("Require train & val parquet globs.")

    # Apply cleaned mapping to topic_id_A/B if present
    def tidy(df):
        if df is None: return None
        if "topic_id_A" in df.columns:
            df["topic_id_A_clean"] = apply_topics_map(df["topic_id_A"], topics_map)
        if "topic_id_B" in df.columns:
            df["topic_id_B_clean"] = apply_topics_map(df["topic_id_B"], topics_map)
        return df

    tr, va, te = tidy(tr), tidy(va), tidy(te)

    # Embeddings (already present)
    def get_emb(df):
        if df is None: return None
        if "embedding" not in df.columns:
            raise SystemExit("Expected 'embedding' column in parquet splits.")
        return np.stack(df["embedding"].to_list()).astype("float32")

    E_tr = get_emb(tr); E_va = get_emb(va); E_te = get_emb(te) if te is not None else None

    # PCA on train
    pca = PCA(n_components=min(max_dim, E_tr.shape[1]), svd_solver="full", random_state=0).fit(E_tr)
    csum = np.cumsum(pca.explained_variance_ratio_)
    k = int(np.searchsorted(csum, target_variance) + 1); k = max(1, min(k, max_dim))
    pca = PCA(n_components=k, svd_solver="full").fit(E_tr)

    TP_tr = pca.transform(E_tr).astype("float32")
    TP_va = pca.transform(E_va).astype("float32")
    TP_te = pca.transform(E_te).astype("float32") if E_te is not None else None

    # Clamp tiny float jitter from SVD/BLAS
    TP_tr = np.round(TP_tr, 6).astype("float32")
    TP_va = np.round(TP_va, 6).astype("float32")
    TP_te = np.round(TP_te, 6).astype("float32") if TP_te is not None else None


    # Build centroids from TRAIN ONLY 
    centroids = {}
    min_count = 3  # avoid ultra-noisy centroids; tune as you like
    #topic_col = "topic_id_A_clean" if "topic_id_A_clean" in tr.columns else ("topic_id_B_clean" if "topic_id_B_clean" in tr.columns else None)
    topic_col = "topic_id_B_clean" if "topic_id_B_clean" in tr.columns else None
    def compute_centroids(min_c):
        t_centroids = {}
        for tid, idx in tr.groupby(topic_col).groups.items():
            idx = np.asarray(list(idx))
            if tid == 122:
                print("Topic 122 size:", idx.size)
            if idx.size >= min_c:
                t_centroids[int(tid)] = E_tr[idx].mean(axis=0)
        return t_centroids
    centroids = compute_centroids(min_count)
    centroid_ids = np.array(sorted(centroids.keys()))
    #C_mat = np.vstack([centroids[t] for t in centroid_ids])        # shape: (T, D)
    # Change to use cleaned centroids from topic cleanup steps instead of compute fron TRAIN
    # since TRAIN is now dropped non labeled docs
    C_mat = C_clean        # shape: (T, D)

    # normalize once for cosine 
    def _norm(a): 
        n = np.linalg.norm(a, axis=1, keepdims=True) + 1e-12
        return a / n

    C_unit = _norm(C_mat)


    # Build feature with TopK similarity values of each document to centroids of topics
    def topk_centroid_sims(E, k=32):
        if E is None: return None
        E_unit = _norm(E)
        sims = E_unit @ C_unit.T                                   # (N, T)
        # take top-K sims (indices only used if you want one-hot-ish; here we just keep values)
        if sims.shape[1] > k:
            # partial topk without sorting entire axis
            idx = np.argpartition(-sims, kth=k-1, axis=1)[:, :k]
            row = np.arange(sims.shape[0])[:, None]
            topk_vals = sims[row, idx]
            # sort K desc per row for determinism
            order = np.argsort(-topk_vals, axis=1)
            topk_vals = topk_vals[row, order]
            return topk_vals.astype("float32")
        return sims.astype("float32")

    S_tr = topk_centroid_sims(E_tr, k=32)
    S_va = topk_centroid_sims(E_va, k=32)
    S_te = topk_centroid_sims(E_te, k=32) if te is not None else None



    # Build feature with topics embedding from cleaned centroids 
    # (Get cleaned centroids from topic cleanup step to include all topics since TRAIN is now dropped none label docs. 
    # To rerun topics job with TRAIN dropped none label docs.)
    new_roots = sorted({v for v in topics_map.values() if v != -1})
    def get_cleaned_centroid(topic_id, default=None):
        mapped = topics_map.get(str(topic_id), -1)
        if mapped == -1:
            # Option 1: return None
            # Option 2 (a consistent shape placeholder):
            if default is not None:
                return default
            else:
                return np.zeros(C_clean.shape[1], dtype=np.float32)
        idx = new_roots.index(mapped)
        return C_clean[idx]
    
    def get_topic_centroid(df):
        if df is None:
            return None
        if "topic_id_B_clean" not in df.columns:
            raise SystemExit("Expected 'topic_id_B_clean' column in parquet splits.")
        # USE A LIST, not a generator, for np.stack
        rows = [get_cleaned_centroid(i) for i in df["topic_id_B_clean"]]
        return np.stack(rows, axis=0).astype("float32")
    
    T_cen_tr = get_topic_centroid(tr)
    T_cen_va = get_topic_centroid(va)
    T_cen_te = get_topic_centroid(te) if te is not None else None
    # PCA for topics embedding on TRAIN
    t_pca = PCA(n_components=min(max_dim, T_cen_tr.shape[1]), svd_solver='full', random_state=0).fit(T_cen_tr)
    t_csum = np.cumsum(t_pca.explained_variance_ratio_)
    t_k = int(np.searchsorted(t_csum, target_variance) + 1); t_k = max(1, min(t_k, max_dim))
    t_pca = PCA(n_components=t_k, svd_solver="full").fit(T_cen_tr)

    T_tr = t_pca.transform(T_cen_tr).astype("float32")
    T_va = t_pca.transform(T_cen_va).astype("float32")
    T_te = t_pca.transform(T_cen_te).astype("float32") if T_cen_te is not None else None

    # Small topics embedding = first 'topics_dim' PCs
    def first_k(A, k):
        if A is None: return None
        if A.shape[1] >= k: return A[:, :k]
        return np.pad(A, ((0,0),(0,k-A.shape[1])))

    # T_emb_tr = first_k(TP_tr, topics_dim)
    # T_emb_va = first_k(TP_va, topics_dim)
    # T_emb_te = first_k(TP_te, topics_dim) if TP_te is not None else None
    # T_emb_tr = first_k(T_tr, topics_dim)
    # T_emb_va = first_k(T_va, topics_dim)
    # T_emb_te = first_k(T_te, topics_dim) if T_te is not None else None
    T_emb_tr = T_tr
    T_emb_va = T_va
    T_emb_te = T_te if T_te is not None else None



    # Numeric block
    def num_block(df):
        if df is None or not numeric_cols: return None
        cols = [c for c in numeric_cols if c in df.columns]
        return df[cols].fillna(0.0).astype("float32").to_numpy() if cols else None

    
    # --- Categorical encoders (topK OHE + FeatureHasher spillover) ---
    def fit_categorical_encoders(df):
        if not cat_cols: 
            return {"ohe": {}, "hash": {}, "meta": {}}
        encs = {"ohe": {}, "hash": {}, "meta": {}}
        for col in cat_cols:
            vals = df[col].fillna("__NA__").astype(str)
            vc = vals.value_counts()
            keep = set(vc.sort_index().head(ht_topk).index) if (ht_enabled and col in ht_fields) else set() # tie-stable
            small = (ohe_small and len(vc) <= max(16, ht_topk))
            if keep or small or cat_cfg.get("encoding", {}).get("strategy") == "onehot":
                ohe = OneHotEncoder(handle_unknown='ignore', sparse_output=False)
                fit_vals = vals if small else vals[vals.isin(keep)]
                if len(fit_vals) > 0:
                    _ = ohe.fit(fit_vals.to_frame())
                    encs["ohe"][col] = ohe
                    encs["meta"][f"{col}_ohe_cats"] = ohe.categories_[0].tolist()
                else:
                    encs["ohe"][col] = None
                    encs["meta"][f"{col}_ohe_cats"] = []
            if ht_enabled and col in ht_fields:
                encs["hash"][col] = {"n_features": ht_buckets}
                encs["meta"][f"{col}_hash_buckets"] = ht_buckets
        return encs

    def transform_categorical(df, encs):
        if not cat_cols: 
            return None
        pieces = []
        for col in cat_cols:
            vals = df[col].fillna("__NA__").astype(str)
            ohe = encs["ohe"].get(col)
            if ohe is not None:
                pieces.append(ohe.transform(vals.to_frame()).astype("float32"))
            if col in encs["hash"]:
                fh = FeatureHasher(n_features=encs["hash"][col]["n_features"], input_type='string')
                H = fh.transform(vals).astype("float32").toarray()
                pieces.append(H)
        return np.concatenate(pieces, axis=1).astype("float32") if pieces else None
    
    Xn_tr = num_block(tr); Xn_va = num_block(va); Xn_te = num_block(te)
    
    # Fit encoders on train & persist
    cat_encs = fit_categorical_encoders(tr) if (tr is not None) else {"ohe": {}, "hash": {}, "meta": {}}
    

    def actors_hash_block(df):
        if df is None or not actors_cols: return None
        fh = FeatureHasher(n_features=int(actors_cfg.get("encoding", {}).get("hash", {}).get("n_features", 512)), input_type='string')
        toks = df[actors_cols].fillna("__NA__").astype(str).agg(" ", axis=1)
        return fh.transform(toks).astype("float32").toarray()
    

    # Assemble features: text_pca + topics_emb + numeric
    def hstack(parts):
        ps = [p for p in parts if p is not None]
        return np.concatenate(ps, axis=1).astype("float32")
    
    Xc_tr = transform_categorical(tr, cat_encs) if ablate.get("use_categorical", True) else None
    Xc_va = transform_categorical(va, cat_encs) if ablate.get("use_categorical", True) else None
    Xc_te = transform_categorical(te, cat_encs) if (TP_te is not None and ablate.get("use_categorical", True)) else None

    Xa_tr = actors_hash_block(tr) if ablate.get("use_actors", False) else None
    Xa_va = actors_hash_block(va) if ablate.get("use_actors", False) else None
    Xa_te = actors_hash_block(te) if (TP_te is not None and ablate.get("use_actors", False)) else None

    def parts_list(tp, temb, xc, xa, xn):
        parts = []
        if ablate.get("use_text", True): parts.append(tp)
        if ablate.get("use_topics", True): parts.append(temb)
        if xc is not None: parts.append(xc)
        if xa is not None: parts.append(xa)
        if ablate.get("use_numeric", True): parts.append(xn)
        return parts

    # Now concatenate all selected features
    X_tr = hstack(parts_list(TP_tr, T_emb_tr, Xc_tr, Xa_tr, Xn_tr) + [S_tr])
    X_va = hstack(parts_list(TP_va, T_emb_va, Xc_va, Xa_va, Xn_va) + [S_va])
    X_te = hstack(parts_list(TP_te, T_emb_te, Xc_te, Xa_te, Xn_te) + [S_te]) if te is not None else None
    # X_tr = hstack(parts_list(TP_tr, None, Xc_tr, Xa_tr, Xn_tr) + [S_tr])
    # X_va = hstack(parts_list(TP_va, None, Xc_va, Xa_va, Xn_va) + [S_va])
    # X_te = hstack(parts_list(TP_te, None, Xc_te, Xa_te, Xn_te) + [S_te]) if te is not None else None
    

    # Print dims of splits before scaling
    def dims(name, *blocks):
        print("Dims of feature blocks in splits TR/VA (topic, embedding, categorical, actors, numeric, topKSimilarity):")
        print(name, [None if b is None else b.shape[1] for b in blocks],
            "→", sum(b.shape[1] for b in blocks if b is not None))
    dims("TR", TP_tr, T_emb_tr, Xc_tr, Xa_tr, Xn_tr, S_tr)
    dims("VA", TP_va, T_emb_va, Xc_va, Xa_va, Xn_va, S_va)
    # dims("TR", TP_tr, None, Xc_tr, Xa_tr, Xn_tr, S_tr)
    # dims("VA", TP_va, None, Xc_va, Xa_va, Xn_va, S_va)


    scaler = StandardScaler(with_mean=True, with_std=True).fit(X_tr)
    X_tr = scaler.transform(X_tr).astype("float32")
    X_va = scaler.transform(X_va).astype("float32")
    if X_te is not None:
        X_te = scaler.transform(X_te).astype("float32")

    y_tr = tr[label_col].to_numpy() if label_col in tr.columns else None
    y_va = va[label_col].to_numpy() if label_col in va.columns else None
    y_te = te[label_col].to_numpy() if (te is not None and label_col in te.columns) else None

    # Schema
    schema = {
        "id_col": id_col,
        "label_col": label_col,
        "blocks": {"numeric": [c for c in (numeric_cols or []) if c in tr.columns]},
        "pca": {"n_components": int(pca.n_components_), "target_variance": float(target_variance)},
        "topics": {"dim": int(topics_dim), "n_new": int(n_topics_new)},
    }

    # Prepare out folder: compute prep_id as folder name
    import hashlib, json as _json
    # schema_sig = {
    # "blocks": ["TP","Temb","Xc","Xa","Xn"],             # whatever you include
    # "dims":   [TP_tr.shape[1], T_emb_tr.shape[1], 
    #             0 if Xc_tr is None else Xc_tr.shape[1],
    #             0 if Xa_tr is None else Xa_tr.shape[1],
    #             0 if Xn_tr is None else Xn_tr.shape[1]],
    # "cat_meta": cat_encs["meta"],                       # OHE cats, hash buckets
    # "actors": actors_cfg.get("encoding", {}),
    # "pca_dim": int(pca.n_components_),
    # "target_var": float(target_variance),
    # }
    # scaler_hash = hashlib.md5(_json.dumps(schema_sig, sort_keys=True).encode()).hexdigest()

    def stable_scaler_hash(scaler):
        arr = {
            "mean": np.round(scaler.mean_, 6).tolist(),
            "scale": np.round(scaler.scale_, 6).tolist(),
        }
        blob = _json.dumps(arr, sort_keys=True).encode("utf-8")
        return hashlib.md5(blob).hexdigest()

    scaler_hash = stable_scaler_hash(scaler)
    prep_id = args.prep_id or compute_prep_id(
        int(pca.n_components_), float(target_variance), scaler_hash, n_topics_new
    )

    out = Path(args.prep_root) / prep_id
    out.mkdir(parents=True, exist_ok=True)


    # Save artifacts
    try:
        joblib.dump(cat_encs, out / "cat_encoders.joblib")
        joblib.dump(pca, out / "pca.joblib")
        joblib.dump(scaler, out / "scaler.joblib")
        # Save cleaned topics references for inference convenience
        np.save(out / "topic_centroids.clean.npy", C_clean)
        json.dump(topics_map, open(out / "topics_map.clean.json","w",encoding="utf-8"), indent=2)
    except Exception:
        pass
    
    
    # Emit schema with block spans (optional consumer by trainers)
    blocks = []
    s = 0
    if ablate.get("use_text", True): blocks.append(("text_pca", TP_tr.shape[1])); s += TP_tr.shape[1]
    if ablate.get("use_topics", True): blocks.append(("topics_emb", T_emb_tr.shape[1])); s += T_emb_tr.shape[1]
    if "Xc_tr" in locals() and Xc_tr is not None: blocks.append(("categorical", Xc_tr.shape[1])); s += Xc_tr.shape[1]
    if "Xa_tr" in locals() and Xa_tr is not None: blocks.append(("actors_hash", Xa_tr.shape[1])); s += Xa_tr.shape[1]
    if ablate.get("use_numeric", True) and Xn_tr is not None: blocks.append(("numeric", Xn_tr.shape[1])); s += Xn_tr.shape[1]
    if S_tr is not None: blocks.append(("top_K_topic_centroid_similarity", S_tr.shape[1])); s += S_tr.shape[1]
    schema = {"id_col": id_col,
        "label_col": label_col,
        "blocks": [{"name": n, "start": int(sum(d for _,d in blocks[:i])), "end": int(sum(d for _,d in blocks[:i+1]))} for i,(n,d) in enumerate(blocks)],
        "pca": {"n_components": int(pca.n_components_), "target_variance": float(target_variance)},
        "topics": {"dim": int(topics_dim), "n_new": int(n_topics_new)},
        "cols": {"categorical": [c for c in (cat_cols or []) if c in tr.columns],
            "numeric": [c for c in (numeric_cols or []) if c in tr.columns],
            "actors": [c for c in (actors_cols or []) if c in tr.columns]},
        }
    try:
        (out / "schema.json").write_text(json.dumps(schema, indent=2), encoding="utf-8")
    except Exception:
        pass

    # Save matrices
    def save_matrix(X, y, name):
        write_arrow_matrix(X, out / f"X_{name}.arrow")
        if y is not None:
            np.save(out / f"y_{name}.npy", y.astype(str))

    save_matrix(X_tr, y_tr, "train")
    save_matrix(X_va, y_va, "val")
    if X_te is not None and y_te is not None:
        save_matrix(X_te, y_te, "test")

    print(f"[prep_from_parquets] wrote artifacts to {out}")

if __name__ == "__main__":
    main()
