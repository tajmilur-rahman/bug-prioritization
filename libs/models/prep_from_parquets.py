"""
prep_from_parquets.py  
-------------------------------------------
Reads:
    data/bugs_enriched.parquet

Produces:
    artifacts/prep/<prep_id>/
        X_train.arrow
        X_val.arrow
        X_test.arrow
        y_train.npy
        y_val.npy
        y_test.npy
        text_pca.joblib
        topic_pca.joblib
        scaler.joblib
        categorical_tables.joblib
        topic_centroids.clean.npy
        topics_map.clean.json
        schema.json

Key Features:
- Safe column existence checks
- Strict row filtering:
      requires embedding + topic_B_clean + label
- Time split (train/val/test)
- Text PCA
- Topic centroid PCA
- Dense categorical PCA embeddings
- Numeric block
- Actors hash block (optional)
- Top-K centroid similarity
- Block fusion (A1–A4 / B1–B2)
- Standard scaling
- Analytics export (optional)
"""

import os, json, argparse
import numpy as np
import pandas as pd
from pathlib import Path

from sklearn.decomposition import PCA
from sklearn.preprocessing import StandardScaler
from sklearn.feature_extraction import FeatureHasher
import joblib
import pyarrow as pa
import pyarrow.ipc as ipc


# ======================================================================
# ========== 0. THREAD ENV SAFETY ======================================
# ======================================================================

for v in ["OMP_NUM_THREADS", "MKL_NUM_THREADS",
          "OPENBLAS_NUM_THREADS", "VECLIB_MAXIMUM_THREADS",
          "NUMEXPR_NUM_THREADS"]:
    os.environ.setdefault(v, "1")


# ======================================================================
# ========== 1. SMALL HELPERS ==========================================
# ======================================================================

CATEGORICAL_COLS = [
    "product",
    "component",
    "platform",
    "op_sys",
    "classification",
    "type",
    "version",
]

def load_yaml(path):
    import yaml
    with open(path, "r", encoding="utf-8") as f:
        return yaml.safe_load(f)

def write_arrow_matrix(X: np.ndarray, out_path: Path):
    """Write dense float32 matrix as Arrow list<float32>."""
    out_path.parent.mkdir(parents=True, exist_ok=True)
    arr = pa.array([list(row) for row in X], type=pa.list_(pa.float32()))
    table = pa.Table.from_arrays([arr], names=["X"])

    with pa.OSFile(str(out_path), "wb") as sink:
        with ipc.new_file(sink, table.schema) as writer:
            writer.write_table(table)

def stable_scaler_hash(scaler: StandardScaler) -> str:
    arr = {
        "mean": np.round(scaler.mean_, 6).tolist(),
        "scale": np.round(scaler.scale_, 6).tolist(),
    }
    blob = json.dumps(arr, sort_keys=True).encode("utf-8")
    import hashlib
    return hashlib.md5(blob).hexdigest()[:8]

def compute_prep_id(pca_dim, target_var, scaler_hash, n_topics, fusion_mode):
    return f"pca{pca_dim}_tv{target_var:.2f}_sc{scaler_hash}_nt{n_topics}_fm{fusion_mode}"

def norm_rows(A):
    n = np.linalg.norm(A, axis=1, keepdims=True) + 1e-12
    return A / n

def topk_centroid_sims(E_unit, C_unit, k=32):
    sims = E_unit @ C_unit.T
    if sims.shape[1] <= k:
        return sims.astype("float32")

    idx = np.argpartition(-sims, kth=k - 1, axis=1)[:, :k]
    row = np.arange(len(E_unit))[:, None]
    vals = sims[row, idx]
    order = np.argsort(-vals, axis=1)
    return vals[row, order].astype("float32")

def make_time_split(df, time_col, val_frac, test_frac):
    df = df.sort_values(time_col).reset_index(drop=True)
    n = len(df)
    n_test = int(n * test_frac)
    n_val = int(n * val_frac)

    test_df = df.iloc[-n_test:] if n_test > 0 else df.iloc[:0]
    val_df = df.iloc[-n_test - n_val: -n_test] if n_val > 0 else df.iloc[:0]
    train_df = df.iloc[: -n_test - n_val] if (n_test + n_val) > 0 else df

    return train_df, val_df, test_df


# ======================================================================
# ========== 2. LOAD CLEANED TOPICS ARTIFACTS ==========================
# ======================================================================

def load_topic_artifacts(topics_dir: Path):
    cent = topics_dir / "cleaned_centroids.npy"
    mapp = topics_dir / "topics_map.json"
    if not cent.exists():
        raise FileNotFoundError(f"Missing centroids at {cent}")
    if not mapp.exists():
        raise FileNotFoundError(f"Missing topics_map.json at {mapp}")

    C_clean = np.load(cent)
    topics_map = json.load(open(mapp, "r", encoding="utf-8"))
    return C_clean.astype("float32"), topics_map


# ======================================================================
# ========== 3. CATEGORICAL PCA ENCODING ===============================
# ======================================================================

def build_dense_categorical_embeddings(df, cols, dim=16):
    if not cols:
        return None, None

    enc_tables = {}
    outputs = []

    for col in cols:
        if col not in df.columns:
            enc_tables[col] = {"pca": None, "cats": []}
            outputs.append(np.zeros((len(df), dim), dtype="float32"))
            continue

        vals = df[col].fillna("__NA__").astype(str)
        cats = vals.unique().tolist()
        vals = np.asarray(vals)            
        cats = np.asarray(cats)

        one_hot = (vals[:, None] == cats[None, :]).astype("float32")


        k = min(dim, one_hot.shape[1])
        if k == 0:
            E = np.zeros((len(df), dim), dtype="float32")
            enc_tables[col] = {"pca": None, "cats": cats}
        else:
            pca = PCA(n_components=k, random_state=0).fit(one_hot)
            E = pca.transform(one_hot)
            if k < dim:
                E = np.pad(E, ((0, 0), (0, dim - k)))
            enc_tables[col] = {"pca": pca, "cats": cats}

        outputs.append(E.astype("float32"))

    return np.concatenate(outputs, axis=1), enc_tables

def apply_cat(df, cat_tables, dim=16):
    outs = []
    for col in CATEGORICAL_COLS:
        if col not in df.columns:
            outs.append(np.zeros((len(df), dim), dtype="float32"))
            continue

        vals = df[col].fillna("__NA__").astype(str)
        cats = cat_tables[col]["cats"]
        pca = cat_tables[col]["pca"]

        if len(cats) == 0:
            outs.append(np.zeros((len(df), dim), dtype="float32"))
            continue

        vals = np.asarray(vals)            
        cats = np.asarray(cats)
        oh = (vals[:, None] == cats[None, :]).astype("float32")

        if pca is None:
            E = np.zeros((len(df), dim), dtype="float32")
        else:
            k = pca.n_components_
            E = pca.transform(oh)
            if k < dim:
                E = np.pad(E, ((0, 0), (0, dim - k)))

        outs.append(E.astype("float32"))

    return np.concatenate(outs, axis=1) if outs else np.zeros((len(df), 0))


# ======================================================================
# ========== 4. ACTORS HASHING =========================================
# ======================================================================

def actors_block(df, actors_cfg):
    if not actors_cfg.get("use", False):
        return np.zeros((len(df), 0), dtype="float32")

    cols = actors_cfg.get("cols", [])
    if not cols:
        return np.zeros((len(df), 0), dtype="float32")

    fh = FeatureHasher(
        n_features=actors_cfg.get("encoding", {}).get("hash", {}).get("n_features", 512),
        input_type="string"
    )
    toks = df[cols].fillna("__NA__").astype(str).agg(" ".join, axis=1)
    H = fh.transform(toks).astype("float32").toarray()
    return H.astype("float32")


# ======================================================================
# ========== 5. MAIN PREP LOGIC ========================================
# ======================================================================

def main():
    # ------------------------------------------------------------------
    # Args
    # ------------------------------------------------------------------
    ap = argparse.ArgumentParser()
    ap.add_argument("--input", default="data/bugs_enriched.parquet")
    ap.add_argument("--features-config", default="configs/features.yaml")
    ap.add_argument("--prep-root", default=os.getenv("PREP_CACHE_ROOT", "artifacts/prep"))
    ap.add_argument("--topics-dir", default="artifacts/topics_clean")
    ap.add_argument("--prep-id", default="")
    ap.add_argument("--export-analytics", action="store_true")
    args = ap.parse_args()

    # ------------------------------------------------------------------
    # Load config
    # ------------------------------------------------------------------
    cfg = load_yaml(args.features_config)
    target_variance = cfg["prep"]["pca"]["target_variance"]
    max_dim = cfg["prep"]["pca"]["max_dim"]

    numeric_cols = cfg["features"]["numeric"]["cols"]
    actors_cfg = cfg["features"].get("actors", {})
    use_actors = actors_cfg.get("use", False)

    fusion_mode = os.getenv("FUSION", "B1").upper()
    label_kind = os.getenv("LABEL_KIND", "severity").lower()

    # pick label column safely
    if label_kind.startswith("severity"):
        label_col = "severity_norm"
    else:
        label_col = "priority_norm"

    # ------------------------------------------------------------------
    # Load dataset
    # ------------------------------------------------------------------
    df = pd.read_parquet(args.input)
    print(f"[prep] Loaded {len(df)} rows")

    # ------------------------------------------------------------------
    # Strict filtering: embedding + topic_B_clean + label must exist
    # ------------------------------------------------------------------
    miss_embed = ~df["embedding"].notna() if "embedding" in df.columns else True
    miss_topic = ~df["topic_B_clean"].notna() if "topic_B_clean" in df.columns else True
    miss_label = ~df[label_col].notna() if label_col in df.columns else True

    keep_mask = ~(miss_embed | miss_topic | miss_label)
    dropped = len(df) - keep_mask.sum()
    df = df.loc[keep_mask].reset_index(drop=True)

    print(df[label_col].value_counts(dropna=False))
    print(f"[prep] Dropped {dropped}, remaining {len(df)}")

    # ------------------------------------------------------------------
    # Time-based split
    # ------------------------------------------------------------------
    time_col = os.getenv("TIME_COL", "creation_time")
    if time_col not in df.columns:
        raise SystemExit(f"TIME_COL={time_col} missing from input")

    val_frac = float(os.getenv("VAL_FRAC", "0.10"))
    test_frac = float(os.getenv("TEST_FRAC", "0.10"))

    train_df, val_df, test_df = make_time_split(df, time_col, val_frac, test_frac)
    print(f"[prep] Split → train={len(train_df)}, val={len(val_df)}, test={len(test_df)}")

    # ------------------------------------------------------------------
    # Load cleaned topic artifacts
    # ------------------------------------------------------------------
    topics_dir = Path(args.topics_dir)
    C_clean, topics_map = load_topic_artifacts(topics_dir)
    C_unit = norm_rows(C_clean)

    # topic centroid embedding block
    def topic_centroid_block(dfpart):
        if "topic_B_clean" not in dfpart.columns:
            return np.zeros((len(dfpart), C_clean.shape[1]), dtype="float32")
        out = []
        for tid in dfpart["topic_B_clean"].astype(int).tolist():
            mapped = topics_map.get(str(tid), -1)
            if mapped == -1 or mapped >= len(C_clean):
                out.append(np.zeros(C_clean.shape[1], dtype="float32"))
            else:
                out.append(C_clean[mapped])
        return np.stack(out).astype("float32")

    # ------------------------------------------------------------------
    # Embedding PCA
    # ------------------------------------------------------------------
    def stack_embed(dfpart):
        if "embedding" not in dfpart.columns:
            return np.zeros((len(dfpart), 0), dtype="float32")
        # embedding is list
        return np.stack(dfpart["embedding"].to_list()).astype("float32")

    E_tr = stack_embed(train_df)
    E_va = stack_embed(val_df)
    E_te = stack_embed(test_df)

    base_pca = PCA(n_components=min(max_dim, E_tr.shape[1]),
                   svd_solver="full", random_state=0).fit(E_tr)
    csum = np.cumsum(base_pca.explained_variance_ratio_)
    k = int(np.searchsorted(csum, target_variance) + 1)
    k = max(1, min(k, max_dim))

    p_text = PCA(n_components=k, svd_solver="full", random_state=0).fit(E_tr)
    TP_tr = np.round(p_text.transform(E_tr), 6).astype("float32")
    TP_va = np.round(p_text.transform(E_va), 6).astype("float32")
    TP_te = np.round(p_text.transform(E_te), 6).astype("float32")

    print(f"[prep] Text PCA dims = {k}")

    # ------------------------------------------------------------------
    # Topic centroid PCA
    # ------------------------------------------------------------------
    Tcen_tr = topic_centroid_block(train_df)
    Tcen_va = topic_centroid_block(val_df)
    Tcen_te = topic_centroid_block(test_df)

    base_tp = PCA(n_components=min(max_dim, Tcen_tr.shape[1]),
                  svd_solver="full", random_state=0).fit(Tcen_tr)
    t_csum = np.cumsum(base_tp.explained_variance_ratio_)
    t_k = int(np.searchsorted(t_csum, target_variance) + 1)
    t_k = max(1, min(t_k, max_dim))

    p_topic = PCA(n_components=t_k, svd_solver="full", random_state=0).fit(Tcen_tr)
    T_emb_tr = p_topic.transform(Tcen_tr).astype("float32")
    T_emb_va = p_topic.transform(Tcen_va).astype("float32")
    T_emb_te = p_topic.transform(Tcen_te).astype("float32")

    print(f"[prep] Topic PCA dims = {t_k}")

    # ------------------------------------------------------------------
    # Top-K centroid similarity
    # ------------------------------------------------------------------
    E_unit_tr = norm_rows(E_tr)
    E_unit_va = norm_rows(E_va)
    E_unit_te = norm_rows(E_te)

    S_tr = topk_centroid_sims(E_unit_tr, C_unit, k=32)
    S_va = topk_centroid_sims(E_unit_va, C_unit, k=32)
    S_te = topk_centroid_sims(E_unit_te, C_unit, k=32)

    print(f"[prep] TopK dims = {S_tr.shape[1]}")

    # ------------------------------------------------------------------
    # Numeric block
    # ------------------------------------------------------------------
    def numeric_block(dfpart):
        cols = [c for c in numeric_cols if c in dfpart.columns]
        if not cols:
            return np.zeros((len(dfpart), 0), dtype="float32")
        return dfpart[cols].fillna(0).astype("float32").to_numpy()

    Xn_tr = numeric_block(train_df)
    Xn_va = numeric_block(val_df)
    Xn_te = numeric_block(test_df)

    print(f"[prep] Numeric dims = {Xn_tr.shape[1]}")

    # ------------------------------------------------------------------
    # Categorical PCA embeddings
    # ------------------------------------------------------------------
    Xcat_tr, cat_tables = build_dense_categorical_embeddings(train_df, CATEGORICAL_COLS, dim=16)
    Xcat_va = apply_cat(val_df, cat_tables, dim=16)
    Xcat_te = apply_cat(test_df, cat_tables, dim=16)

    print(f"[prep] Categorical dims = {Xcat_tr.shape[1]}")

    # ------------------------------------------------------------------
    # Actors hashing
    # ------------------------------------------------------------------
    Xa_tr = actors_block(train_df, actors_cfg)
    Xa_va = actors_block(val_df, actors_cfg)
    Xa_te = actors_block(test_df, actors_cfg)

    if use_actors:
        print(f"[prep] Actors dims = {Xa_tr.shape[1]}")
    else:
        print("[prep] Actors disabled")

    # ------------------------------------------------------------------
    # Fusion logic
    # ------------------------------------------------------------------
    def build_parts(tp, tpc, xc, xa, xn, sim):
        if fusion_mode.startswith("A"):
            if fusion_mode == "A1":
                return [tp]
            elif fusion_mode == "A2":
                return [tp, tpc]
            elif fusion_mode == "A3":
                return [tp, tpc, sim]
            elif fusion_mode == "A4":
                return [tp, sim]
            else:
                raise ValueError(f"Unknown fusion: {fusion_mode}")

        if fusion_mode.startswith("B"):
            if fusion_mode == "B1":
                return [tp, tpc, xc, xn, sim]
            elif fusion_mode == "B2":
                return [tp, tpc, xc, xa, xn, sim]
            else:
                raise ValueError(f"Unknown fusion: {fusion_mode}")

        raise ValueError(f"Unknown fusion: {fusion_mode}")

    def hstack(parts):
        return np.concatenate([p for p in parts if p is not None and p.size > 0], axis=1).astype("float32")

    X_tr = hstack(build_parts(TP_tr, T_emb_tr, Xcat_tr, Xa_tr, Xn_tr, S_tr))
    X_va = hstack(build_parts(TP_va, T_emb_va, Xcat_va, Xa_va, Xn_va, S_va))
    X_te = hstack(build_parts(TP_te, T_emb_te, Xcat_te, Xa_te, Xn_te, S_te))

    print(f"[prep] Final dims → train={X_tr.shape}, val={X_va.shape}, test={X_te.shape}")

    # ------------------------------------------------------------------
    # Standard scaling
    # ------------------------------------------------------------------
    scaler = StandardScaler().fit(X_tr)
    X_tr = scaler.transform(X_tr).astype("float32")
    X_va = scaler.transform(X_va).astype("float32")
    X_te = scaler.transform(X_te).astype("float32")

    # labels
    y_tr = train_df[label_col].astype(str).to_numpy()
    y_va = val_df[label_col].astype(str).to_numpy()
    y_te = test_df[label_col].astype(str).to_numpy()

    # ------------------------------------------------------------------
    # prep_id & output folder
    # ------------------------------------------------------------------
    scaler_hash = stable_scaler_hash(scaler)
    prep_id = args.prep_id or compute_prep_id(
        int(p_text.n_components_), target_variance, scaler_hash, C_clean.shape[0], fusion_mode
    )

    out_dir = Path(args.prep_root) / prep_id
    out_dir.mkdir(parents=True, exist_ok=True)
    print(f"[prep] Writing artifacts to: {out_dir}")

    # ------------------------------------------------------------------
    # Save matrices
    # ------------------------------------------------------------------
    def save_matrix(X, y, name):
        write_arrow_matrix(X, out_dir / f"X_{name}.arrow")
        np.save(out_dir / f"y_{name}.npy", y)

    save_matrix(X_tr, y_tr, "train")
    save_matrix(X_va, y_va, "val")
    save_matrix(X_te, y_te, "test")

    # ------------------------------------------------------------------
    # Save PCA, scaler, categorical tables, topic maps
    # ------------------------------------------------------------------
    joblib.dump(p_text, out_dir / "text_pca.joblib")
    joblib.dump(p_topic, out_dir / "topic_pca.joblib")
    joblib.dump(scaler, out_dir / "scaler.joblib")
    joblib.dump(cat_tables, out_dir / "categorical_tables.joblib")

    np.save(out_dir / "topic_centroids.clean.npy", C_clean)
    with open(out_dir / "topics_map.clean.json", "w", encoding="utf-8") as f:
        json.dump(topics_map, f, indent=2)

    # ------------------------------------------------------------------
    # Schema.json
    # ------------------------------------------------------------------
    used_blocks = []

    if fusion_mode == "A1":
        used_blocks = ["text_pca"]
    elif fusion_mode == "A2":
        used_blocks = ["text_pca", "topic_pca"]
    elif fusion_mode == "A3":
        used_blocks = ["text_pca", "topic_pca", "topK_sims"]
    elif fusion_mode == "A4":
        used_blocks = ["text_pca", "topK_sims"]
    elif fusion_mode == "B1":
        used_blocks = ["text_pca", "topic_pca", "categorical", "numeric", "topK_sims"]
    elif fusion_mode == "B2":
        used_blocks = ["text_pca", "topic_pca", "categorical", "actors_hash", "numeric", "topK_sims"]

    schema = {
        "label_col": label_col,
        "fusion_mode": fusion_mode,
        "used_blocks": used_blocks,
        "blocks": {
            "text_pca": TP_tr.shape[1],
            "topic_pca": T_emb_tr.shape[1],
            "categorical": Xcat_tr.shape[1],
            "actors_hash": Xa_tr.shape[1] if use_actors else 0,
            "numeric": Xn_tr.shape[1],
            "topK_sims": S_tr.shape[1],
        },
        "categorical_cols": CATEGORICAL_COLS,
        "numeric_cols": numeric_cols,
        "actors_cols": actors_cfg.get("cols", []) if use_actors else [],
        "topic_count": int(C_clean.shape[0]),
        "text_pca_dim": int(p_text.n_components_),
        "topic_pca_dim": int(p_topic.n_components_),
        "scaler_hash": scaler_hash,
    }
    with open(out_dir / "schema.json", "w", encoding="utf-8") as f:
        json.dump(schema, f, indent=2)

    # ------------------------------------------------------------------
    # Optional analytics export
    # ------------------------------------------------------------------
    if args.export_analytics:
        ana_dir = Path("analytics")
        ana_dir.mkdir(exist_ok=True)
        outp = ana_dir / "full_features.parquet"
        print(f"[prep] Writing analytics → {outp}")

        dfA = df.reset_index(drop=True)
        A_cols = {}

        # include raw columns except raw embedding
        for c in dfA.columns:
            if c != "embedding":
                A_cols[c] = dfA[c]

        # full blocks (unscaled)
        # reconstruct TP_all etc.
        TP_all = np.concatenate([TP_tr, TP_va, TP_te], axis=0)
        T_all = np.concatenate([T_emb_tr, T_emb_va, T_emb_te], axis=0)
        S_all = np.concatenate([S_tr, S_va, S_te], axis=0)
        C_all = np.concatenate([Xcat_tr, Xcat_va, Xcat_te], axis=0)
        N_all = np.concatenate([Xn_tr, Xn_va, Xn_te], axis=0)
        if use_actors:
            A_all = np.concatenate([Xa_tr, Xa_va, Xa_te], axis=0)
        else:
            A_all = None

        # pack
        def pack_block(prefix, arr):
            if arr is None or arr.size == 0:
                return
            for i in range(arr.shape[1]):
                A_cols[f"{prefix}_{i}"] = arr[:, i]

        pack_block("text_pca", TP_all)
        pack_block("topic_pca", T_all)
        pack_block("topK", S_all)
        pack_block("categorical", C_all)
        pack_block("numeric", N_all)
        if use_actors:
            pack_block("actors", A_all)

        pd.DataFrame(A_cols).to_parquet(outp, index=False)
        print("[prep] Analytics export completed")

    print(f"[prep] DONE. Artifacts ready at: {out_dir}")


# ======================================================================
# ========== ENTRY ======================================================
# ======================================================================

if __name__ == "__main__":
    main()
