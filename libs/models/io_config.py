# io_config.py
import os, json, yaml, joblib, numpy as np, pandas as pd
from pathlib import Path

def load_yaml(path):
    with open(path, "r", encoding="utf-8") as f:
        return yaml.safe_load(f)

def getenv_bool(name, default=False):
    v = os.getenv(name, str(default)).lower()
    return v in ("1","true","yes","y","on")

def load_prep_artifacts(prep_root: str, prep_id: str):
    root = Path(prep_root) / prep_id
    art = {
        "pca": joblib.load(root / "text_pca.joblib"),
        "topic_pca": joblib.load(root / "topic_pca.joblib"),
        "scaler": joblib.load(root / "scaler.joblib") if (root / "scaler.joblib").exists() else None,
        "schema": json.load(open(root / "schema.json","r",encoding="utf-8")) if (root / "schema.json").exists() else None,
        "categorical_tables": joblib.load(root / "categorical_tables.joblib"),
        "topics_centroids": np.load(root / "topic_centroids.clean.npy") if (root / "topic_centroids.clean.npy").exists() else None,
        "topics_map": json.load(open(root / "topics_map.clean.json","r",encoding="utf-8")) if (root / "topics_map.clean.json").exists() else None,
        "X_train": str(root / "X_train.arrow"),
        "y_train": str(root / "y_train.npy"),
        "X_val": str(root / "X_val.arrow"),
        "y_val": str(root / "y_val.npy"),
        "X_test": str(root / "X_test.arrow") if (root / "X_test.arrow").exists() else None,
        "y_test": str(root / "y_test.npy") if (root / "y_test.npy").exists() else None,
    }
    return art

def read_arrow_matrix(path: str):
    import pyarrow as pa, pyarrow.ipc as ipc
    with pa.memory_map(path, 'r') as source:
        reader = ipc.RecordBatchFileReader(source)
        table = reader.read_all()
    # Expect a single column "X" of list< float >
    arr = np.stack(table.column(0).to_pylist(), axis=0)
    return arr

def build_test_features_from_csv(
    csv_path: str,
    schema: dict,
    text_pca,
    topic_pca,
    scaler,
    C_clean,              # cleaned centroids (topic_centroids.clean.npy)
    cat_tables,           # categorical_tables.joblib
    actors_cfg,           # from features.yaml
    text_cols,            # ['summary','description'] or similar
    emb_model_name: str,
    max_len: int
):
    """
    Full production-grade test-time feature builder.
    Reconstructs EXACT feature blocks used in prep, based on:
        - schema["fusion_mode"]
        - schema["used_blocks"]
        - PCA models
        - C_clean (cleaned centroids)
        - categorical PCA tables
        - actors hashing config
        - numeric columns
        - top-K similarity settings

    Returns:
        X_test (float32 np.ndarray), ids (Series)
    """
    import numpy as np
    import pandas as pd
    from sentence_transformers import SentenceTransformer
    from sklearn.feature_extraction import FeatureHasher

    df = pd.read_csv(csv_path)
    ids = df[schema.get("id_col", "id")] if "id_col" in schema else df.index

    fusion = schema.get("fusion_mode", "B1")
    used_blocks = schema.get("used_blocks", [])      # MUST be present
    block_dims = schema.get("blocks", {})

    numeric_cols = schema.get("numeric_cols", [])
    cat_cols = schema.get("categorical_cols", [])
    actor_cols = schema.get("actors_cols", [])

    # -----------------------
    # 1. TEXT → EMBEDDINGS
    # -----------------------
    model = SentenceTransformer(emb_model_name)
    t0 = df[text_cols[0]].fillna("").astype(str)
    t1 = df[text_cols[1]].fillna("").astype(str)
    texts = (t0 + " " + t1).str.slice(0, max_len * 5)

    text_emb = model.encode(
        texts.tolist(), 
        batch_size=256,
        show_progress_bar=True,
        normalize_embeddings=False
    ).astype("float32")

    # Normalize for similarity
    text_unit = text_emb / (np.linalg.norm(text_emb, axis=1, keepdims=True) + 1e-12)

    parts = []

    # -----------------------
    # BLOCK: TEXT PCA
    # -----------------------
    if "text_pca" in used_blocks:
        TP = text_pca.transform(text_emb).astype("float32")
        parts.append(TP)

    # -----------------------
    # BLOCK: TOPIC CENTROID PCA
    # -----------------------
    if "topic_pca" in used_blocks:

        # topic assignment by cosine
        C_unit = C_clean / (np.linalg.norm(C_clean, axis=1, keepdims=True) + 1e-12)

        dot = np.dot(text_unit, C_unit.T)
        topic_ids = np.argmax(dot, axis=1)

        centroid_vecs = C_clean[topic_ids]
        TPC = topic_pca.transform(centroid_vecs).astype("float32")
        parts.append(TPC)

    # -----------------------
    # BLOCK: TOP-K SIMILARITY
    # -----------------------
    if "topK_sims" in used_blocks:

        K = block_dims.get("topK_sims", 32)

        C_unit = C_clean / (np.linalg.norm(C_clean, axis=1, keepdims=True) + 1e-12)
        sims = np.dot(text_unit, C_unit.T)

        # Select top-K
        if sims.shape[1] <= K:
            S = sims.astype("float32")
        else:
            idx = np.argpartition(-sims, K - 1, axis=1)[:, :K]
            row = np.arange(sims.shape[0])[:, None]
            topk_vals = sims[row, idx]
            order = np.argsort(-topk_vals, axis=1)
            S = topk_vals[row, order].astype("float32")

        parts.append(S)

    # -----------------------
    # BLOCK: CATEGORICAL PCA
    # -----------------------
    if "categorical" in used_blocks:

        Xcat_blocks = []
        for col in cat_cols:

            if col not in df.columns:
                Xcat_blocks.append(np.zeros((len(df), block_dims["categorical"] // len(cat_cols)), dtype="float32"))
                continue

            vals = df[col].fillna("__NA__").astype(str)
            info = cat_tables.get(col, None)

            if info is None or info["pca"] is None:
                # No PCA — fill zeros
                dim = block_dims.get("categorical", 0) // max(1, len(cat_cols))
                Xcat_blocks.append(np.zeros((len(df), dim), dtype="float32"))
                continue

            cats = info["cats"]
            pca = info["pca"]

            # one-hot
            arr_cats = np.array(cats)
            one_hot = (vals[:, None] == arr_cats[None, :]).astype("float32")

            emb = pca.transform(one_hot)
            k = emb.shape[1]

            # pad if needed
            dim = block_dims.get("categorical", 0) // len(cat_cols)
            if k < dim:
                emb = np.pad(emb, ((0, 0), (0, dim - k)))

            Xcat_blocks.append(emb.astype("float32"))

        Xcat = np.concatenate(Xcat_blocks, axis=1)
        parts.append(Xcat)

    # -----------------------
    # BLOCK: ACTORS HASHING
    # -----------------------
    if "actors_hash" in used_blocks:
        if actors_cfg and actor_cols:
            fh = FeatureHasher(
                n_features=block_dims["actors_hash"],
                input_type="string"
            )
            toks = df[actor_cols].fillna("__NA__").astype(str).agg(" ".join, axis=1)
            H = fh.transform(toks).astype("float32").toarray()
        else:
            H = np.zeros((len(df), block_dims.get("actors_hash", 0)), dtype="float32")

        parts.append(H)

    # -----------------------
    # BLOCK: NUMERIC
    # -----------------------
    if "numeric" in used_blocks:
        if numeric_cols:
            Xnum = df[numeric_cols].fillna(0.0).astype("float32").to_numpy()
        else:
            Xnum = np.zeros((len(df), block_dims.get("numeric", 0)), dtype="float32")
        parts.append(Xnum)

    # -----------------------
    # FINAL CONCAT
    # -----------------------
    X = np.concatenate(parts, axis=1).astype("float32")

    # -----------------------
    # SCALER
    # -----------------------
    if scaler is not None:
        X = scaler.transform(X).astype("float32")

    return X, ids

