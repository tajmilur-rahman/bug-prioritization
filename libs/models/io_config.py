
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
        "pca": joblib.load(root / "pca.joblib"),
        "scaler": joblib.load(root / "scaler.joblib") if (root / "scaler.joblib").exists() else None,
        "schema": json.load(open(root / "schema.json","r",encoding="utf-8")) if (root / "schema.json").exists() else None,
        "topics_centroids": np.load(root / "topic_centroids.npy") if (root / "topic_centroids.npy").exists() else None,
        "topics_map": json.load(open(root / "topics_map.json","r",encoding="utf-8")) if (root / "topics_map.json").exists() else None,
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

def build_test_features_from_csv(csv_path: str, schema: dict, pca, scaler, topics_centroids, text_cols, emb_model_name: str, max_len: int):
    """Minimal test-time builder that uses saved schema + PCA + scaler + topics centroids.
    It embeds text locally using sentence-transformers if available.
    Returns: X_test (np.ndarray), ids (pd.Series)
    """
    import numpy as np, pandas as pd
    from sentence_transformers import SentenceTransformer

    df = pd.read_csv(csv_path)
    ids = df[schema.get("id_col","id")] if "id_col" in schema else df.index

    # 1) Text -> embeddings
    model = SentenceTransformer(emb_model_name)
    texts = (df[text_cols[0]].fillna("") + " " + df[text_cols[1]].fillna("")).str.slice(0, max_len*5)  # crude trunc
    text_emb = model.encode(texts.tolist(), batch_size=256, show_progress_bar=True)

    # 2) Topic centroid assignment (cosine)
    def assign_topic_centroid(vec, C):
        v = vec / (np.linalg.norm(vec) + 1e-9)
        Cn = C / (np.linalg.norm(C, axis=1, keepdims=True) + 1e-9)
        sim = Cn @ v
        idx = int(np.argmax(sim))
        return idx
    topics_idx = np.array([assign_topic_centroid(v, topics_centroids) if topics_centroids is not None else -1 for v in text_emb], dtype=int)
    # One simple 8-d topic embedding by taking the centroid vector projected to first 8 PCs of PCA components if available
    # (fallback: zero vector when topics missing)
    if topics_centroids is not None:
        # project text to PCA
        text_pca = pca.transform(text_emb)
        # for a stable 8-d representation, just take first 8 PCA comps as "topics_emb" surrogate
        topics_emb = text_pca[:, :8]
    else:
        topics_emb = np.zeros((len(df), 8), dtype=float)

    # 3) PCA on text embeddings
    text_pca = pca.transform(text_emb)

    # 4) Numeric features (from schema if present)
    X_parts = [text_pca]
    if schema and "blocks" in schema and "numeric" in schema["blocks"]:
        num_cols = schema["blocks"]["numeric"]
        if all(c in df.columns for c in num_cols):
            X_num = df[num_cols].fillna(0.0).astype(float).to_numpy()
            X_parts.append(X_num)

    # 5) Topics embedding if preset expects it
    X_parts.append(topics_emb)

    X = np.concatenate(X_parts, axis=1)
    # 6) Scaler
    if scaler is not None:
        X = scaler.transform(X)
    return X, ids
