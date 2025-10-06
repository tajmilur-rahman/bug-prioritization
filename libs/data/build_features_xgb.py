import numpy as np, pandas as pd
from typing import Dict, Tuple, Any, Optional
from sklearn.preprocessing import OneHotEncoder
from sklearn.feature_extraction import FeatureHasher

def extract_embedding(df: pd.DataFrame, emb_col: str='embedding') -> np.ndarray:
    if emb_col in df.columns:
        return np.array(df[emb_col].tolist(), dtype=np.float32)
    emb_cols = [c for c in df.columns if c.startswith('emb_')]
    if not emb_cols:
        raise ValueError("No embedding found: expected 'embedding' list column or 'emb_*' columns.")
    return df[emb_cols].to_numpy(np.float32)

def assign_topic_centroid(E: np.ndarray, centroids: np.ndarray, thr: float=0.35) -> np.ndarray:
    E = E / (np.linalg.norm(E, axis=1, keepdims=True)+1e-12)
    C = centroids / (np.linalg.norm(centroids, axis=1, keepdims=True)+1e-12)
    sims = E @ C.T
    tid = sims.argmax(1)
    m = sims.max(1)
    tid[m < thr] = -1
    return tid.astype(np.int32)

def encode_categorical_xgb(df: pd.DataFrame, cfg: Dict) -> Tuple[np.ndarray, Dict]:
    cats = cfg.get('categorical', {}).get('fields', [])
    if not cats:
        return np.zeros((len(df),0), np.float32), {}
    Xs, meta = [], {}
    ht = cfg.get('categorical', {}).get('hashing_trick', {'enabled': False})
    for col in cats:
        vals = df[col].fillna('__NA__').astype(str)
        if ht.get('enabled', False) and col in ht.get('fields', []):
            vc = vals.value_counts()
            keep = set(vc.head(ht.get('topk_pass', 100)).index)
            top_mask = vals.isin(keep)
            ohe = OneHotEncoder(handle_unknown='ignore', sparse_output=False)
            X_top = ohe.fit_transform(vals[top_mask].to_frame()) if top_mask.any() else np.zeros((0,0), dtype=np.float32)
            Xo = np.zeros((len(vals), X_top.shape[1]), dtype=np.float32)
            if top_mask.any():
                Xo[top_mask.values] = X_top.astype(np.float32)
            meta[f"{col}_ohe_cats"] = ohe.categories_[0].tolist() if X_top.shape[1] else []
            buckets = int(ht.get('buckets', 512))
            fh = FeatureHasher(n_features=buckets, input_type='string', alternate_sign=False)
            if (~top_mask).any():
                X_hash = fh.transform(vals[~top_mask].to_numpy()).toarray().astype(np.float32)
            else:
                X_hash = np.zeros((0, buckets), dtype=np.float32)
            Xh = np.zeros((len(vals), buckets), dtype=np.float32)
            if (~top_mask).any():
                Xh[~top_mask.values] = X_hash
            Xs += [Xo, Xh]
            meta[f"{col}_hash_buckets"] = buckets
        else:
            ohe = OneHotEncoder(handle_unknown='ignore', sparse_output=False)
            X = ohe.fit_transform(vals.to_frame()).astype(np.float32)
            Xs.append(X)
            meta[f"{col}_ohe_cats"] = ohe.categories_[0].tolist()
    Xcat = np.hstack(Xs).astype(np.float32) if Xs else np.zeros((len(df),0), np.float32)
    return Xcat, meta

def build_xgb_features(df: pd.DataFrame, cfg: Dict, centroids: np.ndarray=None, threshold: float=0.35):
    E = extract_embedding(df, cfg.get('text', {}).get('embedding_col', 'embedding'))
    if cfg.get('topics', {}).get('use', True):
        if cfg['topics'].get('source','centroid')=='transform' and 'topic_id_A' in df.columns:
            tid = df['topic_id_A'].to_numpy(np.float32).reshape(-1,1)
        else:
            if centroids is None:
                raise ValueError('centroids required for centroid-based topic assignment')
            tid = assign_topic_centroid(E, centroids, threshold).reshape(-1,1).astype(np.float32)
    else:
        tid = np.zeros((len(df),1), np.float32)
    num_cols = cfg.get('numeric', {}).get('fields', [])
    Xnum = df[num_cols].astype(np.float32).to_numpy() if num_cols else np.zeros((len(df),0), np.float32)
    Xcat, cat_meta = encode_categorical_xgb(df, cfg)
    mask = cfg.get('regularization', {}).get('feature_ablation_switches', {'use_text': True,'use_metadata': True,'use_numeric': True,'use_topic': True})
    pieces = []
    if mask.get('use_text', True):     pieces.append(E)
    if mask.get('use_topic', True):    pieces.append(tid)
    if mask.get('use_metadata', True): pieces.append(Xcat)
    if mask.get('use_numeric', True):  pieces.append(Xnum)
    X = np.hstack(pieces).astype(np.float32) if pieces else np.zeros((len(df),0), np.float32)
    y = df['priority'].to_numpy()
    meta = {'cat_meta': cat_meta, 'dims': {'emb': E.shape[1], 'cat': Xcat.shape[1], 'num': Xnum.shape[1]}}
    return X, y, meta
