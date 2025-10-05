"""
scripts/topic_jobs.py
Topics pipeline: fit BERTopic (on stored embeddings), compute centroids, assign topics and save to shards.

Requires:
- Parquet shards with column 'embedding' (list[float]), plus optional 'summary'/'description'.
- configs/topics.yaml for hyperparams and sampling

Outputs under artifacts/topics/* and updated shards with topic ids.
"""
import argparse, glob, json, os
from pathlib import Path
import numpy as np
import pandas as pd
from tqdm import tqdm

from bertopic import BERTopic
from hdbscan import HDBSCAN
from hdbscan import approximate_predict
from sklearn.feature_extraction.text import CountVectorizer

def load_parquet_glob(glob_pat, cols=None, limit=None, sample_frac=None, sample_max=None, text_cols=("summary","description")):
    files = sorted(glob.glob(glob_pat))
    if not files:
        raise SystemExit(f"No files match: {glob_pat}")
    rows = []
    n_total = 0
    for fp in files:
        df = pd.read_parquet(fp, columns=cols)
        if sample_frac:
            df = df.sample(frac=sample_frac, random_state=42)
        if sample_max and n_total + len(df) > sample_max:
            df = df.iloc[: max(0, sample_max - n_total)]
        rows.append(df)
        n_total += len(df)
        if limit and n_total >= limit:
            break
    return pd.concat(rows, ignore_index=True) if rows else pd.DataFrame()

def texts_from_df(df, text_cols=("summary","description")):
    cols = [c for c in text_cols if c in df.columns]
    if not cols:  # fallback to blank text if none present
        return [""] * len(df)
    return (df[cols].fillna("").astype(str)).agg(" \n".join, axis=1).tolist()

def compute_centroids(E: np.ndarray, labels: np.ndarray):
    """Average normalized embeddings per topic id; ignore -1."""
    mask = labels >= 0
    E_in = E[mask]; y = labels[mask]
    topics = np.unique(y)
    cents = []
    order = []
    for t in topics:
        v = E_in[y==t].mean(axis=0)
        # normalize to unit length
        v = v / (np.linalg.norm(v) + 1e-12)
        cents.append(v)
        order.append(int(t))
    return np.vstack(cents).astype(np.float32), [int(t) for t in order]

def cosine_top(assign_E: np.ndarray, centroids: np.ndarray, threshold: float):
    # E, C are L2-normalized; cosine = dot
    sims = assign_E @ centroids.T
    idx = sims.argmax(axis=1)
    val = sims[np.arange(len(assign_E)), idx]
    out = idx.copy()
    out[val < threshold] = -1
    return out.astype(np.int32), val.astype(np.float32)

def save_manifest(path, encoder_id, topic_ids, centroid_threshold):
    meta = {
        "encoder_id": encoder_id,
        "topic_ids": list(map(int, topic_ids)),
        "default_centroid_threshold": float(centroid_threshold),
    }
    Path(path).write_text(json.dumps(meta, indent=2))

def build_umap(cfg):
    try:
        import umap
    except Exception:
        return None
    p = cfg.get("umap", {})
    return umap.UMAP(
        n_neighbors=p.get("n_neighbors", 15),
        n_components=p.get("n_components", 5),
        min_dist=p.get("min_dist", 0.0),
        metric=p.get("metric", "cosine"),
        random_state=p.get("random_state", 42),
        low_memory=p.get("low_memory", True),
        verbose=p.get("verbose", False),
    )

def run_fit(args):
    import yaml
    cfg = yaml.safe_load(open(args.config))
    topics_dir = Path(args.out_dir); topics_dir.mkdir(parents=True, exist_ok=True)

    # Columns we need
    need_cols = ["embedding", "id", "summary", "description"]
    df = load_parquet_glob(
        args.train_glob, cols=[c for c in need_cols if c], 
        limit=cfg["fit"].get("limit"),
        sample_frac=cfg["fit"].get("sample_frac"),
        sample_max=cfg["fit"].get("sample_max")
    )
    if df.empty:
        raise SystemExit("No rows loaded for topic fit")

    E = np.stack(df["embedding"].values).astype(np.float32)    # shape [N, D]
    # normalize (safety; MiniLM enc usually normalized already)
    E = E / (np.linalg.norm(E, axis=1, keepdims=True) + 1e-12)

    docs = texts_from_df(df, tuple(cfg["fit"].get("text_cols", ["summary","description"])))

    # Build vectorizer for c-TF-IDF keywords
    vectorizer = CountVectorizer(
        stop_words=cfg["ctfidf"].get("stop_words", "english"),
        max_df=cfg["ctfidf"].get("max_df", 0.9),
        min_df=cfg["ctfidf"].get("min_df", 5),
        ngram_range=tuple(cfg["ctfidf"].get("ngram_range", [1,2])),
    )

    umap_model = build_umap(cfg) if cfg["fit"].get("use_umap", True) else None

    # HDBSCAN with prediction_data
    hdbscan_model = HDBSCAN(min_cluster_size=cfg["hdbscan"]["min_topic_size"],
                            prediction_data=cfg["hdbscan"].get("prediction_data", True))


    topic_model = BERTopic(
        embedding_model=None,                # We pass E directly
        umap_model=umap_model,               # << make UMAP explicit (or skip)
        hdbscan_model=hdbscan_model,     # <-- pass in
        verbose=True,
        min_topic_size=cfg["hdbscan"]["min_topic_size"],
        nr_topics=cfg["ctfidf"].get("nr_topics", None),
        top_n_words=cfg["ctfidf"].get("top_n_words", 10),
        calculate_probabilities=False,
        vectorizer_model=vectorizer,
    )
    topics, _ = topic_model.fit_transform(docs, embeddings=E)

    # Save BERTopic (for Track A)
    model_file = topics_dir / "bertopic_model.pkl"  # single file, not a dir
    # optional: atomic write
    tmp_file = topics_dir / "bertopic_model.pkl.tmp"
    topic_model.save(str(tmp_file))
    tmp_file.replace(model_file)  # atomic swap

    # Export topic info + top words
    info = topic_model.get_topic_info()
    info.to_csv(topics_dir / "topic_info.csv", index=False)
    # Flatten top words per topic
    rows = []
    for t in info["Topic"].tolist():
        words = topic_model.get_topic(t) or []
        rows.extend([{"topic": int(t), "rank": i, "word": w, "score": float(s)} for i,(w,s) in enumerate(words)])
    pd.DataFrame(rows).to_csv(topics_dir / "topic_top_words.csv", index=False)

    # Compute centroids from labels (Track B)
    cents, order = compute_centroids(E, np.array(topics))
    np.save(topics_dir / "topic_centroids.npy", cents)

    encoder_id = cfg.get("encoder_id", "model=all-MiniLM-L6-v2|max_len=160|normalize=True")
    threshold = cfg["centroid"].get("threshold_default", 0.35)
    save_manifest(topics_dir / "manifest.json", encoder_id, order, threshold)

    # Write a tiny flag if UMAP was used
    (topics_dir / "umap_used.json").write_text(json.dumps({"use_umap": umap_model is not None}, indent=2))

    print(f"[topics.fit] saved model → {model_file}")
    print(f"[topics.fit] saved centroids → {topics_dir/'topic_centroids.npy'}")
    print(f"[topics.fit] manifest → {topics_dir/'manifest.json'}")

def assign_on_shard(shard_path: Path, cfg, topics_dir: Path, mode: str, centroid_threshold: float, write_suffix: str):
    df = pd.read_parquet(shard_path)
    if "embedding" not in df.columns:
        print(f"[topics.assign] skip (no embedding) {shard_path}")
        return None

    E = np.stack(df["embedding"].values).astype(np.float32)
    E = E / (np.linalg.norm(E, axis=1, keepdims=True) + 1e-12)

    if mode in ("transform", "both"):
        backend = cfg.get("assign", {}).get("predict_backend", "safe")
        model_file = topics_dir / "bertopic_model.pkl"
        if not model_file.exists():
            raise SystemExit(f"[topics.assign] Missing {model_file}. Run 'topics fit' first, or use --mode centroid.")

        if backend == "safe":
            # Pure-embedding path (no text, avoids BERTopic.transform)
            topic_model = BERTopic.load(str(model_file))
            umap_used = True
            umap_flag = topics_dir / "umap_used.json"
            if umap_flag.exists():
                try:
                    umap_used = json.loads(umap_flag.read_text()).get("use_umap", True)
                except Exception:
                    pass

            if umap_used and topic_model.umap_model is not None:
                X = topic_model.umap_model.transform(E)
            else:
                X = E

            labels, strengths = approximate_predict(topic_model.hdbscan_model, X)
            df["topic_id_A"] = labels.astype(int)

        else:
            # Fallback to BERTopic.transform (may segfault on your stack)
            topic_model = BERTopic.load(str(model_file))
            docs = texts_from_df(df, tuple(cfg["fit"].get("text_cols", ["summary","description"])))
            topics_A, _ = topic_model.transform(docs, embeddings=E)
            df["topic_id_A"] = topics_A

    if mode in ("centroid", "both"):
        cents = np.load(topics_dir / "topic_centroids.npy")
        topics_B, sims = cosine_top(E, cents, centroid_threshold)
        df["topic_id_B"] = topics_B
        df["topic_sim_B"] = sims

    out_path = shard_path.with_name(shard_path.stem + write_suffix + shard_path.suffix)
    df.to_parquet(out_path, index=False)
    return out_path

def run_assign(args):
    import yaml
    cfg = yaml.safe_load(open(args.config))
    topics_dir = Path(args.topics_dir)
    threshold = args.threshold if args.threshold is not None else cfg["centroid"].get("threshold_default", 0.35)
    mode = args.mode
    write_suffix = cfg["assign"].get("write_suffix", ".withtopics")

    files = sorted(glob.glob(args.shards_glob))
    if not files:
        raise SystemExit(f"No shards matched: {args.shards_glob}")

    outs = []
    for fp in tqdm(files, desc=f"assign[{mode}]"):
        outp = assign_on_shard(Path(fp), cfg, topics_dir, mode=mode, centroid_threshold=threshold, write_suffix=write_suffix)
        if outp:
            outs.append(outp)
    print(f"[topics.assign] wrote {len(outs)} files with suffix '{write_suffix}'")

def run_refresh(args):
    run_fit(args)
    # now assign on the specified shards immediately
    class A: pass
    a = A()
    a.config = args.config
    a.topics_dir = args.out_dir
    a.shards_glob = args.shards_glob
    a.mode = args.mode
    a.threshold = args.threshold
    run_assign(a)

def main():
    ap = argparse.ArgumentParser()
    sub = ap.add_subparsers(dest="cmd", required=True)

    # fit
    ap_fit = sub.add_parser("fit")
    ap_fit.add_argument("--config", default="configs/topics.yaml")
    ap_fit.add_argument("--train_glob", default="data/processed/train_emb_shards/*.parquet")
    ap_fit.add_argument("--out_dir", default="artifacts/topics")
    ap_fit.set_defaults(func=run_fit)

    # assign
    ap_asn = sub.add_parser("assign")
    ap_asn.add_argument("--config", default="configs/topics.yaml")
    ap_asn.add_argument("--topics_dir", default="artifacts/topics")
    ap_asn.add_argument("--shards_glob", default="data/processed/train_emb_shards/*.parquet")
    ap_asn.add_argument("--mode", choices=["transform","centroid","both"], default="centroid")
    ap_asn.add_argument("--threshold", type=float)
    ap_asn.set_defaults(func=run_assign)

    # refresh (fit + assign)
    ap_ref = sub.add_parser("refresh")
    ap_ref.add_argument("--config", default="configs/topics.yaml")
    ap_ref.add_argument("--train_glob", default="data/processed/train_emb_shards/*.parquet")
    ap_ref.add_argument("--out_dir", default="artifacts/topics")
    ap_ref.add_argument("--shards_glob", default="data/processed/train_emb_shards/*.parquet")
    ap_ref.add_argument("--mode", choices=["transform","centroid","both"], default="centroid")
    ap_ref.add_argument("--threshold", type=float)
    ap_ref.set_defaults(func=run_refresh)

    args = ap.parse_args()
    args.func(args)

if __name__ == "__main__":
    main()
