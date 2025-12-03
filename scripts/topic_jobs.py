"""
topics_jobs.py — Embedding + Text Topics Pipeline 

Inputs:
    data/embeddings/shards/*.parquet     (id, embedding)
    data/bugs_cleaned.csv                (id, summary, description, etc.)

Outputs:
    artifacts/topics/
        bertopic_model.pkl
        topic_info.csv
        topic_top_words.csv
        topic_centroids.npy
        manifest.json

    data/topics/shards/*.parquet
        id, topic_A, topic_B
"""

import argparse, glob, json, yaml
from pathlib import Path
import numpy as np
import pandas as pd
from tqdm import tqdm

from bertopic import BERTopic
from hdbscan import HDBSCAN, approximate_predict
from sklearn.feature_extraction.text import CountVectorizer


# -------------------------------------------------------------------
# Utilities
# -------------------------------------------------------------------

def merge_embeddings_with_text(emb_shards_glob, bugs_csv, limit=None, sample_frac=None, sample_max=None):
    """Join embeddings shards with bugs_cleaned.csv on id, returning embedding + text."""
    print("[topics] Loading embeddings…")
    shards = sorted(glob.glob(emb_shards_glob))
    if not shards:
        raise SystemExit(f"No embedding shards matched: {emb_shards_glob}")

    E_rows = []
    n_total = 0
    for fp in shards:
        df = pd.read_parquet(fp, columns=["id", "embedding"])
        if sample_frac:
            df = df.sample(frac=sample_frac, random_state=42)
        if sample_max and n_total + len(df) > sample_max:
            df = df.iloc[: max(0, sample_max - n_total)]
        E_rows.append(df)
        n_total += len(df)
        if limit and n_total >= limit:
            break

    E_df = pd.concat(E_rows, ignore_index=True)
    E_df["id"] = E_df["id"].astype(int)
    print(f"Embedding rows  -  {len(E_df)}")

    bugs = pd.read_csv(bugs_csv)
    bugs = bugs[["id", "summary", "description"]].copy()
    bugs["id"] = bugs["id"].astype(int)
    print(f"Bugs rows  -  {len(bugs)}")

    # Merge embeddings with text
    merged = E_df.merge(bugs, on="id", how="left")
    print(f"Merged rows - {len(merged)}")
    print(merged.head())

    # Replace missing text with ""
    merged["summary"] = merged["summary"].fillna("").astype(str)
    merged["description"] = merged["description"].fillna("").astype(str)

    # Combined text
    merged["text"] = (merged["summary"] + "\n" + merged["description"]).str.strip()

    return merged


def compute_centroids(E, labels):
    mask = labels >= 0
    Em = E[mask]
    ym = labels[mask]

    topics = np.unique(ym)
    cents = []
    order = []
    for t in topics:
        v = Em[ym == t].mean(axis=0)
        v = v / (np.linalg.norm(v) + 1e-12)
        cents.append(v)
        order.append(int(t))

    return np.vstack(cents).astype(np.float32), order


def cosine_assign(E, cents, thr):
    sims = E @ cents.T
    idx = sims.argmax(axis=1)
    val = sims[np.arange(len(E)), idx]
    out = idx.copy()
    out[val < thr] = -1
    return out.astype(np.int32), val.astype(np.float32)


# -------------------------------------------------------------------
# FIT PHASE
# -------------------------------------------------------------------
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
    cfg = yaml.safe_load(open(args.config))
    out_dir = Path(args.out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)

    # Load embedding + text
    df = merge_embeddings_with_text(
        args.train_glob,
        args.bugs_csv,
        limit=cfg["fit"].get("limit"),
        sample_frac=cfg["fit"].get("sample_frac"),
        sample_max=cfg["fit"].get("sample_max")
    )

    if df.empty:
        raise SystemExit("[topics.fit] No rows found after merge.")

    # Embeddings
    E = np.vstack(df["embedding"].values).astype(np.float32)
    E = E / (np.linalg.norm(E, axis=1, keepdims=True) + 1e-12)

    texts = df["text"].tolist()

    # HDBSCAN with prediction_data
    hdbscan_model = HDBSCAN(min_cluster_size=cfg["hdbscan"]["min_topic_size"],
                            prediction_data=cfg["hdbscan"].get("prediction_data", True))

    # UMAP MODEL
    umap_model = None
    if cfg["fit"].get("use_umap", True):
        umap_model = build_umap(cfg)

    # Build vectorizer for c-TF-IDF keywords
    vectorizer = CountVectorizer(
        stop_words=cfg["ctfidf"].get("stop_words", "english"),
        max_df=cfg["ctfidf"].get("max_df", 0.9),
        min_df=cfg["ctfidf"].get("min_df", 5),
        ngram_range=tuple(cfg["ctfidf"].get("ngram_range", [1,2])),
    )

    # BERTopic
    topic_model = BERTopic(
        embedding_model=None,
        hdbscan_model=hdbscan_model,
        umap_model=umap_model,           
        vectorizer_model=vectorizer,
        calculate_probabilities=False,
        min_topic_size=cfg["hdbscan"]["min_topic_size"],
        nr_topics=cfg["ctfidf"].get("nr_topics", None),
        top_n_words=cfg["ctfidf"].get("top_n_words", 10),
        verbose=True,
    )

    # Fit transform
    topics, _ = topic_model.fit_transform(texts, embeddings=E)

    # Save model
    tmp = out_dir / "bertopic_model.pkl.tmp"
    final = out_dir / "bertopic_model.pkl"
    topic_model.save(str(tmp))
    tmp.replace(final)

    # Interpretability outputs
    info = topic_model.get_topic_info()
    info.to_csv(out_dir / "topic_info.csv", index=False)

    rows = []
    for t in info["Topic"].tolist():
        topw = topic_model.get_topic(t) or []
        for r, (w, s) in enumerate(topw):
            rows.append({"topic": int(t), "rank": r, "word": w, "score": float(s)})

    pd.DataFrame(rows).to_csv(out_dir / "topic_top_words.csv", index=False)

    # Topic B centroids
    cents, order = compute_centroids(E, np.array(topics))
    np.save(out_dir / "topic_centroids.npy", cents)

    manifest = {
        "encoder_id": cfg.get("encoder_id", args.encoder_id),
        "topic_ids": order,
        "default_centroid_threshold": cfg["centroid"].get("threshold_default", args.threshold),
        "embedding_only": False,
        "text_source": args.bugs_csv
    }
    (out_dir / "manifest.json").write_text(json.dumps(manifest, indent=2))

    (out_dir / "umap_used.json").write_text(json.dumps({"use_umap": umap_model is not None}, indent=2))


    print(f"[topics.fit] topics={len(order)}")
    print(f"[topics.fit] wrote interpretability files in {out_dir}")


# -------------------------------------------------------------------
# ASSIGN PHASE
# -------------------------------------------------------------------

def assign_on_shard(shard_path, cfg, topics_dir, topics_clean_dir, thr, topics_shards_dir):
    df = pd.read_parquet(shard_path, columns=["id", "embedding"])
    if df.empty:
        return None

    E = np.vstack(df["embedding"].values).astype(np.float32)
    E = E / (np.linalg.norm(E, axis=1, keepdims=True) + 1e-12)

    topic_model = BERTopic.load(str(topics_dir / "bertopic_model.pkl"))
    cents = np.load(topics_clean_dir / "cleaned_centroids.npy")
    backend = cfg.get("assign", {}).get("predict_backend", "safe")
    # detect whether UMAP was used
    umap_used = True
    flag_path = topics_dir / "umap_used.json"
    if flag_path.exists():
        try:
            umap_used = json.loads(flag_path.read_text()).get("use_umap", True)
        except:
            pass

    # Topic A
    # UMAP transform if UMAP was used during fit
    if backend == "safe":
        if umap_used and topic_model.umap_model is not None:
            X = topic_model.umap_model.transform(E)
        else:
            X = E

        labels, _ = approximate_predict(topic_model.hdbscan_model, X)
    else:
        # fallback (slower, less stable)
        docs = [""] * len(E)  # Or use text_cols if merging text
        labels, _ = topic_model.transform(docs, embeddings=E)
    df["topic_A_clean"] = labels.astype(int)

    # Topic B
    tB, _ = cosine_assign(E, cents, thr)
    df["topic_B_clean"] = tB.astype(int)

    out_dir = Path(topics_shards_dir) / "shards"
    out_dir.mkdir(parents=True, exist_ok=True)
    out_path = out_dir / f"{str(shard_path.stem).replace("emb_", "tps_")}.topics.parquet"
    df[["id", "topic_A_clean", "topic_B_clean"]].to_parquet(out_path, index=False)
    return out_path


def run_assign(args):
    cfg = yaml.safe_load(open(args.config))
    topics_dir = Path(args.topics_dir)
    topics_clean_dir = Path(args.topics_clean_dir)

    files = sorted(glob.glob(args.shards_glob))
    if not files:
        raise SystemExit(f"No shards matched: {args.shards_glob}")

    outs = []
    thr = args.threshold if args.threshold is not None else cfg["centroid"].get("threshold_default", 0.35)
    for fp in tqdm(files, desc="assign"):
        p = assign_on_shard(Path(fp), cfg, topics_dir, topics_clean_dir, thr, args.topics_shards_dir)
        if p:
            outs.append(p)

    print(f"[topics.assign] wrote {len(outs)} shards → *.topics.parquet")


# -------------------------------------------------------------------
# CLI
# -------------------------------------------------------------------

def main():
    ap = argparse.ArgumentParser()

    sub = ap.add_subparsers(dest="cmd", required=True)

    # FIT
    ap_fit = sub.add_parser("fit")
    ap_fit.add_argument("--config", default="configs/topics.yaml")
    ap_fit.add_argument("--train_glob", default="data/embeddings/shards/*.parquet")
    ap_fit.add_argument("--bugs_csv", default="data/bugs_cleaned.csv")
    ap_fit.add_argument("--out_dir", default="artifacts/topics")
    ap_fit.add_argument("--min_topic_size", type=int, default=30)
    ap_fit.add_argument("--threshold", type=float, default=0.35)
    ap_fit.add_argument("--encoder_id", default="MiniLM-L6-v2")
    ap_fit.set_defaults(func=run_fit)

    # ASSIGN
    ap_asn = sub.add_parser("assign")
    ap_asn.add_argument("--config", default="configs/topics.yaml")
    ap_asn.add_argument("--topics_dir", default="artifacts/topics")
    ap_asn.add_argument("--topics_clean_dir", default="artifacts/topics_clean")
    ap_asn.add_argument("--shards_glob", default="data/embeddings/shards/*.parquet")
    ap_asn.add_argument("--topics_shards_dir", default="data/topics")
    ap_asn.add_argument("--threshold", type=float, default=0.35)
    ap_asn.set_defaults(func=run_assign)

    args = ap.parse_args()
    args.func(args)


if __name__ == "__main__":
    main()
