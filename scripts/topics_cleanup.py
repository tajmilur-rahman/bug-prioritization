"""
topics_cleanup.py 

Inputs:
    artifacts/topics/
        topic_centroids.npy
        topic_info.csv
        topic_top_words.csv
        manifest.json

Outputs:
    artifacts/topics_clean/
        topics_map.json
        cleaned_centroids.npy
        topic_info_clean.csv
        topic_top_words_clean.csv
        manifest.clean.json
"""

import argparse
import json
import numpy as np
import pandas as pd
from pathlib import Path


# -------------------------------------------------------------------
# Utilities
# -------------------------------------------------------------------

def load_topics_artifacts(topics_dir: Path):
    """Load all required files from artifacts/topics/."""
    C = np.load(topics_dir / "topic_centroids.npy")

    info = pd.read_csv(topics_dir / "topic_info.csv")
    info = info.rename(columns={"Topic": "topic_id", "Count": "size"}) \
               .astype({"topic_id": int, "size": int})

    # Load top words
    if (topics_dir / "topic_top_words.csv").exists():
        tw = pd.read_csv(topics_dir / "topic_top_words.csv")
        tw = tw.rename(columns={"topic": "topic_id"})
        tw["topic_id"] = tw["topic_id"].astype(int)
    else:
        tw = None

    # Load manifest
    manifest = {}
    f = topics_dir / "manifest.json"
    if f.exists():
        manifest = json.loads(f.read_text())

    return C, info, tw, manifest


def cosine_sim_matrix(A: np.ndarray):
    """Compute pairwise cosine similarity matrix."""
    A = A.astype("float32")
    n = (A * A).sum(1, keepdims=True) ** 0.5 + 1e-9
    A = A / n
    return A @ A.T


def jaccard(a: set, b: set) -> float:
    if not a and not b:
        return 1.0
    if not a or not b:
        return 0.0
    return len(a & b) / max(1, len(a | b))


# -------------------------------------------------------------------
# Cleanup step
# -------------------------------------------------------------------

def run_cleanup(args):
    topics_dir = Path(args.topics_dir)
    out_dir = Path(args.out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)

    # Load artifacts
    C, info, topw, manifest = load_topics_artifacts(topics_dir)

    # Prepare top words dictionary
    word_sets = {}
    if topw is not None:
        if "word" in topw.columns:
            # One row per word
            for tid, grp in topw.groupby("topic_id"):
                word_sets[int(tid)] = set(grp["word"].astype(str).tolist())

        elif "words" in topw.columns:
            # CSV with comma-separated list
            for _, row in topw.iterrows():
                wid = int(row["topic_id"])
                ws = set([w.strip() for w in str(row["words"]).split(",") if w.strip()])
                word_sets[wid] = ws

    # Filter small/noisy topics
    info = info.copy()
    info["topic_id"] = info["topic_id"].astype(int)
    keep_mask = (info["topic_id"] != -1) & (info["size"] >= args.min_size)

    removed = info.loc[~keep_mask, "topic_id"].tolist()
    info_keep = info.loc[keep_mask].reset_index(drop=True)

    kept_ids = info_keep["topic_id"].tolist()
    if max(kept_ids) >= len(C):
        raise SystemExit("Centroid rows do not align with topic ids.")

    # Centroids to keep
    C_keep = C[kept_ids, :]

    # Cosine similarity between remaining centroids
    S = cosine_sim_matrix(C_keep)

    # Union–Find structure for merging
    parent = {tid: tid for tid in kept_ids}

    def find(x):
        while parent[x] != x:
            parent[x] = parent[parent[x]]
            x = parent[x]
        return x

    def union(a, b):
        ra, rb = find(a), find(b)
        if ra != rb:
            parent[rb] = ra

    # Merge rule
    merged_pairs = []
    for i, ti in enumerate(kept_ids):
        for j in range(i + 1, len(kept_ids)):
            tj = kept_ids[j]
            sim = float(S[i, j])
            if sim >= args.cos_thr:
                ok = True
                if args.use_jaccard:
                    ok = jaccard(
                        word_sets.get(ti, set()),
                        word_sets.get(tj, set())
                    ) >= args.jaccard_thr

                if ok:
                    union(ti, tj)
                    merged_pairs.append((ti, tj, sim))

    # Group topics by root
    groups = {}
    for tid in kept_ids:
        root = find(tid)
        groups.setdefault(root, []).append(tid)

    # Construct new mapping: old_topic_id → new_topic_id
    new_id_map = {old: root for root, members in groups.items() for old in members}
    for r in removed:
        new_id_map[r] = -1

    # Build cleaned centroids
    new_roots = sorted(groups.keys())
    C_clean = []
    for root in new_roots:
        rows = np.stack([C[m] for m in groups[root]], axis=0)
        C_clean.append(rows.mean(0))
    C_clean = np.stack(C_clean, axis=0).astype("float32")

    # Save cleaned centroids
    np.save(out_dir / "cleaned_centroids.npy", C_clean)

    # Save mapping
    (out_dir / "topics_map.json").write_text(json.dumps(new_id_map, indent=2))

    # Save cleaned topic info
    info_clean_rows = []
    for root in new_roots:
        members = groups[root]
        sizes = info_keep[info_keep["topic_id"].isin(members)]["size"]
        info_clean_rows.append({
            "topic_id": root,
            "merged_from": members,
            "total_size": int(sizes.sum())
        })

    pd.DataFrame(info_clean_rows).to_csv(out_dir / "topic_info_clean.csv", index=False)

    # Save cleaned top words
    if topw is not None:
        top_clean = []
        for root, members in groups.items():
            # Collect all words from member topics
            ws = set()
            for t in members:
                ws |= word_sets.get(t, set())
            for w in sorted(ws):
                top_clean.append({"topic_id": root, "word": w})
        pd.DataFrame(top_clean).to_csv(out_dir / "topic_top_words_clean.csv", index=False)

    # Save manifest.clean.json
    (out_dir / "manifest.clean.json").write_text(
        json.dumps({
            "min_size": args.min_size,
            "cos_thr": args.cos_thr,
            "use_jaccard": args.use_jaccard,
            "jaccard_thr": args.jaccard_thr,
            "n_old": int(info["topic_id"].nunique()),
            "n_removed": int(len(removed)),
            "n_kept": int(len(kept_ids)),
            "n_new": int(len(new_roots)),
        }, indent=2)
    )

    print(f"[topics_cleanup] Cleaned topics written to: {out_dir}")
    print(f"[topics_cleanup] New topics count: {len(new_roots)}")
    print(f"[topics_cleanup] Removed topics: {len(removed)}")
    print(f"[topics_cleanup] Merged pairs: {len(merged_pairs)}")


# -------------------------------------------------------------------
# CLI
# -------------------------------------------------------------------

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--topics-dir", default="artifacts/topics")
    ap.add_argument("--out-dir", default="artifacts/topics_clean")
    ap.add_argument("--min-size", type=int, default=10)
    ap.add_argument("--cos-thr", type=float, default=0.92)
    ap.add_argument("--use-jaccard", action="store_true")
    ap.add_argument("--jaccard-thr", type=float, default=0.25)
    args = ap.parse_args()
    run_cleanup(args)


if __name__ == "__main__":
    main()
