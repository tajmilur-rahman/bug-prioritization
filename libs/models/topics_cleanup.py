
# Clean & merge topics based on existing artifacts/topics outputs.
# Now supports:
#   --dry-run        : compute merges/removals only; do NOT write topics_map/centroids
#   --report-html FP : write a preview HTML with before/after stats and proposed merges
#
# Usage example:
#   python models/topics_cleanup.py --topics-dir artifacts/topics --out-dir artifacts/topics_clean \\
#       --min-size 10 --cos-thr 0.92 --use-jaccard --jaccard-thr 0.25 \\
#       --dry-run --report-html artifacts/topics_clean/preview.html
import os, json, argparse, numpy as np, pandas as pd
from pathlib import Path

def load_topics_artifacts(topics_dir: Path):
    centroids = np.load(topics_dir / "topic_centroids.npy")
    info = pd.read_csv(topics_dir / "topic_info.csv")  
    # --- Normalize BERTopic CSV schema ---
    info = info.rename(columns={"Topic": "topic_id", "Count": "size"})  # must include: topic_id, size
    required_cols = {"topic_id", "size"}
    missing = required_cols - set(info.columns)
    if missing:
        raise ValueError(f"topic_info.csv missing required columns: {missing}")


    topw = None
    if (topics_dir / "topic_top_words.csv").exists():
        topw = pd.read_csv(topics_dir / "topic_top_words.csv") 
        # --- Normalize BERTopic CSV schema ---
        topw = topw.rename(columns={"topic": "topic_id"})  # must include: topic_id, words or topic_id,word
        required_cols = {"topic_id", "word"}
        missing = required_cols - set(topw.columns)
        if missing:
            raise ValueError(f"topic_top_words.csv missing required columns: {missing}")
    manifest = {}
    if (topics_dir / "manifest.json").exists():
        manifest = json.load(open(topics_dir / "manifest.json","r",encoding="utf-8"))
    return centroids, info, topw, manifest

def cosine_sim_matrix(A: np.ndarray):
    A = A.astype("float32")
    n = (A * A).sum(1, keepdims=True) ** 0.5 + 1e-9
    An = A / n
    return An @ An.T

def jaccard(a: set, b: set) -> float:
    if not a and not b: return 1.0
    if not a or not b: return 0.0
    return len(a & b) / max(1, len(a | b))

def make_preview_html(path, info, removed, merged_pairs, groups):
    """Render a simple HTML report summarizing the proposed cleanup."""
    import datetime
    total_topics = int(info["topic_id"].nunique())
    kept_ids = sorted(set(info["topic_id"].tolist()) - set(removed))
    proposed_new = len(groups.keys())

    # Tables
    df_removed = pd.DataFrame({"topic_id": removed}).sort_values("topic_id")
    df_merged = pd.DataFrame(merged_pairs, columns=["topic_i","topic_j","cosine"]).sort_values(["topic_i","topic_j"])
    df_sizes = info[["topic_id","size"]].sort_values("size", ascending=False)

    html = f"""
<!doctype html>
<html>
<head>
  <meta charset="utf-8" />
  <title>Topics Cleanup Preview</title>
  <style>
    body {{ font-family: Arial, sans-serif; margin: 24px; }}
    h1 {{ margin-top: 0; }}
    .metric {{ display: inline-block; margin-right: 24px; padding: 8px 12px; background: #f4f4f4; border-radius: 8px; }}
    table {{ border-collapse: collapse; width: 100%; margin: 12px 0; }}
    th, td {{ border: 1px solid #ddd; padding: 6px 8px; font-size: 13px; }}
    th {{ background: #fafafa; }}
    .small {{ font-size: 12px; color: #666; }}
  </style>
</head>
<body>
  <h1>Topics Cleanup Preview</h1>
  <div class="small">Generated: {datetime.datetime.utcnow().isoformat()}Z</div>

  <h2>Summary</h2>
  <div class="metric"><b>Total topics (original)</b>: {total_topics}</div>
  <div class="metric"><b>Removed (tiny/noisy)</b>: {len(removed)}</div>
  <div class="metric"><b>Kept (pre-merge)</b>: {len(kept_ids)}</div>
  <div class="metric"><b>Proposed merges</b>: {len(df_merged)}</div>
  <div class="metric"><b>New topics (after merge)</b>: {proposed_new}</div>

  <h2>Largest topics (by size)</h2>
  {df_sizes.head(50).to_html(index=False)}

  <h2>Removed topics</h2>
  {df_removed.to_html(index=False)}

  <h2>Merged pairs (cosine ≥ threshold)</h2>
  {df_merged.to_html(index=False)}
</body>
</html>
"""
    Path(path).parent.mkdir(parents=True, exist_ok=True)
    Path(path).write_text(html, encoding="utf-8")

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--topics-dir", default="artifacts/topics")
    ap.add_argument("--out-dir", default="artifacts/topics_clean")
    ap.add_argument("--min-size", type=int, default=10, help="Drop topics smaller than this size")
    ap.add_argument("--cos-thr", type=float, default=0.92, help="Merge centroids with cosine >= this")
    ap.add_argument("--use-jaccard", action="store_true", help="Also require top-word Jaccard >= jaccard-thr")
    ap.add_argument("--jaccard-thr", type=float, default=0.25)
    ap.add_argument("--dry-run", action="store_true", help="Do not write final mapping/centroids; only preview files")
    ap.add_argument("--report-html", default=None, help="Where to write an HTML preview")
    args = ap.parse_args()

    topics_dir = Path(args.topics_dir); out_dir = Path(args.out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)

    C, info, topw, manifest = load_topics_artifacts(topics_dir)
    # Build word sets if available
    word_sets = {}
    if topw is not None:
        if "words" in topw.columns:
            for _, row in topw.iterrows():
                wid = int(row["topic_id"])
                s = set([w.strip() for w in str(row["words"]).split(",") if w.strip()])
                word_sets[wid] = s
        elif "word" in topw.columns:
            for tid, grp in topw.groupby("topic_id"):
                word_sets[int(tid)] = set(grp["word"].astype(str).tolist())

    info = info.copy()
    info["topic_id"] = info["topic_id"].astype(int)
    keep_mask = (info["topic_id"] != -1) & (info["size"] >= args.min_size)
    removed = info.loc[~keep_mask, "topic_id"].tolist()
    info_keep = info.loc[keep_mask].reset_index(drop=True)

    if C.shape[0] <= max(info["topic_id"]):
        raise SystemExit("Centroids rows do not align with topic ids; expected centroids indexed by topic_id.")

    kept_ids = info_keep["topic_id"].tolist()
    C_keep = C[kept_ids, :]

    S = cosine_sim_matrix(C_keep)

    parent = {tid: tid for tid in kept_ids}
    def find(x):
        while parent[x] != x:
            parent[x] = parent[parent[x]]
            x = parent[x]
        return x
    def union(a,b):
        ra, rb = find(a), find(b)
        if ra != rb: parent[rb] = ra

    merged_pairs = []
    for ii, ti in enumerate(kept_ids):
        for jj in range(ii+1, len(kept_ids)):
            tj = kept_ids[jj]
            sim = float(S[ii, jj])
            if sim >= args.cos_thr:
                ok = True
                if args.use_jaccard:
                    ok = jaccard(word_sets.get(ti,set()), word_sets.get(tj,set())) >= args.jaccard_thr
                if ok:
                    union(ti, tj); merged_pairs.append((ti, tj, sim))

    groups = {}
    for tid in kept_ids:
        root = find(tid)
        groups.setdefault(root, []).append(tid)

    # Always write preview CSVs
    pd.DataFrame(merged_pairs, columns=["topic_i","topic_j","cosine"]).to_csv(out_dir / "merged_pairs.csv", index=False)
    pd.DataFrame({"topic_id": removed}).to_csv(out_dir / "removed_topics.csv", index=False)

    # Optional HTML preview
    if args.report_html:
        make_preview_html(args.report_html, info, removed, merged_pairs, groups)
        print(f"[topics_cleanup] wrote preview HTML: {args.report_html}")

    if args.dry_run:
        print("[topics_cleanup] dry-run mode: not writing final mapping/centroids.")
        return

    # Build mapping and cleaned centroids
    new_id_map = {old: root for root, members in groups.items() for old in members}
    for r in removed:
        new_id_map[r] = -1

    new_roots = sorted(groups.keys())
    C_clean = []
    for root in new_roots:
        rows = np.stack([C[m] for m in groups[root]], axis=0)
        C_clean.append(rows.mean(0))
    C_clean = np.stack(C_clean, axis=0).astype("float32")

    # Save final artifacts
    with open(out_dir / "topics_map.json","w",encoding="utf-8") as f:
        json.dump(new_id_map, f, indent=2)
    np.save(out_dir / "cleaned_centroids.npy", C_clean)

    with open(out_dir / "manifest.clean.json","w",encoding="utf-8") as f:
        json.dump({
            "min_size": args.min_size,
            "cos_thr": args.cos_thr,
            "use_jaccard": args.use_jaccard,
            "jaccard_thr": args.jaccard_thr,
            "n_old": int(info["topic_id"].nunique()),
            "n_removed": int(len(removed)),
            "n_kept": int(len(kept_ids)),
            "n_new": int(len(new_roots)),
        }, f, indent=2)

    print(f"[topics_cleanup] wrote {out_dir} (new topics: {len(new_roots)})")

if __name__ == "__main__":
    main()
