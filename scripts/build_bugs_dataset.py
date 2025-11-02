"""
build_bugs_dataset.py — merged pipeline

Phase 1: Stream -> extract raw/clean rows -> write chunked CSVs
Phase 2: Read clean chunks -> filter RESOLVED/VERIFIED (optional) ->
         clean labels/features -> merge -> write single dataset CSV

Usage example:
  python build_bugs_dataset.py \
    --input data/bugs.json \
    --format json \
    --outdir artifacts/bugs_chunks \
    --chunk 100000 \
    --write-raw no --write-clean yes \
    --output data/bugs_resolved.csv \
    --resolved true --keep-verified --allow-resolutions FIXED --max-missing-frac 0.6

This merges the functionality of your previous process_bugs.py and merge_resolved.py with
all the recommended hardening changes.
"""

import argparse, os, sys, json, csv, re, datetime
from pathlib import Path
from typing import Any, Dict, Iterable, List, Tuple

import pandas as pd

# -----------------------------
# Regex utilities (from processor)
# -----------------------------
URL_RE = re.compile(r'https?://\S+', re.I)
STACKTRACE_HINT_RE = re.compile(r'(?i)(Traceback|stack trace|^#\d+| at .+\(|^\s*Exception:|^Fatal error:)', re.M)
CODE_FENCE_RE = re.compile(r'```|~~~')
WHITESPACE_RE = re.compile(r'\s+')

# -----------------------------
# Label ontology & normalizers
# -----------------------------
KNOWN_PRIORITIES = [
    "P1","P2","P3","P4","P5",
    "Blocker","Critical","Major","Normal","Minor","Trivial"
]
PRIORITY_MAP = {
    "P1":"P1","P2":"P2","P3":"P3","P4":"P4","P5":"P5",
    "Blocker":"P1","Critical":"P1","Major":"P2",
    "Normal":"P3","Minor":"P4","Trivial":"P5",
}
NULLISH = {"", " ", "nan", "none", "null", "---", "--", "n/a", "na", "(null)"}

# -----------------------------
# Small helpers
# -----------------------------

def parse_boolish(v):
    """Strict tri-state: 1/0/None. Unknown tokens -> None."""
    if v is None:
        return None
    if isinstance(v, bool):
        return int(v)
    s = str(v).strip().lower()
    truthy = {"1","true","yes","y","t"}
    falsy  = {"0","false","no","n","f"}
    nullish = NULLISH
    if s in truthy: return 1
    if s in falsy:  return 0
    if s in nullish: return None
    return None

def normalize_text(s: Any) -> str:
    if s is None:
        return ""
    if not isinstance(s, str):
        s = str(s)
    s = WHITESPACE_RE.sub(' ', s).strip()
    return s

def first_description(bug: Dict[str, Any]) -> str:
    comments = bug.get('comments') or []
    if comments and isinstance(comments, list):
        txt = comments[0].get('text') if isinstance(comments[0], dict) else str(comments[0])
        return normalize_text(txt)
    return ""

def join_keywords(bug: Dict[str, Any]) -> str:
    kw = bug.get('keywords')
    if isinstance(kw, list):
        toks = [str(x).strip() for x in kw if str(x).strip()]
        return ';'.join(toks)
    return normalize_text(kw)

def count_list(bug: Dict[str, Any], key: str) -> int:
    v = bug.get(key)
    return len(v) if isinstance(v, list) else 0

def parse_iso(dt):
    if not dt:
        return None
    s = str(dt).strip()
    try:
        return datetime.datetime.fromisoformat(s.replace('Z','+00:00'))
    except Exception:
        try:
            from dateutil import parser  # type: ignore
            return parser.parse(s)
        except Exception:
            return None

# -----------------------------
# Row builders (from processor)
# -----------------------------

def text_feats(summary: str, desc: str) -> Dict[str, Any]:
    text = f"{summary} [SEP] {desc}".strip()
    return {
        "summary_len": len(summary.split()),
        "desc_len": len(desc.split()),
        "url_count": len(URL_RE.findall(text)),
        "code_fence_count": len(CODE_FENCE_RE.findall(text)),
        "has_stacktrace": 1 if STACKTRACE_HINT_RE.search(text) else 0,
    }

CF_SCALAR_KEEP = {
    "cf_crash_signature", "cf_rank", "cf_webcompat_priority", "cf_fx_points", "cf_performance_impact",
    "cf_user_story", "cf_has_str", "cf_qa_whiteboard", "cf_fx_iteration", "cf_cab_review",
    "cf_accessibility_severity", "cf_a11y_review_project_flag"
}

def build_row_raw(bug: Dict[str, Any]) -> Dict[str, Any]:
    summary = normalize_text(bug.get("summary"))
    description = first_description(bug)
    row = {
        "id": bug.get("id"),
        "summary": summary,
        "description": description,
        "priority": bug.get("priority"),
        "severity": bug.get("severity"),
        "status": (bug.get("status") or "").upper(),
        "resolution": (bug.get("resolution") or "").upper(),
        "is_open": bug.get("is_open"),
        "type": bug.get("type"),
        "product": bug.get("product"),
        "component": bug.get("component"),
        "version": bug.get("version"),
        "platform": bug.get("platform"),
        "op_sys": bug.get("op_sys"),
        "classification": bug.get("classification"),
        "creator": bug.get("creator"),
        "assigned_to": bug.get("assigned_to"),
        "qa_contact": bug.get("qa_contact"),
        "url": bug.get("url"),
        "whiteboard": normalize_text(bug.get("whiteboard")),
        "alias": ';'.join(bug.get("alias") or []) if isinstance(bug.get("alias"), list) else bug.get("alias"),
        "dupe_of": bug.get("dupe_of"),
        "creation_time": bug.get("creation_time"),
        "last_change_time": bug.get("last_change_time"),
        "cf_last_resolved": bug.get("cf_last_resolved"),
        "comment_count": bug.get("comment_count"),
        "votes": bug.get("votes"),
        "target_milestone": bug.get("target_milestone"),
        "keywords": join_keywords(bug),
    }
    for k in CF_SCALAR_KEEP:
        row[k] = bug.get(k)
    return row

def build_row_clean(bug: Dict[str, Any]) -> Dict[str, Any]:
    raw = build_row_raw(bug)
    feats = text_feats(raw["summary"] or "", raw["description"] or "")
    counts = {
        "duplicates_count": count_list(bug,"duplicates"),
        "depends_on_count": count_list(bug,"depends_on"),
        "blocks_count": count_list(bug,"blocks"),
        "cc_count": len(bug.get("cc") or []),
        "attachments_count": len(bug.get("attachments") or []),
        "regressions_count": len(bug.get("regressions") or []),
        "regressed_by_count": len(bug.get("regressed_by") or []),
        "see_also_count": len(bug.get("see_also") or []),
    }
    raw.update(feats)
    raw.update(counts)
    # normalize cf_has_str to numeric flag
    try:
        raw["cf_has_str_flag"] = parse_boolish(raw.get("cf_has_str"))
    except Exception:
        raw["cf_has_str_flag"] = None
    created = parse_iso(raw.get("creation_time")) if raw.get("creation_time") else None
    lastchg = parse_iso(raw.get("last_change_time")) if raw.get("last_change_time") else None
    if created and lastchg:
        delta_days = (lastchg - created).total_seconds() / 86400.0
        raw["days_open_est"] = max(delta_days, 0.0)
    else:
        raw["days_open_est"] = None
    raw["is_open_flag"] = parse_boolish(raw.get("is_open"))
    return raw

# -----------------------------
# Streaming readers
# -----------------------------

def stream_json_array(fp) -> Iterable[Dict[str, Any]]:
    """Stream over a large JSON array; tries ijson first."""
    try:
        import ijson  # type: ignore
        for obj in ijson.items(fp, "item"):
            if isinstance(obj, dict):
                yield obj
    except Exception:
        data = json.load(fp)
        if isinstance(data, list):
            for obj in data:
                if isinstance(obj, dict):
                    yield obj

def stream_ndjson(fp) -> Iterable[Dict[str, Any]]:
    for line in fp:
        line = line.strip()
        if not line:
            continue
        try:
            obj = json.loads(line)
            if isinstance(obj, dict):
                yield obj
        except Exception:
            continue

# -----------------------------
# CSV writing with unioned headers
# -----------------------------

def write_chunk(rows: List[Dict[str, Any]], outpath: str, header: List[str]):
    if not rows:
        return
    os.makedirs(os.path.dirname(outpath), exist_ok=True)
    with open(outpath, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=header)
        w.writeheader()
        for r in rows:
            w.writerow({k: r.get(k, "") for k in header})

# -----------------------------
# Cleaning/merging phase (ex-merge_resolved)
# -----------------------------
IMPORTANT_COLS_DEFAULT = ["summary","description","product","component","platform","op_sys"]

def clean_training_data(df: pd.DataFrame,
                        label_col: str = "priority",
                        max_missing_frac: float = 0.3,
                        important_cols: list[str] | None = None) -> pd.DataFrame:
    """Drop rows with excessive missing features and invalid labels."""
    n0 = len(df)
    # A) Missing features over IMPORTANT columns only (exclude label)
    cols = important_cols or IMPORTANT_COLS_DEFAULT
    present_cols = [c for c in cols if c in df.columns]
    if not present_cols:
        # Nothing to score; skip feature-based dropping
        miss_frac = pd.Series(0.0, index=df.index)
    else:
        miss_frac = df[present_cols].isna().mean(axis=1)
    mask_a = (miss_frac <= max_missing_frac)
    dropped_a = int((~mask_a).sum())
    df = df.loc[mask_a].copy()

    # B) Labels
    # drop true nulls before string ops
    df = df[df[label_col].notna()].copy()
    lab = df[label_col].astype(str).str.strip()
    lab_lower = lab.str.lower()
    lab = lab.mask(lab_lower.isin(NULLISH))
    # map mixed ontology to P1..P5; unmapped stay as-is
    lab = lab.map(PRIORITY_MAP).fillna(lab)
    valid = lab.isin(["P1","P2","P3","P4","P5"])  # enforce canonical set
    dropped_b = int((~valid).sum())
    df = df.loc[valid].copy()
    df[label_col] = lab[valid].to_numpy()

    print(f"[clean_training_data] Missing-feat drop: {dropped_a} / {n0} ({dropped_a/max(n0,1):.1%}) on {present_cols}")
    n_after_a = n0 - dropped_a
    print(f"[clean_training_data] Label drop: {dropped_b} / {max(n_after_a,1)} ({(dropped_b/max(n_after_a,1)):.1%})")
    return df.reset_index(drop=True), dropped_a, dropped_b

# -----------------------------
# Main
# -----------------------------

def main():
    ap = argparse.ArgumentParser()
    # Phase 1 (stream -> chunks)
    ap.add_argument("--input", required=True, help="Path to JSON (array) or NDJSON file")
    ap.add_argument("--format", choices=["json","ndjson"], required=True, help="Input file format")
    ap.add_argument("--outdir", required=True, help="Output directory for chunked CSVs")
    ap.add_argument("--chunk", type=int, default=100000, help="Rows per CSV chunk")
    ap.add_argument("--write-raw", choices=["yes","no"], default="yes")
    ap.add_argument("--write-clean", choices=["yes","no"], default="yes")

    # Phase 2 (merge/filter)
    ap.add_argument("--output", required=True, help="Final merged CSV output path")
    ap.add_argument("--resolved", choices=["true","false"], default="true")
    ap.add_argument("--keep-verified", action="store_true", help="Also keep status=VERIFIED")
    ap.add_argument("--allow-resolutions", default="FIXED", help="Comma list of resolutions to allow; empty=allow all")
    ap.add_argument("--max-missing-frac", type=float, default=0.6)

    args = ap.parse_args()

    outdir = Path(args.outdir)
    outdir.mkdir(parents=True, exist_ok=True)

    """
    # -------- Phase 1: stream & write chunks --------
    print("[phase1] Streaming input -> chunked CSVs …")
    gen = stream_json_array if args.format == "json" else stream_ndjson

    raw_rows: List[Dict[str, Any]] = []
    clean_rows: List[Dict[str, Any]] = []
    raw_keys, clean_keys = set(), set()
    raw_idx = clean_idx = 0
    total = 0

    with open(args.input, "r", encoding="utf-8") as fp:
        for bug in gen(fp):
            total += 1
            if args.write_raw == "yes":
                rr = build_row_raw(bug)
                raw_rows.append(rr)
                raw_keys.update(rr.keys())
                if len(raw_rows) >= args.chunk:
                    out = outdir / "raw" / f"bugs_raw_{raw_idx:03d}.csv"
                    print("[phase1] Writing raw ->", out)
                    write_chunk(raw_rows, str(out), header=sorted(raw_keys))
                    raw_rows.clear(); raw_idx += 1
            if args.write_clean == "yes":
                cr = build_row_clean(bug)
                clean_rows.append(cr)
                clean_keys.update(cr.keys())
                if len(clean_rows) >= args.chunk:
                    out = outdir / "clean" / f"bugs_clean_{clean_idx:03d}.csv"
                    print("[phase1] Writing clean ->", out)
                    write_chunk(clean_rows, str(out), header=sorted(clean_keys))
                    clean_rows.clear(); clean_idx += 1

    if args.write_raw == "yes" and raw_rows:
        out = outdir / "raw" / f"bugs_raw_{raw_idx:03d}.csv"
        print("[phase1] Writing raw (final) ->", out)
        write_chunk(raw_rows, str(out), header=sorted(raw_keys))
    if args.write_clean == "yes" and clean_rows:
        out = outdir / "clean" / f"bugs_clean_{clean_idx:03d}.csv"
        print("[phase1] Writing clean (final) ->", out)
        write_chunk(clean_rows, str(out), header=sorted(clean_keys))

    manifest = {
        "input": os.path.abspath(args.input),
        "format": args.format,
        "outdir": str(outdir.resolve()),
        "chunk": args.chunk,
        "write_raw": args.write_raw,
        "write_clean": args.write_clean,
        "rows_processed": total,
        "timestamp": datetime.datetime.utcnow().isoformat() + "Z",
    }
    with open(outdir / "manifest.json", "w", encoding="utf-8") as mf:
        json.dump(manifest, mf, indent=2)
"""
    # -------- Phase 2: merge & filter --------
    dropped_status = 0
    dropped_resolution = 0
    dropped_clean_missing = 0
    dropped_clean_label = 0
    print("[phase2] Reading clean chunks -> filtering -> merging …")
    merged_out = Path(args.output)
    want_resolved = args.resolved.lower() == "true"
    allow_resolutions = [r.strip().upper() for r in args.allow_resolutions.split(",") if r.strip()]

    dfs: List[pd.DataFrame] = []
    classes: List[str] = []
    unresolved = 0
    invalid = 0
    n0_total = 0

    clean_dir = outdir / "clean"
    files = sorted(clean_dir.glob("bugs_clean_*.csv")) if clean_dir.exists() else []
    if args.write_clean != "yes" and not files:
        print("[phase2] No clean chunks found; nothing to merge.")
        sys.exit(0)

    for f in files:
        df = pd.read_csv(f)
        n0_total += len(df)

        if want_resolved:
            # normalize status/resolution casing
            st = df.get("status")
            st = st.astype(str).str.upper() if st is not None else pd.Series([""], index=df.index)
            keep_status = st.eq("RESOLVED")
            # optionally include VERIFIED
            if getattr(args, "keep_verified", False):
                keep_status = keep_status | st.eq("VERIFIED")
            n_before = len(df)
            df = df.loc[keep_status].copy()
            unresolved += (n_before - len(df))
            dropped_status += (n_before - len(df))

            if allow_resolutions and "resolution" in df.columns:
                n_before_res = len(df)
                res = df["resolution"].astype(str).str.upper().str.strip()
                df = df.loc[res.isin(allow_resolutions)].copy()
                dropped_resolution += (n_before_res - len(df))

        n_before_clean = len(df)
        df, drop_miss, drop_lab = clean_training_data(df, label_col="priority", max_missing_frac=args.max_missing_frac)
        invalid += (n_before_clean - len(df))
        dropped_clean_missing += drop_miss
        dropped_clean_label += drop_lab

        classes.extend(pd.unique(df["priority"]))
        dfs.append(df)

    if not dfs:
        print("[phase2] No rows matched filters; nothing to write.")
        sys.exit(0)

    merged = pd.concat(dfs, ignore_index=True)
    dropped_dedup = 0
    if "id" in merged.columns:
        n_before_dedup = len(merged)
        merged = merged.drop_duplicates("id", keep="last").reset_index(drop=True)
        dropped_dedup = n_before_dedup - len(merged)

    merged_out.parent.mkdir(parents=True, exist_ok=True)
    merged.to_csv(merged_out, index=False)

    # class order summary
    order = [c for c in ["P1","P2","P3","P4","P5"] if c in set(classes)]
    extras = sorted([c for c in pd.unique(classes) if c not in set(order)])
    classes_final = order + extras

    print("[phase2] Classes:", classes_final)
    print(f"[phase2] Drop summary (counts and % of original {n0_total}):")
    print(f"  status filter:     {dropped_status:7d}  ({dropped_status/max(n0_total,1):.1%})")
    print(f"  resolution filter: {dropped_resolution:7d}  ({dropped_resolution/max(n0_total,1):.1%})")
    print(f"  missing features:  {dropped_clean_missing:7d}  ({dropped_clean_missing/max(n0_total,1):.1%})")
    print(f"  invalid labels:    {dropped_clean_label:7d}  ({dropped_clean_label/max(n0_total,1):.1%})")
    print(f"  deduplicated:      {dropped_dedup:7d}  ({dropped_dedup/max(n0_total,1):.1%})")

    kept = len(merged)
    dropped_total = (dropped_status + dropped_resolution +
                    dropped_clean_missing + dropped_clean_label + dropped_dedup)
    # This should equal n0_total - kept; print both to verify
    print(f"[phase2] Kept rows: {kept}")
    print(f"[phase2] Dropped total (sum of above): {dropped_total} | Check: n0_total - kept = {n0_total - kept}")

    print(f"[done] Merged {len(merged)} rows -> {merged_out}")

if __name__ == "__main__":
    main()
