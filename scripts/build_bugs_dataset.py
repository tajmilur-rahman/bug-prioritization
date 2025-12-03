"""
build_bugs_dataset.py — unified dataset builder for both severity & priority.

Usage example:
  python build_bugs_dataset.py \
    --input data/bugs.ndjson \
    --format ndjson \
    --outdir artifacts/bugs_chunks \
    --chunk 100000 \
    --write-clean yes \
    --output data/bugs_cleaned.csv \
    --resolved true \
    --keep-verified \
    --allow-resolutions FIXED \
    --max-missing-frac 0.6
"""

import argparse
import os
import sys
import json
import csv
import re
import datetime
from pathlib import Path
from typing import Any, Dict, Iterable, List, Tuple

import pandas as pd

# -----------------------------
# Regex utilities
# -----------------------------
URL_RE = re.compile(r'https?://\S+', re.I)
STACKTRACE_HINT_RE = re.compile(r'(?i)(Traceback|stack trace|^#\d+| at .+\(|^\s*Exception:|^Fatal error:)', re.M)
CODE_FENCE_RE = re.compile(r'```|~~~')
WHITESPACE_RE = re.compile(r'\s+')

# -----------------------------
# Label ontology & normalizers
# -----------------------------
KNOWN_SEVERITIES = [
    "s1","s2","s3","s4",
    "blocker","critical","major","normal","minor","trivial","enhancement"
]
SEVERITY_MAP = {
    "s1": "S1", "s2": "S2", "s3": "S3", "s4": "S4",
    "blocker": "S1","critical": "S1","major": "S2",
    "normal": "S3","minor": "S3","trivial": "S4","enhancement": "S4"
}

KNOWN_PRIORITIES = [
    "p1","p2","p3","p4","p5",
    "urgent","high","medium","low","unspecified"
]
PRIORITY_MAP = {
    "p1": "P1", "p2": "P2", "p3": "P3", "p4": "P4", "p5": "P5",
    "urgent": "P1","high": "P2","medium": "P3","low": "P4","unspecified": "P5"
}

NULLISH = {"", " ", "nan", "none", "null", "---", "--", "n/a", "na", "(null)"}

# -----------------------------
# Small helpers
# -----------------------------
def parse_boolish(v):
    if v is None:
        return None
    if isinstance(v, bool):
        return int(v)
    s = str(v).strip().lower()
    truthy = {"1","true","yes","y","t"}
    falsy  = {"0","false","no","n","f"}
    nullish = NULLISH
    if s in truthy:
        return 1
    if s in falsy:
        return 0
    if s in nullish:
        return None
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
# Row builders
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
    "cf_crash_signature","cf_rank","cf_webcompat_priority","cf_fx_points",
    "cf_performance_impact","cf_user_story","cf_has_str","cf_qa_whiteboard",
    "cf_fx_iteration","cf_cab_review","cf_accessibility_severity",
    "cf_a11y_review_project_flag"
}

def build_row_raw(bug: Dict[str, Any]) -> Dict[str, Any]:
    summary = normalize_text(bug.get("summary"))
    description = first_description(bug)
    row = {
        "id": bug.get("id"),
        "summary": summary,
        "description": description,
        "priority_raw": bug.get("priority"),
        "severity_raw": bug.get("severity"),
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
        "duplicates_count": count_list(bug, "duplicates"),
        "depends_on_count": count_list(bug, "depends_on"),
        "blocks_count": count_list(bug, "blocks"),
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
# Cleaning / normalization
# -----------------------------
IMPORTANT_COLS_DEFAULT = ["summary","description","product","component","platform","op_sys"]

def clean_training_data(df: pd.DataFrame,
                        max_missing_frac: float = 0.3,
                        important_cols: List[str] = None
                       ) -> Tuple[pd.DataFrame,int,int]:
    n0 = len(df)
    cols = important_cols or IMPORTANT_COLS_DEFAULT
    present_cols = [c for c in cols if c in df.columns]
    if present_cols:
        miss_frac = df[present_cols].isna().mean(axis=1)
    else:
        miss_frac = pd.Series(0.0, index=df.index)
    mask_feat = (miss_frac <= max_missing_frac)
    dropped_feat = (~mask_feat).sum()
    df = df.loc[mask_feat].copy()

    # Label normalization
    # severity
    df["severity_norm"] = (
        df["severity_raw"].astype(str).str.strip().str.lower().map(SEVERITY_MAP)
        .fillna(df["severity_raw"].astype(str))
    )
    # priority
    df["priority_norm"] = (
        df["priority_raw"].astype(str).str.strip().str.lower().map(PRIORITY_MAP)
        .fillna(df["priority_raw"].astype(str))
    )
    df["severity_norm"] = df["severity_norm"].replace(list(NULLISH), None)
    df["priority_norm"] = df["priority_norm"].replace(list(NULLISH), None)


    # Drop rows where both labels missing
    mask_label = df["severity_norm"].notna() | df["priority_norm"].notna()
    dropped_label = (~mask_label).sum()
    df = df.loc[mask_label].copy()

    print(f"[clean_training_data] Dropped insufficient features: {dropped_feat}/{n0}")
    print(f"[clean_training_data] Dropped missing both labels: {dropped_label}/{n0-dropped_feat}")
    return df.reset_index(drop=True), dropped_feat, dropped_label

def add_analytics_fields(df: pd.DataFrame) -> pd.DataFrame:
    df["desc_lines"] = df["description"].fillna("").str.count("\n")
    df["desc_words"] = df["description"].fillna("").str.split().str.len()
    df["desc_unique_words"] = df["description"].fillna("").apply(lambda x: len(set(x.split())))
    df["desc_paragraphs"] = df["description"].fillna("").str.count("\n\n")

    df["duplicate_ratio"] = df["duplicates_count"] / (df["duplicates_count"] + 1)
    df["comment_density"] = df["comment_count"] / (df["days_open_est"] + 1)
    df["attachment_ratio"] = df["attachments_count"] / (df["comment_count"] + 1)
    df["block_dependency_ratio"] = df["blocks_count"] / (df["depends_on_count"] + 1)

    df["has_crash_sig"] = df["cf_crash_signature"].notna().astype(int)
    df["crash_sig_len"] = df["cf_crash_signature"].fillna("").str.len()
    crash_kw = ["EXCEPTION","SIGABRT","SIGSEGV","NullPointer","Crash","Assertion"]
    df["crash_kw_score"] = df["cf_crash_signature"].fillna("").apply(
        lambda x: sum(int(kw.lower() in x.lower()) for kw in crash_kw)
    )
    sev_kw = ["crash","freeze","hang","block","fail","data loss","security","regress"]
    df["severity_kw_score"] = df["description"].fillna("").apply(
        lambda x: sum(int(kw in x.lower()) for kw in sev_kw)
    )
    return df

# -----------------------------
# Main
# -----------------------------
def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--input", required=True, help="Path to JSON or NDJSON file")
    ap.add_argument("--format", choices=["json","ndjson"], required=True)
    ap.add_argument("--outdir", required=True, help="Output directory for chunks")
    ap.add_argument("--chunk", type=int, default=100000)
    ap.add_argument("--write-clean", choices=["yes","no"], default="yes")
    ap.add_argument("--output", required=True, help="Final merged CSV output path")
    ap.add_argument("--resolved", choices=["true","false"], default="true")
    ap.add_argument("--keep-verified", action="store_true")
    ap.add_argument("--allow-resolutions", default="FIXED",
                    help="Comma-separated list")
    ap.add_argument("--max-missing-frac", type=float, default=0.6)
    args = ap.parse_args()

    outdir = Path(args.outdir)
    (outdir / "clean").mkdir(parents=True, exist_ok=True)

    # Phase 1: chunk writing
    print("[phase1] Streaming input → chunked CSVs …")
    gen = stream_json_array if args.format == "json" else stream_ndjson

    clean_rows = []
    clean_keys = set()
    clean_idx = 0
    total = 0

    with open(args.input, "r", encoding="utf-8") as fp:
        for bug in gen(fp):
            total += 1
            if args.write_clean == "yes":
                row = build_row_clean(bug)
                clean_rows.append(row)
                clean_keys.update(row.keys())
                if len(clean_rows) >= args.chunk:
                    out_path = outdir / "clean" / f"bugs_clean_{clean_idx:03d}.csv"
                    print(f"[phase1] Writing clean chunk → {out_path}")
                    write_chunk(clean_rows, str(out_path), header=sorted(clean_keys))
                    clean_rows.clear()
                    clean_idx += 1

    if args.write_clean == "yes" and clean_rows:
        out_path = outdir / "clean" / f"bugs_clean_{clean_idx:03d}.csv"
        print(f"[phase1] Writing final clean chunk → {out_path}")
        write_chunk(clean_rows, str(out_path), header=sorted(clean_keys))

    print(f"[phase1] Processed total {total} bugs.")

    # Phase 2: merge & filter
    print("[phase2] Reading clean chunks → merge & normalize labels …")
    clean_dir = outdir / "clean"
    files = sorted(clean_dir.glob("bugs_clean_*.csv"))
    dfs = []
    n0_total = 0

    for f in files:
        df = pd.read_csv(f, low_memory=False)
        n0_total += len(df)

        if args.resolved.lower() == "true":
            st = df["status"].astype(str).str.upper()
            keep = (st == "RESOLVED")
            if args.keep_verified:
                keep = keep | (st == "VERIFIED")
            df = df.loc[keep].copy()

            if args.allow_resolutions:
                allow = [r.strip().upper() for r in args.allow_resolutions.split(",") if r.strip()]
                if allow:
                    res = df["resolution"].astype(str).str.upper().str.strip()
                    df = df.loc[res.isin(allow)].copy()

        df, drop_feat, drop_lab = clean_training_data(df, max_missing_frac=args.max_missing_frac)
        dfs.append(df)

    if not dfs:
        print("[phase2] No data after filtering → exit.")
        sys.exit(0)

    merged = pd.concat(dfs, ignore_index=True)
    if "id" in merged.columns:
        merged = merged.drop_duplicates("id", keep="last").reset_index(drop=True)

    print(merged['severity_norm'].value_counts(dropna=False))
    print(merged['priority_norm'].value_counts(dropna=False))

    # Add analytics fields
    merged = add_analytics_fields(merged)

    merged_out = Path(args.output)
    merged_out.parent.mkdir(parents=True, exist_ok=True)
    merged.to_csv(merged_out, index=False)
    print(f"[done] Merged {len(merged)} rows → {merged_out}")

if __name__ == "__main__":
    main()
