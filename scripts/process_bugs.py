#!/usr/bin/env python3
"""
process_bugs.py
- Streams a >100K-bug JSON (array) or NDJSON file
- Extracts raw fields
- Builds cleaned rows 
- Writes CSV chunks of N=10_000 rows (configurable)
"""

import argparse, os, sys, json, csv, re, datetime
from typing import Any, Dict, Iterable, List

URL_RE = re.compile(r'https?://\S+', re.I)
STACKTRACE_HINT_RE = re.compile(r'(?i)(Traceback|stack trace|^#\d+| at .+\(|^\s*Exception:|^Fatal error:)', re.M)
CODE_FENCE_RE = re.compile(r'```|~~~')
WHITESPACE_RE = re.compile(r'\s+')

def parse_boolish(v):
    if v is None:
        return None
    if isinstance(v, bool):
        return int(v)
    s = str(v).strip().lower()
    if s in {"1","true","yes","y","t"}:
        return 1
    if s in {"0","false","no","n","f"}:
        return 0
    if s in {"---","","none","null"}:
        return None
    return 1

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
        return ';'.join(map(str, kw))
    return normalize_text(kw)

def count_list(bug: Dict[str, Any], key: str) -> int:
    v = bug.get(key)
    if isinstance(v, list):
        return len(v)
    return 0

def parse_iso(dt):
    try:
        return datetime.datetime.fromisoformat(dt.replace('Z','+00:00'))
    except Exception:
        return None

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
        "status": bug.get("status"),
        "resolution": bug.get("resolution"),
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
    raw["days_open_est"] = (lastchg - created).days if created and lastchg else None
    return raw

def stream_json_array(fp) -> Iterable[Dict[str, Any]]:
    """
    Stream over a large JSON array without loading it fully.
    Uses `ijson` if available; otherwise falls back to json.load (not recommended for 100k+).
    """
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
        if not line: continue
        try:
            obj = json.loads(line)
            if isinstance(obj, dict):
                yield obj
        except Exception:
            continue

def write_chunk(rows: List[Dict[str, Any]], outpath: str, header: List[str] = None):
    if not rows:
        return
    os.makedirs(os.path.dirname(outpath), exist_ok=True)
    with open(outpath, "w", newline="", encoding="utf-8") as f:
        if header is None:
            header = sorted(rows[0].keys())
        w = csv.DictWriter(f, fieldnames=header)
        w.writeheader()
        for r in rows:
            w.writerow({k: r.get(k, "") for k in header})

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--input", required=True, help="Path to JSON (array) or NDJSON file")
    ap.add_argument("--format", choices=["json","ndjson"], required=True, help="Input file format")
    ap.add_argument("--outdir", required=True, help="Output directory")
    ap.add_argument("--chunk", type=int, default=10000, help="Rows per CSV chunk")
    ap.add_argument("--write-raw", choices=["yes","no"], default="yes")
    ap.add_argument("--write-clean", choices=["yes","no"], default="yes")
    args = ap.parse_args()

    print("Making dir: ", args.outdir)
    os.makedirs(args.outdir, exist_ok=True)
    print("Loading json items...")
    gen = stream_json_array if args.format == "json" else stream_ndjson

    raw_rows, clean_rows = [], []
    raw_idx, clean_idx = 0, 0
    total = 0

    raw_header = None
    clean_header = None

    with open(args.input, "r", encoding="utf-8") as fp:
        for bug in gen(fp):
            total += 1
            if args.write_raw == "yes":
                rr = build_row_raw(bug)
                raw_rows.append(rr)
                if len(raw_rows) >= args.chunk:
                    out = os.path.join(args.outdir, "raw", f"bugs_raw_{raw_idx:03d}.csv")
                    print("Writing raw to: ", out)
                    write_chunk(raw_rows, out, header=raw_header)
                    if raw_header is None and raw_rows:
                        raw_header = sorted(raw_rows[0].keys())
                    raw_rows.clear()
                    raw_idx += 1
                    print("Loading json items...")
            if args.write_clean == "yes":
                cr = build_row_clean(bug)
                clean_rows.append(cr)
                if len(clean_rows) >= args.chunk:
                    out = os.path.join(args.outdir, "clean", f"bugs_clean_{clean_idx:03d}.csv")
                    print("Writing clean to: ", out)
                    write_chunk(clean_rows, out, header=clean_header)
                    if clean_header is None and clean_rows:
                        clean_header = sorted(clean_rows[0].keys())
                    clean_rows.clear()
                    clean_idx += 1
                    print("Loading json items...")

    if args.write_raw == "yes" and raw_rows:
        out = os.path.join(args.outdir, "raw", f"bugs_raw_{raw_idx:03d}.csv")
        print("Writing raw to: ", out)
        write_chunk(raw_rows, out, header=raw_header)
    if args.write_clean == "yes" and clean_rows:
        out = os.path.join(args.outdir, "clean", f"bugs_clean_{clean_idx:03d}.csv")
        print("Writing clean to: ", out)
        write_chunk(clean_rows, out, header=clean_header)

    manifest = {
        "input": os.path.abspath(args.input),
        "format": args.format,
        "outdir": os.path.abspath(args.outdir),
        "chunk": args.chunk,
        "write_raw": args.write_raw,
        "write_clean": args.write_clean,
        "rows_processed": total,
        "timestamp": datetime.datetime.utcnow().isoformat() + "Z"
    }
    with open(os.path.join(args.outdir, "manifest.json"), "w", encoding="utf-8") as mf:
        json.dump(manifest, mf, indent=2)
    print(f"[done] processed rows={total} -> {args.outdir}")

if __name__ == "__main__":
    main()
