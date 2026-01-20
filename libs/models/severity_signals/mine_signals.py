import json
import os
import textwrap
from collections import Counter
from pathlib import Path

import pandas as pd
import numpy as np
from openai import OpenAI
import hashlib

import sys
ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from models.common.make_time_split import make_time_split
from saturation import SaturationTracker


# ============================================================
# CONFIG
# ============================================================

SEED = 42
BATCH_SIZE = 10

OUTDIR = Path("artifacts")
OUTDIR.mkdir(exist_ok=True)

RAW_SIGNAL_FILE = OUTDIR / "severity_signals" / "raw_llm_signals.jsonl"
RAW_SIGNAL_FILE.mkdir(parents=True, exist_ok=True)

# ============================================================
# LLM CALL FUNCTION
# ============================================================
client = OpenAI()

CACHE_DIR = OUTDIR / "llm_cache" / "signal_mining"
CACHE_DIR.mkdir(parents=True, exist_ok=True)

def prompt_hash(prompt: str) -> str:
    return hashlib.sha256(prompt.encode("utf-8")).hexdigest()

def call_llm(prompt: str) -> str:
    """
    Actual LLM call.
    Return raw text response.
    """
    h = prompt_hash(prompt)
    cache_file = CACHE_DIR / f"{h}.json"

    if cache_file.exists():
        return json.loads(cache_file.read_text())["response"]

    response = client.chat.completions.create(
        model="gpt-4o-mini",
        messages=[
            {"role": "system", "content": "You are a careful software analysis assistant."},
            {"role": "user", "content": prompt},
        ],
        temperature=0.2,    # low creativity, high consistency
        max_tokens=400,     # controls output tokens enough for a bullet list of abstract signals only, no explanation
    ).choices[0].message.content

    cache_file.write_text(json.dumps({
        "prompt_hash": h,
        "model": "gpt-4o-mini",
        "prompt": prompt,
        "response": response,
    }, indent=2))

    return response


# ============================================================
# STRATIFIED SAMPLING
# ============================================================
rng = np.random.default_rng(SEED)

TOTAL_TARGET = 1200

SEVERITY_FRACTIONS = {
    "S1": 0.22,
    "S2": 0.22,
    "S3": 0.30,
    "S4": 0.26,
}

TIME_BUCKETS = [
(1997, 2000),
(2001, 2004),
(2005, 2008),
(2009, 2012),
(2013, 2014),
(2015, 2016),
(2017, 2018),
(2019, 2020),
(2021, 2100),
]


def assign_time_bucket(year: int) -> str:
    for start, end in TIME_BUCKETS:
        if start <= year <= end:
            return f"{start}-{end}"
    return "unknown"

def stratified_sample(df: pd.DataFrame, label_col: str) -> pd.DataFrame:
    """
        Returns ~TOTAL_TARGET bugs with:
        - severity balance 
        - time coverage
        - randomized selection
    """
    df = df.copy()
    df["year"] = pd.to_datetime(df["creation_time"]).dt.year
    df["time_bucket"] = df["year"].apply(assign_time_bucket)

    samples = []

    for sev, frac in SEVERITY_FRACTIONS.items():
        target_sev = int(TOTAL_TARGET * frac)
        df_sev = df[df[label_col] == sev]

        if df_sev.empty:
            continue

        # distribute roughly across time buckets
        per_bucket = max(5, target_sev // len(TIME_BUCKETS))

        sev_samples = []

        for bucket in df_sev["time_bucket"].unique():
            sub = df_sev[df_sev["time_bucket"] == bucket]
            if len(sub) == 0:
                continue

            n = min(per_bucket, len(sub))
            sev_samples.append(
                sub.sample(n=n, random_state=SEED)
            )

        sev_df = pd.concat(sev_samples)

        # top-up if needed (random within severity)
        if len(sev_df) < target_sev:
            remaining = df_sev.drop(sev_df.index)
            needed = min(target_sev - len(sev_df), len(remaining))
            if needed > 0:
                sev_df = pd.concat([
                    sev_df,
                    remaining.sample(n=needed, random_state=SEED)
                ])

        samples.append(sev_df)

    sampled = pd.concat(samples).sample(frac=1, random_state=SEED)
    return sampled.reset_index(drop=True)


# ============================================================
# BUG RENDERING
# ============================================================
def truncate(text, max_chars=1500):
    return text[:max_chars]


def render_bug(bug: dict) -> str:
    summary = str(bug.get('summary', '')).strip()
    description = str(bug.get('description', '')).strip()

    max_chars = 1500
    if len(description) > max_chars:
        description = truncate(description, 1500)

    text = f"""
    Bug summary:
    {summary}

    Bug description:
    {description}
    """
    return textwrap.dedent(text).strip()

# ============================================================
# PROMPT CONSTRUCTION
# ============================================================

def build_signal_mining_prompt(bugs: list[dict]) -> str:
    bug_blocks = []
    for i, bug in enumerate(bugs, 1):
        bug_blocks.append(
            f"Bug {i}:\n{render_bug(bug)}"
        )

    prompt = f"""
    You are analyzing software bug reports.

    Task:
    Across the following bug reports (with each bug includes bug summary and bug description), identify recurring indicators that reflect
    impact, urgency, or risk. Do NOT assign severity levels. Do NOT judge importance.
    Focus only on abstract signals that might influence severity decisions.

    Instructions:
    - Look for patterns across multiple reports
    - Describe signals abstractly (not keywords or regex)
    - Avoid duplication
    - Ignore any explicit severity labels if present
    - Focus on signals that could plausibly influence urgency, user impact, or risk prioritization

    Bug reports:
    {'\n\n'.join(bug_blocks)}

    Output:
    A bullet list of distinct severity-related signals.
    """
    return textwrap.dedent(prompt).strip()

# ============================================================
# BATCHING
# ============================================================

def make_batches(df: pd.DataFrame) -> list[list[dict]]:
    rows = df.sample(frac=1, random_state=SEED).to_dict("records")
    return [
        rows[i:i+BATCH_SIZE]
        for i in range(0, len(rows), BATCH_SIZE)
    ]

# ============================================================
# Run mining signals
# ============================================================

def run_mining_signals(df: pd.DataFrame, label_col: str):
    sampled = stratified_sample(df, label_col)
    batches = make_batches(sampled)
    tracker = SaturationTracker(
        time_buckets=sorted(sampled["time_bucket"].unique()),
        severity_levels=sorted(sampled[label_col].unique()),
        min_batches_per_bucket=2,
        min_batches_per_severity=2,
        window=12,
        threshold=0.05,
    )

    trying_batch_num = 100
    trying_batch_count = 0
    

    print(f"Running signal mining on {len(sampled)} bugs "
          f"({len(batches)} batches × {BATCH_SIZE})")

    with RAW_SIGNAL_FILE.open("w", encoding="utf-8") as fout:
        for batch_idx, batch in enumerate(batches, 1):
            prompt = build_signal_mining_prompt(batch)
            response = call_llm(prompt)


            fout.write(json.dumps({
                "batch": batch_idx,
                "bug_ids": [b["id"] for b in batch],
                "response": response
            }) + "\n")

            print(f"Batch {batch_idx}/{len(batches)} processed")


            saturated = tracker.update(
                llm_response=response,
                batch_time_buckets={b["time_bucket"] for b in batch},
                batch_severities={b[label_col] for b in batch},
            )

            print(f"Batch {batch_idx} | New signals tracked = {len(tracker.seen_signals)} | " 
                  f"Buckets covered: {dict(tracker.bucket_batch_counts)} | "
                  f"Severity covered; {dict(tracker.severity_batch_counts)}")

            if saturated:
                print(f"Saturation detected at batch {batch_idx}. Stopping Signal Mining.")
                break

            trying_batch_count += 1
            if trying_batch_count == trying_batch_num:
                break

# ============================================================
# QUICK SIGNAL FREQUENCY CHECK
# ============================================================

def extract_bullets(text: str) -> list[str]:
    lines = text.splitlines()
    bullets = []
    for ln in lines:
        ln = ln.strip()
        if ln.startswith("-") or ln.startswith("•"):
            bullets.append(ln.lstrip("-• ").strip())
    return bullets

def quick_frequency_report():
    counter = Counter()
    with RAW_SIGNAL_FILE.open() as f:
        for line in f:
            obj = json.loads(line)
            bullets = extract_bullets(obj["response"])
            for b in bullets:
                counter[b.lower()] += 1

    print("\nTop recurring raw signals:\n")
    for sig, cnt in counter.most_common(30):
        print(f"{cnt:>3}  {sig}")

# ============================================================
# MAIN
# ============================================================

if __name__ == "__main__":
    # Load bug DataFrame
    df = pd.read_csv("data/bugs_cleaned.csv")
    label_col = "severity_norm"

    # Filtering: labels must exist
    miss_label = ~df[label_col].notna() if label_col in df.columns else True
    keep_mask = ~(miss_label)

    dropped = len(df) - keep_mask.sum()
    df = df.loc[keep_mask].reset_index(drop=True)

    print(df[label_col].value_counts(dropna=False))
    print(f"[prep] Dropped {dropped}, remaining {len(df)}")

    # Time-based split
    train_df, val_df, test_df = make_time_split(df, label_col=label_col)
    print(f"[prep] Split → train={len(train_df)}, val={len(val_df)}, test={len(test_df)}")


    # run mining signals for Train set only
    run_mining_signals(train_df, label_col)

    #   3. (Optional) run quick_frequency_report()
    quick_frequency_report()    
