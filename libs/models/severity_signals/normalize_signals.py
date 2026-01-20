import re
from collections import defaultdict
from pathlib import Path
import json
from collections import Counter

import sys
ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))


WORKING_DIR = Path("artifacts")
RAW_SIGNAL_FILE = WORKING_DIR / "severity_signals" / "raw_llm_signals.jsonl"
NORMALIZED_SIGNAL_FILE = WORKING_DIR / "severity_signals" / "nomalized_signals.jsonl"

# Regex to capture **Label**: explanation
SIGNAL_RE = re.compile(
    r"-\s+\*\*(.+?)\*\*\s*:\s*(.+?)(?=\n\n-|\Z)",
    re.DOTALL
)

def normalize_label(label: str) -> str:
    """
    Conservative normalization:
    - lowercase
    - strip punctuation
    - replace spaces with underscores
    - DO NOT remove semantic words yet
    """
    label = label.lower()
    label = re.sub(r"[^\w\s]", "", label)
    label = re.sub(r"\s+", "_", label)
    return label.strip("_")

def extract_canonical_signals(raw_batches):
    signals = defaultdict(lambda: {
        "label_raw_variants": set(),
        "occurrences": []
    })

    for entry in raw_batches:
        batch = entry["batch"]
        bug_ids = entry.get("bug_ids", [])
        text = entry["response"]

        for raw_label, explanation in SIGNAL_RE.findall(text):
            canon = normalize_label(raw_label)

            signals[canon]["label_raw_variants"].add(raw_label.strip())
            signals[canon]["occurrences"].append({
                "batch": batch,
                "bug_ids": bug_ids,
                "explanation": explanation.strip()
            })

    # convert sets to lists for JSON
    for k in signals:
        signals[k]["label_raw_variants"] = list(signals[k]["label_raw_variants"])

    return signals

def print_signal(canonical_signals, canon, n=3):
    sig = canonical_signals[canon]
    print("Canonical:", canon)
    print("Variants:", sig["label_raw_variants"])
    print("Examples:")
    for ex in sig["occurrences"][:n]:
        print("-", ex["explanation"][:200], "...")

def run_normalize_signals():
    raw_batches = []
    with open(RAW_SIGNAL_FILE) as f:
        for line in f:
            raw_batches.append(json.loads(line))

    canonical_signals = extract_canonical_signals(raw_batches)

    # Save for inspection
    with NORMALIZED_SIGNAL_FILE.open("w", encoding="utf-8") as fout:
        for key in list(canonical_signals.keys()):
            canon = {"label": key}
            canon.update(canonical_signals[key])
            fout.write(json.dumps(
                canon) + "\n")
    

    print("Canonical signals: ", len(list(canonical_signals.keys())))
    for cannon in list(canonical_signals.keys()):
        print(cannon)

    print_signal(canonical_signals, "performance_degradation")
    print_signal(canonical_signals, "crashes_and_stability_issues")

    print(Counter(len(v["occurrences"]) for v in canonical_signals.values()))


run_normalize_signals()
