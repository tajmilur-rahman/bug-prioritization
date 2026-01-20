"""
Phase C: Train severity classifier from Phase B features

Inputs:
  - X_concept_scores.npy                (N x F dense feature matrix)
  - data/bugs_cleaned.csv             (bug_id, severity_norm)
  - feature_index.json          (concept_id -> column index)

Outputs:
  - model.joblib
  - scaler.joblib
  - metrics.json
"""

import json
import numpy as np
import pandas as pd
from pathlib import Path
import sys
from sklearn.model_selection import train_test_split
from sklearn.preprocessing import StandardScaler
from sklearn.ensemble import HistGradientBoostingClassifier
from sklearn.metrics import classification_report, f1_score
import joblib

ROOT = Path(__file__).resolve().parents[3]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))


# =========================
# CONFIG
# =========================
WORKING_DIR = Path("artifacts")
DATA_DIR = Path("data")

MERGED_CONCEPT_FILE = WORKING_DIR / "severity_signals" / "merged_concepts.jsonl"
X_PATH = WORKING_DIR / "severity_scores" / "X_concept_scores.npy"
Y_PATH =  DATA_DIR / "bugs_cleaned.csv"     # bug_id,severity_norm
FEATURE_INDEX_PATH = WORKING_DIR / "severity_scores" / "feature_index.json"
OUT_DIR =  WORKING_DIR / "severity_training" 

OUT_DIR.mkdir(exist_ok=True)

RANDOM_STATE = 42
TEST_SIZE = 0.2

STRONG_THRESHOLD = 0.7
EPS = 1e-9

scores_df = pd.read_parquet(WORKING_DIR / "severity_scores" / "bug_concept_scores.parquet")

rows = []

for _, r in scores_df.iterrows():
    bug_id = r["bug_id"]
    scores = r["concept_scores"]

    # convert None → 0 safely
    vals = np.array([
        float(v) if v is not None else 0.0
        for v in scores.values()
    ], dtype=np.float32)

    active = vals[vals > 0]
    strong = vals[vals >= STRONG_THRESHOLD]

    sum_scores = float(active.sum()) if len(active) else 0.0
    max_score = float(active.max()) if len(active) else 0.0
    num_active = int(len(active))
    num_strong = int(len(strong))

    if len(active) > 0:
        p = active / (active.sum() + EPS)
        entropy = float(-(p * np.log(p + EPS)).sum())
    else:
        entropy = 0.0

    rows.append({
        "bug_id": bug_id,
        "sum_scores": sum_scores,
        "max_score": max_score,
        "num_active": num_active,
        "num_strong": num_strong,
        "entropy": entropy
    })

mag_df = pd.DataFrame(rows)


# ======= Align magnitude features with X =========
# Load X
X_concept = np.load(X_PATH)
# Load bug metadata (must include bug_id in same order as X)
bugs = pd.read_csv(Y_PATH)
assert len(bugs) == X_concept.shape[0]




mask = bugs["severity_norm"].notna()
bugs = bugs[mask]
idx = bugs.index.values



# Merge magnitude features
bugs = bugs.merge(mag_df, left_on="id", right_on="bug_id", how="left")

# Sanity check
assert bugs[["sum_scores", "max_score", "num_active", "entropy"]].isna().sum().sum() == 0



# Normalize magnitude features

MAG_COLS = [
    "sum_scores",
    "max_score",
    "num_active",
    "num_strong",
    "entropy"
]

scaler = StandardScaler()
X_mag = scaler.fit_transform(bugs[MAG_COLS].values.astype(np.float32))


# Concatenate with existing X (original concept matrix)
X_concept = X_concept[idx]
X_final = np.hstack([X_concept, X_mag])
print("\nX final shape: ", X_final.shape)
assert len(bugs) == X_concept.shape[0]



# Train model
CRASH_PATTERNS = [
    r"\bSIGSEGV\b",
    r"\bsegmentation fault\b",
    r"\baccess violation\b",
    r"\bEXCEPTION_ACCESS_VIOLATION\b",
    r"\bcrash signature\b",
    r"\bstack trace\b",
    r"\bbacktrace\b",
    r"\bcrash-stats\b",
    r"\bsocorro\b",
]
import re

CRASH_RE = re.compile("|".join(CRASH_PATTERNS), re.IGNORECASE)

def has_confirmed_crash(text: str) -> int:
    if not isinstance(text, str):
        return 0
    return int(bool(CRASH_RE.search(text)))


crash_flag = (
    bugs["summary"].fillna("") + " " + bugs["description"].fillna("")
).apply(has_confirmed_crash).values.reshape(-1, 1)

# Align length defensively
crash_flag = crash_flag[: X_final.shape[0]]

# Append as last column
X_policy = np.hstack([X_final, crash_flag])

print("New X shape:", X_policy.shape)


SECURITY_PATTERNS = [
    r"\bsecurity\b",
    r"\bvulnerability\b",
    r"\bexploit\b",
    r"\bremote code execution\b",
    r"\bRCE\b",
    r"\bprivilege escalation\b",
    r"\bXSS\b",
    r"\bCSRF\b",
    r"\bCVE-\d{4}-\d+\b",
    r"\bsec-\b",              # Bugzilla security prefix
    r"\bsecurity-sensitive\b",
    r"\bsec-critical\b",
]
SECURITY_RE = re.compile("|".join(SECURITY_PATTERNS), re.IGNORECASE)

def has_security_policy(text: str) -> int:
    if not isinstance(text, str):
        return 0
    return int(bool(SECURITY_RE.search(text)))

security_flag = (
    bugs["summary"].fillna("") + " " + bugs["description"].fillna("")
).apply(has_security_policy).values.reshape(-1, 1)

# Align length defensively
security_flag = security_flag[: X_policy.shape[0]]

# Append
X_policy2 = np.hstack([X_policy, security_flag])

print("New X shape:", X_policy2.shape)


# Map severity to integers if needed
severity_map = {"S1": 0, "S2": 1, "S3": 2, "S4": 3}
y = bugs["severity_norm"].map(severity_map).values

X_tr, X_va, y_tr, y_va = train_test_split(
    X_policy2,
    y,
    test_size=0.2,
    stratify=y,
    random_state=42
)

# Example weights (tune later)
CLASS_WEIGHTS = {
    0: 6.0,
    1: 4.0,
    2: 1.0,
    3: 3.0,
}

sample_weight = np.array([CLASS_WEIGHTS[s] for s in y_tr])

clf = HistGradientBoostingClassifier(
    max_depth=6,
    learning_rate=0.05,
    max_iter=300,
    min_samples_leaf=50,
    random_state=42
)

clf.fit(X_tr, y_tr, sample_weight=sample_weight)

y_pred = clf.predict(X_va)


print("Macro F1:", f1_score(y_va, y_pred, average="macro"))
print(classification_report(y_va, y_pred, digits=3, target_names=["S1", "S2", "S3", "S4"]))


# Diagnostic
from sklearn.inspection import permutation_importance

result = permutation_importance(
    clf,
    X_va,
    y_va,
    n_repeats=5,
    random_state=42,
    scoring="f1_macro"
)

with open(FEATURE_INDEX_PATH) as f:
    feat_index = json.load(f)

inv_index = {v: k for k, v in feat_index.items()}

feat_names = (
    [f"{inv_index[i]} c_{i}" for i in range(X_concept.shape[1])] +
    MAG_COLS + ["policy", "security"]
)

imp = pd.Series(result.importances_mean, index=feat_names)
print("\nImportance: ", imp.sort_values(ascending=False).head(15))



# # Diagnostic check
# coef = pd.Series(
#     clf.coef_.mean(axis=0),
#     index=[f"c_{i}" for i in range(X_concept.shape[1])] + MAG_COLS
# ).sort_values(ascending=False)
# print("\nDiagnostic coef: ")
# print(coef.tail(10))
# print(coef.head(10))




