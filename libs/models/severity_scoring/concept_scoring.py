"""
Bug scoring based on severity concepts/features 

Requirements:
  pip install sentence-transformers numpy pandas tqdm

Inputs:
  - merged_concepts.jsonl       (merged concepts from severity signals)
  - data/bugs_cleaned.csv       (id, summary, description)

Outputs:
  - concept_embeddings.npy
  - bug_concept_scores.parquet
  - X_concept_scores.npy
  - feature_index.json
"""

import re
import json
import os
import sys
from pathlib import Path
import numpy as np
import pandas as pd
from tqdm import tqdm
from sentence_transformers import SentenceTransformer

ROOT = Path(__file__).resolve().parents[3]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from bug_embedding_preprocessing import build_embedding_text
from scripts.embedding.embedding_common import run_embed, embed

# =========================
# CONFIG
# =========================
WORKING_DIR = Path("artifacts")
DATA_DIR = Path("data")

MERGED_CONCEPT_FILE = WORKING_DIR / "severity_signals" / "merged_concepts.jsonl"
BUG_FILE =  DATA_DIR / "bugs_cleaned.csv"
OUT_DIR =  WORKING_DIR / "severity_scores" 

BUG_MAX_TOKENS = 190
CONCEPT_MAX_TOKENS = 190    # 128
SIM_THRESHOLD = 0.35

os.makedirs(OUT_DIR, exist_ok=True)


# =========================
# TEXT UTILITIES
# =========================

def truncate_tokens(text, max_tokens):
    tokens = text.split()
    return " ".join(tokens[:max_tokens])


def build_bug_embedding_text(title, description):
    text = f"""
    Bug title: {title}
    Bug description: {description}
    """

    text_to_embed = build_embedding_text(text)

    return truncate_tokens(text_to_embed, BUG_MAX_TOKENS)


def build_concept_text(name, explanations):
    joined = " ".join(explanations[:5])
    text = f"""
    Concept: {name.replace('_', ' ')}
    Explanation: {joined}
    """
    return truncate_tokens(text, CONCEPT_MAX_TOKENS)


# =========================
# LOAD DATA
# =========================
def load_data():
    # Concepts
    concepts = []
    with open(MERGED_CONCEPT_FILE, "r", encoding="utf-8") as f:
        for line in f:
            concepts.append(json.loads(line))

    concept_ids = []
    concept_texts = []
    concept_id_roles = {}

    for c in concepts:
        concept_ids.append(c["merged_id"])
        concept_texts.append(
            build_concept_text(c["merged_name"], c["evidence"])
        )
        concept_id_roles[c["merged_id"]] = c["role"]


    # Bugs
    bugs = pd.read_csv(BUG_FILE)

    assert {"id", "summary", "description"}.issubset(bugs.columns)

    return concept_ids, concept_texts, concept_id_roles , bugs

# =========================
# EMBED CONCEPTS (CACHE)
# =========================
def embed_concepts(concept_texts):
    concept_emb_path = os.path.join(OUT_DIR, "concept_embeddings.npy")

    if os.path.exists(concept_emb_path):
        concept_embeddings = np.load(concept_emb_path)
    else:
        concept_embeddings = embed(concept_texts)
        np.save(concept_emb_path, concept_embeddings)

    return concept_embeddings


# =========================
# SCORE BUGS
# =========================
def shape_scores(scores, concept_roles):
    shaped = {}

    for cid, score in scores.items():
        role = concept_roles[cid]

        if role == "core":
            shaped[cid] = max(0.0, (score - 0.30) / 0.70)
        elif role == "impact":
            shaped[cid] = max(0.0, score - 0.20)
        else:  # context
            shaped[cid] = 1.0 if score > 0.40 else 0.0

    return shaped

def needs_llm(scores):
    top = sorted(scores.values(), reverse=True)[:3]

    if len(top) == 0:
        return False

    if 0.35 < top[0] < 0.55:
        return True

    if scores.get("MC_security_and_data_exposure", 0) > 0.25:
        return True

    if (
        scores.get("MC_regression_from_previous_behavior", 0) > 0.3
        and scores.get("MC_performance_and_resource_issues", 0) > 0.3
    ):
        return True

    return False

def llm_refine_scores(bug_text, candidate_scores):
    """
    Replace with OpenAI / local LLM call later.
    Must return dict {concept_id: score}
    """
    #raise NotImplementedError

    # LLM refinement prompt
    llm_prompt = f"""
    You are scoring bug severity signals.

    Bug:
    {bug_text}

    Score each concept from 0–3:
    - stability_crashes
    - performance_and_resource_issues
    - security_and_data_exposure
    - functional_correctness_errors
    - user_experience_degradation

    Return JSON only.

    """

    # print("\nNeed LLM refine for scores but SKIP LLM refine for now.")
    return candidate_scores


if __name__ == "__main__":
    # Prepare inputs
    concept_ids, concept_texts, concept_id_roles , bugs = load_data()
    bug_texts = []
    bug_ids = []
    bug_severities = []
    bug_desc = []
    for bug_row in bugs.itertuples():
        bug_texts.append(build_bug_embedding_text(title=getattr(bug_row, "summary"), description=getattr(bug_row, "description")))
        bug_ids.append(getattr(bug_row, "id"))
        bug_severities.append(getattr(bug_row, "severity_norm"))
        bug_desc.append(getattr(bug_row, "description"))


    # Embed concepts
    concept_embeddings = embed_concepts(concept_texts)

    # Embed bugs
    bug_embs = run_embed(bug_texts)

    # Concept scroring for bugs
    records = []
    X_rows = []
    llm_refine_count = 0
    raw_scores = []
    long_records_text = []
    long_records_score = []
    for bug_idx in range(len(bug_ids)):
        bug_id = bug_ids[bug_idx]
        bug_emb = bug_embs[bug_idx]

        # Scoring bug based on cosin between bug embedding with concepts' embeddings
        sims = concept_embeddings @ bug_emb
        raw_scores.extend(sims[sims >= SIM_THRESHOLD])


        # Filter noise
        hit_idx = np.where(sims >= SIM_THRESHOLD)[0]


        # shape / transform the remaining scores
        score_map = {
            concept_ids[i]: float(sims[i])
            for i in hit_idx
        }
        shaped_scores = shape_scores(scores=score_map, concept_roles=concept_id_roles)

        if needs_llm(shaped_scores):
            shaped_scores = llm_refine_scores(bug_text=bug_texts[bug_idx], candidate_scores=shaped_scores)
            llm_refine_count += 1

        for cid, score in score_map.items():
            score_map[cid] = shaped_scores[cid]

    
        # Records to save for later inspectation
        records.append({
            "bug_id": bug_id,
            "concept_scores": score_map
        })
        

        # Dense row
        row_vec = np.zeros(len(concept_ids), dtype=np.float32)
        for i in hit_idx:
            row_vec[i] = score_map[concept_ids[i]]

        long_records_text.append({
            "bug_id": bug_id,
            "severity": bug_severities[bug_idx],
            "text": bug_texts[bug_idx],
            "desc": bug_desc[bug_idx],
        })

        long_records_score.append({
            "bug_id": bug_id,
            "severity": bug_severities[bug_idx],
            "concept_scores": row_vec,
        })

        X_rows.append(row_vec)

    print("\nQuantile [0.5, 0.9, 0.99] of sims - old preprocessing:  [0.3879115  0.46373203 0.56438369]")
    print("\nQuantile [0.5, 0.9, 0.99] of sims - new preprocessing: ", np.quantile(raw_scores, [0.5, 0.9, 0.99]))

    print(f"\nScoring completes - llm refine needed: {llm_refine_count}")
    # =========================
    # SAVE RESULTS
    # =========================

    # Sparse (debuggable)
    df_scores = pd.DataFrame(records)
    df_scores.to_parquet(
        os.path.join(OUT_DIR, "bug_concept_scores.parquet"),
        index=False
    )

    df_wide = (
    pd.json_normalize(long_records_score, sep="__")
    .rename(columns=lambda c: c.replace("concept_scores__", "")))
    df_wide.to_csv(
        os.path.join(OUT_DIR, "bug_concept_scores_wide.csv"),
        index=False
    )

    df_long = pd.DataFrame(long_records_text)
    df_long.to_csv(
        os.path.join(OUT_DIR, "bug_concept_scores_long.csv"),
        index=False
    )
    df_long_1 = df_long.copy()
    df_long_1.drop("desc", axis=1)
    df_long_1.to_csv(
        os.path.join(OUT_DIR, "bug_nodesc.csv"),
        index=False
    )

    df_long_s1 = df_long_1[df_long_1["severity"] == "S1"]
    df_long_s1.to_csv(
        os.path.join(OUT_DIR, "bug_nodesc_s1.csv"),
        index=False
    )

    df_long_s2 = df_long_1[df_long_1["severity"] == "S2"]
    df_long_s2.to_csv(
        os.path.join(OUT_DIR, "bug_nodesc_s2.csv"),
        index=False
    )

    df_long_s3 = df_long_1[df_long_1["severity"] == "S3"]
    df_long_s3.to_csv(
        os.path.join(OUT_DIR, "bug_nodesc_s3.csv"),
        index=False
    )

    df_long_s4 = df_long_1[df_long_1["severity"] == "S4"]
    df_long_s4.to_csv(
        os.path.join(OUT_DIR, "bug_nodesc_s4.csv"),
        index=False
    )

    # Dense ML matrix
    X_concept_scores = np.vstack(X_rows)
    np.save(os.path.join(OUT_DIR, "X_concept_scores.npy"), X_concept_scores)


    # Feature index
    feature_index = {
        str(cid): idx
        for idx, cid in enumerate(concept_ids)
    }

    with open(os.path.join(OUT_DIR, "feature_index.json"), "w") as f:
        json.dump(feature_index, f, indent=2)


    print("✅ Concept scoring completed")
    print(f"Features: {len(concept_ids)}")
    print(f"Bugs scored: {len(bugs)}")
    print(f"Output dir: {OUT_DIR}")
