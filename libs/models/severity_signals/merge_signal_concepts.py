

from sentence_transformers import SentenceTransformer
import numpy as np
from pathlib import Path
from sklearn.cluster import AgglomerativeClustering
from openai import OpenAI
import hashlib
import json

import sys
ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

# ============================================================
# LLM CALL FUNCTION
# ============================================================
client = OpenAI()

WORKING_DIR = Path("artifacts")

NORMALIZED_SIGNAL_FILE = WORKING_DIR / "severity_signals" / "nomalized_signals.jsonl"
CONCEPT_FILE = WORKING_DIR / "severity_signals" / "concepts.jsonl"

CACHE_DIR = WORKING_DIR / "llm_cache" / "judge_merged_concepts"
CACHE_DIR.mkdir(parents=True, exist_ok=True)



# =========================================================
# Embedding-based candidate clustering
# =========================================================

def build_signal_merge_text(signal):
    """
    signal = {
        "label": str,
        "explanations": List[str]
    }
    """

    examples = " ".join(signal["explanations"][:3])

    return f"""
    Signal label:
    {signal['label'].replace('_', ' ')}

    Meaning examples:
    {examples}
    """.strip()

def merge_signals(canonical_signals):
    """
    :param canonical_signals: list of canonical signals
    Return a 1-D array of cluster labels, one label per input canonical signals, indicating which cluster each item belongs to.
    """
    model = SentenceTransformer("all-MiniLM-L6-v2")

    texts = [build_signal_merge_text(s) for s in canonical_signals]
    embeddings = model.encode(texts, normalize_embeddings=True)

    # Conservative clustering (high recall)
    clusterer = AgglomerativeClustering(
        n_clusters=None,
        distance_threshold=0.25,  # tuneable
        linkage="average",
        metric="cosine"
    )

    labels = clusterer.fit_predict(embeddings)

    return labels


# =========================================================
# Recheck the merged concepts: LLM based
# =========================================================

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

    try:
        response = client.responses.create(
            model="gpt-4o-mini",
            input=prompt,
            temperature=0,
            max_output_tokens=5,
        )

        text = response.output_text.strip().upper()

        if text not in {"YES", "NO"}:
            # Safety: default to NO
            text = "NO"

    except Exception as e:
        # Safety: never merge on failure
        print(f"[LLM ERROR] {e}")
        text = "NO"

    # write cache
    cache_file.write_text(json.dumps({
        "prompt_hash": h,
        "model": "gpt-4o-mini",
        "prompt": prompt,
        "response": text,
    }, indent=2))

    return text

def llm_merge_judge(signals):
    """
    signals: List[CanonicalSignal]
    Returns: True / False
    """
    prompt = f"""
You are deciding whether multiple bug signal descriptions represent
the SAME underlying technical concept.

Definition:
- SAME concept = same root cause or technical issue
- DIFFERENT = different causes, even if related

Signals:
{chr(10).join('- ' + s['label'] + ': ' + s['explanations'][0] for s in signals)}

Answer with ONLY one word:
YES or NO
"""
    return call_llm(prompt).strip() == "YES"


# ============================================================
# MAIN
# ============================================================

if __name__ == "__main__":
    concepts = []
    concept_id = 0

    canonical_signals = []
    labels = []
    clusters = []

    # Load canonical signals from file
    canons_to_update_back_normalized_file_with_id = [] 
    with open(NORMALIZED_SIGNAL_FILE, "r", encoding="utf-8") as f:
        canon_id = 0
        for line in f:
            canon = json.loads(line)

            canonical_signals.append({
                "id": canon_id,
                "label": canon["label"],
                "explanations": [o["explanation"] for o in canon["occurrences"]]
            })

            # ** update back normalized signal file with id field **
            cannon_to_update_back = {"id": canon_id}
            cannon_to_update_back.update(canon)
            canons_to_update_back_normalized_file_with_id.append(cannon_to_update_back)

            canon_id += 1

    # Cluster canonical signals to candidates
    labels = merge_signals(canonical_signals)

    # Getting cluster candidates
    for cluster_id in set(labels):
        members = [
            canonical_signals[i]
            for i, lbl in enumerate(labels)
            if lbl == cluster_id    
        ]

        print(f"\ncandidate cluster {cluster_id}",
               f" - name: {members[0]["label"]}",
                f" - members: {len(members)}")
        

        if len(members) == 1:
            concepts.append({
                "concept_id": concept_id,
                "name": members[0]["label"],
                "member_signal_ids": [members[0]["id"]],
                "explanation_evidence": members[0]["explanations"],
            })
            concept_id += 1
            continue

        #if llm_merge_judge(members):   # judge with LLM
        keep = True                     # not judge with LLM, keep all and review by human, then merge more by rule based 
        if keep:
            concepts.append({
                "concept_id": concept_id,
                "name": members[0]["label"],  # temp, can rename later
                "member_signal_ids": [m["id"] for m in members],
                "explanation_evidence": sum(
                    (m["explanations"] for m in members), []
                ),
            })
            concept_id += 1
        else:
            # split cluster
            for m in members:
                concepts.append({
                    "concept_id": concept_id,
                    "name": m["label"],
                    "member_signal_ids": [m["id"]],
                    "explanation_evidence": m["explanations"],
                })
                concept_id += 1
       

    # Save concepts
    fout = open(CONCEPT_FILE, "w", encoding="utf-8")
    for concept in concepts:
        fout.write(json.dumps(concept) + "\n")

    # Save back normalized signal file with id field
    fout = open(NORMALIZED_SIGNAL_FILE, "w", encoding="utf-8")
    for canon in canons_to_update_back_normalized_file_with_id:
        fout.write(json.dumps(canon) + "\n")

