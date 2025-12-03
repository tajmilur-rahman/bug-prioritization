#!/usr/bin/env bash
set -euo pipefail

# ===========================================
# CONFIG
# ===========================================
LABEL_KIND=${LABEL_KIND:-"severity"}         # severity | priority
FUSION=${FUSION:-"B1"}                       # A1/A2/A3/A4/B1/B2
PREP_ROOT="artifacts/prep"
MODELS_ROOT="artifacts/models"
TOPICS_ROOT="artifacts/topics"
TOPICS_CLEAN_ROOT="artifacts/topics_clean"
EMB_OUT="data/embeddings/shards"
ANALYTICS_ROOT="analytics"

TIMESTAMP=$(date +"%Y%m%d_%H%M%S")


echo "============================================================"
echo "              BUG PRIORITIZATION FULL PIPELINE              "
echo "============================================================"
echo "LABEL_KIND: $LABEL_KIND"
echo "FUSION:     $FUSION"
echo "============================================================"


# ===========================================
# 1. BUILD BUGS CLEANED DATASET
# ===========================================
echo "[1] Building cleaned bugs dataset..."
python scripts/build_bugs_dataset.py \
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

# ===========================================
# 2. EMBEDDINGS
# ===========================================
echo "[2] Embedding stage..."
rm -rf $EMB_OUT
mkdir -p $EMB_OUT

python scripts/embed_docs.py \
    --input_csv data/bugs_cleaned.csv \
    --out_dir data/embeddings \
    --shard_size 50000 \
    --mode local \
    --model_name sentence-transformers/all-MiniLM-L6-v2


# ===========================================
# 3. TOPICS (FIT + CLEANUP + ASSIGN)
# ===========================================
echo "[3] Topic modeling..."

# --- Fit topics ---
python scripts/topic_jobs.py fit \
    --train_glob "data/embeddings/shards/*.parquet" \
    --out_dir "$TOPICS_ROOT"

# --- Cleanup topics ---
python scripts/topics_cleanup.py \
    --topics-dir "$TOPICS_ROOT" \
    --out-dir "$TOPICS_CLEAN_ROOT" \
    --min-size 10 --cos-thr 0.92 --use-jaccard --jaccard-thr 0.25

# --- Assign topics (clean version) ---
python scripts/topic_jobs.py assign \
    --topics_clean_dir "$TOPICS_CLEAN_ROOT" \
    --topics_dir "$TOPICS_ROOT" \
    --shards_glob "data/embeddings/shards/*.parquet" \
    --topics_shards_dir "data/topics"


# ===========================================
# 4. MERGE + ENRICH (ID JOIN)
# ===========================================
echo "[4] Merge enrichment..."
python scripts/merge_enrichment.py \
    --bugs-cleaned data/bugs_cleaned.csv \
    --emb-glob "data/embeddings/shards/*.parquet" \
    --topics-glob "data/topics/shards/*.topics.parquet" \
    --out-file data/bugs_enriched.parquet

