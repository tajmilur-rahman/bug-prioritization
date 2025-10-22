
set -euo pipefail

# --------- CONFIG (override via env) ---------
TOPICS_DIR="${TOPICS_DIR:-artifacts/topics}"
TOPICS_CLEAN_DIR="${TOPICS_CLEAN_DIR:-artifacts/topics_clean}"

TRAIN_GLOB="${TRAIN_GLOB:-data/processed/train_emb_shards/*.withtopics.parquet}"
VAL_GLOB="${VAL_GLOB:-data/processed/val_emb_shards/*.withtopics.parquet}"
TEST_GLOB="${TEST_GLOB:-}"         # optional; if empty, test split is skipped
TEST_CSV="${TEST_CSV:-data/processed/test.csv}"           # optional; used only by trainers for CSV-only inference

FEATURES_YAML="${FEATURES_YAML:-configs/features.yaml}"

# Cleanup knobs
MIN_SIZE="${MIN_SIZE:-10}"
COS_THR="${COS_THR:-0.92}"
USE_JACCARD="${USE_JACCARD:-1}"    # 1=true 0=false
JAC_THR="${JAC_THR:-0.25}"
CLEAN_DRY_RUN="${CLEAN_DRY_RUN:-0}" # 1=preview only, do not persist mapping/centroids
REPORT_HTML="${REPORT_HTML:-$TOPICS_CLEAN_DIR/preview.html}"

# Prep cache root
PREP_CACHE_ROOT="${PREP_CACHE_ROOT:-artifacts/prep}"


# --------- 3) PREP FROM PARQUETS ---------
echo "==> [3/5] Build cached matrices (PCA+scaler) from cleaned parquets"
PREP_LOG="$(mktemp)"
python libs/models/prep_from_parquets.py \
  --features-config "$FEATURES_YAML" \
  --train_parquet_glob "data/train_clean/*_clean.parquet" \
  --val_parquet_glob   "data/val_clean/*_clean.parquet" \
  --topics-clean-dir "$TOPICS_CLEAN_DIR" | tee "$PREP_LOG"

# Extract prep dir path printed by the script, fallback to newest folder
PREP_PATH="$(grep -oE 'artifacts/prep/[^ ]+' "$PREP_LOG" | tail -n1 || true)"
if [[ -z "$PREP_PATH" ]]; then
  echo "Could not parse PREP path from prep output; falling back to newest dir under $PREP_CACHE_ROOT"
  PREP_PATH="$(ls -1dt "$PREP_CACHE_ROOT"/* | head -n1)"
fi
PREP_ID="$(basename "$PREP_PATH")"
echo "==> PREP_ID=$PREP_ID"

# Export so trainers that read .env can pick it up if needed
export PREP_CACHE_ROOT
export PREP_ID

echo "All done. PREP artifacts: $PREP_PATH"
