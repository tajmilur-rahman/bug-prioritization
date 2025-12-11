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
echo "              BUG PRIORITIZATION ANALYTICS.                 "
echo "============================================================"
echo "LABEL_KIND: $LABEL_KIND"
echo "FUSION:     $FUSION"
echo "============================================================"

PREP_LOG=""
# Extract prep dir path printed by the script, fallback to newest folder
 PREP_PATH="$(grep -oE 'artifacts/prep/[^ ]+' "$PREP_LOG" | tail -n1 || true)"
if [[ -z "$PREP_PATH" ]]; then
  echo "Could not parse PREP path from prep output; falling back to newest dir under $PREP_ROOT"
  PREP_PATH="$(ls -1dt "$PREP_ROOT"/* | head -n1)"
fi
PREP_ID="$(basename "$PREP_PATH")"
echo "==> PREP_ID=$PREP_ID"

# ===========================================
# 7. ANALYTICS (dataset + topics + fusion blocks)
# ===========================================
echo "[7] Running analytics..."
python scripts/run_analytics.py \
    --prep-root "$PREP_ROOT" \
    --prep-id "$PREP_ID" \
    --input data/bugs_enriched.parquet \
    --label-kind "$LABEL_KIND"

echo "Analytics saved to analytics/$LABEL_KIND/$PREP_ID"

