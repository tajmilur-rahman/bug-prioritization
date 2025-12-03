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

PREP_LOG=""

# ===========================================
# 5. PREP (FEATURE ASSEMBLY)
# ===========================================
echo "[5] Running prep_from_parquets.py ..."
PREP_LOG="$(mktemp)"
python libs/models/prep_from_parquets.py \
    --input data/bugs_enriched.parquet \
    --features-config configs/features.yaml \
    --topics-dir "$TOPICS_CLEAN_ROOT" \
    --prep-root "$PREP_ROOT" \
    | tee "$PREP_LOG"

PREP_PATH="$(grep -oE 'artifacts/prep/[^ ]+' "$PREP_LOG" | tail -n1 || true)"
echo "PREP_PATH detected: $PREP_PATH"


# ===========================================
# 6. MODEL TRAINING (MLP + XGB)
# ===========================================
echo "[6] Training models..."
# # Extract prep dir path printed by the script, fallback to newest folder
 PREP_PATH="$(grep -oE 'artifacts/prep/[^ ]+' "$PREP_LOG" | tail -n1 || true)"
if [[ -z "$PREP_PATH" ]]; then
  echo "Could not parse PREP path from prep output; falling back to newest dir under $PREP_ROOT"
  PREP_PATH="$(ls -1dt "$PREP_ROOT"/* | head -n1)"
fi
PREP_ID="$(basename "$PREP_PATH")"
echo "==> PREP_ID=$PREP_ID"

MODEL_BASE="$MODELS_ROOT/$LABEL_KIND/$PREP_ID"
mkdir -p "$MODEL_BASE"


# --- MLP ---
python libs/models/train_mlp.py \
    --prep-root "$PREP_ROOT" \
    --prep-id "$PREP_ID" \
    --ordinal \
    --features-config configs/features.yaml

# Move output to correct folder
MLP_RUN=$(ls -td artifacts/MLP_* | head -n1)
mv "$MLP_RUN" "$MODEL_BASE/clf_mlp_${FUSION}_${TIMESTAMP}"

echo "Saved MLP model → $MODEL_BASE/clf_mlp_${FUSION}_${TIMESTAMP}"

# # --- XGB ---
# python libs/models/train_xgb.py \
#     --prep-root "$PREP_ROOT" \
#     --prep-id "$PREP_ID" \
#     --features-config configs/features.yaml

# # Move output
# XGB_RUN=$(ls -td artifacts/XGB_* | head -n1)
# mv "$XGB_RUN" "$MODEL_BASE/clf_xgb_${FUSION}_${TIMESTAMP}"

# echo "Saved XGB model → $MODEL_BASE/clf_xgb_${FUSION}_${TIMESTAMP}"

