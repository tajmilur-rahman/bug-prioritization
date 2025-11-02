
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

# Trainer knobs (fallback to .env inside trainers)
XGB_THREADS="${XGB_THREADS:-16}"
MLP_EPOCHS="${MLP_EPOCHS:-140}"
MLP_BS="${MLP_BS:-512}"

# Imbalance auto knobs (read by trainers via dotenv)
#export IMBALANCE="${IMBALANCE:-auto}"
export FOCAL_GAMMA="${FOCAL_GAMMA:-1.5}"
export CLASS_WEIGHT_CLIP_MIN="${CLASS_WEIGHT_CLIP_MIN:-1.0}"
export CLASS_WEIGHT_CLIP_MAX="${CLASS_WEIGHT_CLIP_MAX:-8.0}"
export AUTO_IMBAL_IR_MILD="${AUTO_IMBAL_IR_MILD:-3}"
export AUTO_IMBAL_IR_MODERATE="${AUTO_IMBAL_IR_MODERATE:-8}"
export MINORITY_MIN_SAMPLES="${MINORITY_MIN_SAMPLES:-500}"

# Extract prep dir path via dotenv, fallback to newest folder
#PREP_ID=pca206_v0.95_sc77e03067_tn396   # (the best-run PREP)
PREP_PATH="${PREP_ID:-}"
if [[ -z "$PREP_PATH" ]]; then
  echo "Could not get PREP path from dotenv; falling back to newest dir under $PREP_CACHE_ROOT"
  PREP_PATH="$(ls -1dt "$PREP_CACHE_ROOT"/* | head -n1)"
fi
PREP_ID="$(basename "$PREP_PATH")"
echo "==> PREP_ID=$PREP_ID"                                   


# --------- 4) TRAIN (XGBoost) ---------
# echo "==> [4/5] Train XGBoost baseline (fusion A1)"
# python libs/models/train_xgb.py --fusion A1 --prep-id "$PREP_ID" --threads "$XGB_THREADS"

# --------- 5) TRAIN (MLP + temp scaling) ---------
echo "==> [5/5] Train MLP baseline (fusion B1)"
python libs/models/train_mlp.py --fusion B1 --prep-id "$PREP_ID" --epochs "$MLP_EPOCHS" --batch-size "$MLP_BS" 
    # --early-stop 24 \
    # --early-stop-min-delta 0.0003 \
    # --lr 0.0006 \
    # --dropout 0.45 \
    # --weight-decay 0.0007


# # Optional: inference on a CSV with no topics/embeddings
# if [[ -n "$TEST_CSV" ]]; then
#   echo "==> [extra] Predict on CSV (XGB)"
#   python libs/models/train_xgb.py --fusion A1 --prep-id "$PREP_ID" --test-csv "$TEST_CSV"
# #   echo "==> [extra] Predict on CSV (MLP)"
#   python libs/models/train_mlp.py --fusion B1 --prep-id "$PREP_ID" --test-csv "$TEST_CSV"
# fi

# echo "All done. PREP artifacts: $PREP_PATH"
echo "All done. "
