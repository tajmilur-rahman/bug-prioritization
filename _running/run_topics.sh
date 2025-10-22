TOPICS_CFG=configs/topics.yaml
TRAIN_EMB_GLOB=data/processed/train_emb_shards/*.parquet
VAL_EMB_GLOB=data/processed/val_emb_shards/*.parquet
TOPICS_DIR=artifacts/topics

export OMP_NUM_THREADS=1 OPENBLAS_NUM_THREADS=1 MKL_NUM_THREADS=1 MKL_THREADING_LAYER=GNU NUMBA_THREADING_LAYER=workqueue

# Fit Topic model with training data
python scripts/topic_jobs.py fit \
    --config "$TOPICS_CFG" \
    --train_glob "$TRAIN_EMB_GLOB" \
    --out_dir "$TOPICS_DIR"

# Assign Topics for training data
python scripts/topic_jobs.py assign \
    --config "$TOPICS_CFG" \
    --topics_dir "$TOPICS_DIR" \
    --shards_glob "$TRAIN_EMB_GLOB" \
    --mode both

# Assign Topics for evaluation data
python scripts/topic_jobs.py assign \
    --config "$TOPICS_CFG" \
    --topics_dir "$TOPICS_DIR" \
    --shards_glob "$VAL_EMB_GLOB" \
    --mode both
