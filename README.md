# Bug Prioritization

## Environment Setup
***Recheck and rerun these if any dependencies or environment variables added or changed (changed in configs/base-requirements.txt file or .env file changed).***
From the repository root directory:

1. Install dependencies
```shell
# Python deps common to all services
pip install --no-cache-dir -r configs/base-requirements.txt

# CPU torch (small, no CUDA)
pip install --no-cache-dir torch --index-url https://download.pytorch.org/whl/cpu
```

2. Run **script/load_env.py** then all keys from **.env** file become available for the later running scripts via os.environ
```shell
python scripts/load_env.py
```
Inside later running scripts (example):
```python
import os
ART = os.getenv("ARTIFACTS_DIR", "/app/artifacts")
```

## Data Retrieval 
1. From root of project repository, run scripts/getdataset.py. (May need to install deferent dependecies for **bugbug**. The environment setup step above focuses on dependencies for all other steps.)
This will retrieve the dataset of Bugbug and store as data/bugs.json. However this file is large (~ 8GB) and contains many unused data, we filter out the data which are useful for bug prioritization in the next step.
2. Get the right objects "bugs" from **bugs.json** file and remove unused but large fields like **"history", "attachements"**, so that the data file size is reduced to ~ 6GB. The output file is **bugs.ndjson** which is more easy to process than .json file so the preprocessing steps can be done quickly on local machine.  
From root of project repository, run below shell command in terminal. Output is **bugs.ndjson** file at data folder. (There's one copy of **bugs.ndjson** at https://drive.google.com/file/d/1983RBoiLobw2ITjvED6p8tmE_jh7fNB0/view?usp=share_link)

```shell
jq -c '
  ( if type == "array"                          then .[]
    elif type == "object" and has("bugs")       then .bugs[]
    elif type == "object" and has("result") and (.result|has("bugs")) then .result.bugs[]
    else . end )
  | select(type == "object")
  | del(.history?, .attachments?)
' data/bugs.json > data/bugs.ndjson
```

## Data Preprocessing
1. "process_bugs.py" cleans the data and stores the cleaned data in multiple .csv files inside "clean" folder.

How to run
- Put your big dump at data/bugs.json (array) or data/bugs.ndjson (one JSON object per line).
- Run to produce both raw & clean CSVs in 10k chunks:
```shell
python scripts/process_bugs.py \
  --input data/bugs.ndjson \
  --format ndjson \
  --outdir data \
  --chunk 10000 \
  --write-raw yes \
  --write-clean yes
```
This will create:
```shell
data/
├─ raw/
│  ├─ bugs_raw_000.csv
│  ├─ bugs_raw_001.csv
│  └─ ... (every 10,000 rows)
├─ clean/
│  ├─ bugs_clean_000.csv
│  ├─ bugs_clean_001.csv
│  └─ ...
└─ manifest.json
```

Data fields after cleaning to be used for models training

**Core** text
- summary — Bug summary text
- description — Bug description (from comments[0].text if present)

**Metadata**
- priority, severity, status, resolution, is_open, type, product, component, version, platform, op_sys, classification
- People/fields: creator, assigned_to, qa_contact
- Dates: creation_time, last_change_time, cf_last_resolved
- Other: target_milestone, keywords (joined by ;), url, whiteboard, alias, dupe_of

**Counts**
- Lists: duplicates_count, depends_on_count, blocks_count, cc_count, attachments_count, regressions_count, regressed_by_count, see_also_count
- comment_count, votes

**Text features**
- summary_len, desc_len, url_count, code_fence_count, has_stacktrace (heuristics on summary+description)

**Custom fields** (scalar subset)
- cf_crash_signature, cf_rank, cf_webcompat_priority, cf_fx_points, cf_performance_impact, cf_user_story, cf_has_str, cf_qa_whiteboard, cf_fx_iteration, cf_cab_review, cf_accessibility_severity, cf_a11y_review_project_flag

**Derived**
- days_open_est (from creation_time → last_change_time if both parse)


2. "merge_resolved.py" takes only the **"RESOLVED"** bugs (filterd by **status** field) from all the cleaned .csv files and merge to one file for using in later steps. There were 201539 resolved bugs merged in to data/bugs_resolved.csv file. The same named file on reporsitory is a little sample of that.

Detail result after merging resolved:
```text
Classes:  ['P1', 'P2', 'P3', 'P4', 'P5']
Dropped 187203 / 750750 rows (24.9%) for UNRESOLVED.
Dropped 362008 / 750750 rows (48.2%) with invalid/missing data or labels.
Merged 201539 bugs with valid labels -> data/bugs_resolved.csv
```

3. Split data for training and evaluation
Below command splits data from data/bugs_resolved.csv and writes data/processed/train.csv, val.csv, test.csv (sorted by creation_time).
```shell
python scripts/make_time_splits.py
```
*With current dataset, splits train=450839 val=56354 test=56354 -> data/processed*


## Compute Embeddings
Turn each bug’s text into a fixed-dimensional vector once, cache it, and reuse it for:
- **Topics** (BERTopic discovery/assign & centroids), and
- **Priority classifier** (XGBoost/MLP), fused with metadata + numeric features.
We do this with a small sentence-transformer (default: all-MiniLM-L6-v2) and a stable encoder_id so we only re-embed when the encoder actually changes.

### Inputs
**Cleaned bug rows (CSV):** 
  - data/processed/train.csv, val.csv, test.csv
  - Required text fields: summary, description 
  - Optional: id, metadata (e.g., product, component, …), numeric features (summary_len, desc_len, …).
**Encoder config (defaults from .env / configs/base.yaml):**
  - EMB_MODEL (e.g., sentence-transformers/all-MiniLM-L6-v2)
  - EMB_MAX_LEN (128–192 words)
  - normalization: normalize=True
**Hardware mode:**
  - DEVICE=gpu|cpu|auto (GPU preferred, CPU fallback)

### Outputs (saved to data/processed/{train|val}_emb_shards/*.parquet):
- Parquet shard files (separate output to many files as many more fields added and bugs file is larger) with the fields needed for training & topic jobs.
- Cache: SQLite key-value on sha1(text) → vector, helps avoid re-embedding

Output data row schema (in parquet shard files):
- id (str|int) – keep it for joins/eval
- summary (str), description (str, optional)
- embedding (list[float]) – 384 dims (MiniLM) by default
- Categorical passthrough (can be changed in command argument): product, component, platform, op_sys, priority (if labeled)
- Numeric flags (can be changed in command argument): summary_len, desc_len, url_count, code_fence_count, has_stacktrace (0/1),
dup_count, depends_on_count, blocks_count

### How to use
```shell
python scripts/embed_docs.py \
      --input_csv "data/processed/train.csv" \
      --mode local \
      --device cpu \
      --text_cols summary description \
      --pass_cols product component platform op_sys priority \
      --numeric_from votes comment_count \
      --shard_name train_emb_shards \
      --shard_size 50000 \
      --cache_db artifacts/emb_cache/embeddings.sqlite

python scripts/embed_docs.py \
      --input_csv "data/processed/val.csv" \
      --mode local \
      --device cpu \
      --text_cols summary description \
      --pass_cols product component platform op_sys priority \
      --numeric_from votes comment_count \
      --shard_name val_emb_shards \
      --shard_size 20000 \
      --cache_db artifacts/emb_cache/embeddings.sqlite
```
### Why Parquet shards + SQLite cache?
- Parquet shards (20–50k rows each) are efficient to scan and column-select (fast for training & topic jobs).
- SQLite cache is light and can store millions of records of bugs' embbedings while suitable for 0/1 lookups on sha1(text).
 
### Maintenance notes
- Encoder changes (e.g., switch from MiniLM → DistilBERT): bump encoder_id in topic/manifest; use a new cache DB path (e.g., artifacts/emb_cache/embeddings_v2.sqlite) to avoid mixing vectors of different dimension/normalization.
- Batch sizing: start with 256 (local CPU) and adjust to saturate GPU


## Topics 
BERTopic jobs after embedding: **scripts/topic_jobs.py** can do the following
- Fit UMAP/HDBSCAN on a subset (config the max sample size in **configs/topics.yaml**),
- Assign topics to all rows og bug reports,
- Save artifacts/topics/bertopic_model, topic_centroids.npy, manifest.json, topic_info.csv, and topic_top_words.
- Write topic_id (A/B) back to CSVs parquet shards.

### Input:
```shell
configs/
├─ topics.yaml
data/processed
├─ train_emb_shards/
│  ├─ part-00000000-00050000.parquet 
│  ├─ part-00000000-00100000.parquet
│  └─ ... (every 50,000 () rows)
├─ val_emb_shards/
│  ├─ part-00000000-00020000.parquet
│  ├─ part-00000000-00040000.parquet
│  └─ ... (every 20,000 () rows)
```

### Output:
```shell
artifacts/topics/
├─ bertopic_model.pkl
├─ topic_centroids.npy
├─ manifest.json
├─ topic_info.csv      # Topic,Count,Name,Representation,Representative_Docs
├─ topic_top_words.csv # keywords of topics
data/processed
├─ train_emb_shards/
│  ├─ part-00000000-00050000.withtopics.parquet # topic_id_A (for classification with bertopic model available), 
│  ├                                            # topic_id_B (for classification with topic centroids only)
│  ├─ part-00000000-00100000.withtopics.parquet
│  └─ ... (every 50,000 () rows)
├─ val_emb_shards/
│  ├─ part-00000000-00020000.withtopics.parquet
│  ├─ part-00000000-00040000.withtopics.parquet
│  └─ ... (every 20,000 () rows)
```

### How to run
Run below shell script (save the script into an .sh file an run in a terminal/CLI)
```shell
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
```

