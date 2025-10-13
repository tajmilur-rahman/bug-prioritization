# Project Overview: Bug Report Prioritization with BERT + BERTopic

## 1 · Background and Motivation
Modern software projects (e.g., Mozilla, Eclipse, Chromium) receive **thousands of bug reports daily**.  
These reports vary in impact and urgency—from trivial UI issues to critical security flaws.  
Manually assigning **priority levels (P1–P5)** is time-consuming and subjective.

A common challenge in real-world bug datasets is **class imbalance**:  
- **P3** reports (medium priority) **dominate the dataset**,  
- while **P1 (critical)** and **P2 (high)** reports—though most important for release quality and user impact—are **relatively rare**.  

This imbalance makes it easy for models to perform well on average metrics but still **fail to catch critical bugs**, leading to delayed fixes and potential reliability risks.  
To address this, the framework explicitly incorporates **imbalance handling techniques** (e.g., weighted losses, SMOTE oversampling, and focal loss) to ensure the model gives adequate attention to rare but crucial P1 and P2 cases.

To solve this broader problem, we aim to build a **data-driven framework** that automatically predicts bug priorities using historical bug reports.  
The approach leverages:

- **BERT embeddings** for contextual understanding of bug texts (summary + description).  
- **BERTopic** for topic discovery and interpretability of issue types.  
- **Machine-learning classifiers** (e.g., XGBoost, MLP) that combine text embeddings and metadata.

This hybrid design balances **interpretability (topics)** and **predictive accuracy (classification)** while ensuring scalability, reproducibility, and **fair learning across imbalanced priority classes**.

---

## 2 · High-Level Architecture

**Core Pipeline**  

1️⃣ **Ingestion** → collect structured bug data (summary, description, metadata, labels).  
2️⃣ **Embedding** → encode text with a transformer (MiniLM/BERT).  
3️⃣ **Topic Modeling** → derive semantic clusters using BERTopic (UMAP + HDBSCAN + c-TF-IDF).  
4️⃣ **Classification** → predict priority levels using fused features.  
5️⃣ **Evaluation & Benchmarking** → report metrics, interpret results (SHAP/LIME).  
6️⃣ **Deployment** → refresh embeddings, topics, and predictions periodically.

---

# ⚙️ Detailed Architecture and Design

## 3 · Embedding Module (BERT Encoder)

### Purpose  
Convert text fields (summary + description) into dense vectors capturing semantic context.

### Design Principle  
> **Compute embeddings once; reuse for both BERTopic and classification.**

### Encoder Options and Expected Classifier Accuracy

| Encoder | Dim | Speed | Expected Macro-F1 (final classifier) | Notes |
|----------|-----|--------|---------------------------------------|--------|
| MiniLM-L6-v2 | 384 | ⭐️⭐️⭐️⭐️⭐️ | ≈ 0.74 | Compact baseline |
| all-mpnet-base-v2 | 768 | ⭐️⭐️ | 0.76–0.77 | Better semantic cohesion |
| e5-large-v2 | 1024 | ⭐️ | 0.78–0.79 | Cross-domain strength |
| distilBERT-base-nli | 768 | ⭐️⭐️⭐️ | ≈ 0.73 | Light alternative |

*(Macro-F1 refers to final classifier performance, not embedding accuracy.)*

---

## 4 · Topic Modeling (BERTopic)

### Purpose  
Identify latent semantic groups (topics) of bug reports for interpretability and as extra features for classification.

### Pipeline Steps  
1. **UMAP** → reduce embedding dimension (e.g., 384 → 20).  
2. **HDBSCAN** → cluster reports by semantic density.  
3. **c-TF-IDF** → extract keywords per topic.

### 4.1 · Topic Usage Strategies (Track A and Track B)

| Track | Description | Purpose | Topic Reuse Behavior | Focus |
|--------|--------------|----------|----------------------|-------|
| **Track A – Direct Transform** | Applies the pre-trained BERTopic model’s `.transform()` to new embeddings. | Evaluate how well a frozen topic model assigns topics to new reports. | Topics stay fixed; BERTopic internally finds closest topic. | Tests *reusability and stability* of trained model. |
| **Track B – Centroid Reassignment (Lightweight)** | Computes cosine similarity between new embeddings and stored topic centroids; assigns nearest. | Provide a **lightweight inference path** without BERTopic dependency. | Uses only centroids; no clustering or transform. | Tests *efficiency and consistency* with minimal overhead. |

Both tracks reuse the same learned topic structures—Track A via the full model, Track B via centroid look-ups.  
The difference quantifies how much classification quality is retained under the lightweight deployment mode.

---

## 5 · Classification Module

### Feature Inputs  
1. **Text Embeddings** (BERT)  
2. **Topic Features** (topic ID or centroid vector)  
3. **Metadata** (product, component, severity, etc.)  
4. **Derived Numeric Features** (e.g., description length, stacktrace flag)

### Normalization  
- Text → BERT vectors  
- Categorical → one-hot or learned 8–32 dim embeddings  
- Numeric → scaled to [0, 1]

---

## 6 · Imbalance Handling and Model Variants

Bug priorities are highly imbalanced: **P3 dominates**, while **P1 and P2 are critical**.

| ID | Feature Fusion | Algorithm | Imbalance Handling | Purpose |
|----|----------------|------------|--------------------|----------|
| clf_A1 | [BERT + meta] | XGBoost | class_weight | Baseline |
| clf_A2 | [BERT + topic + meta] | XGBoost | focal_loss | Topic-aware, hard-case focus |
| clf_B1 | [BERT + centroid + meta] | MLP | oversampling (SMOTE P1/P2) | Neural fusion, track B |
| clf_B2 | [BERT + topic + meta] | Logistic Regression | weighted CE | Linear baseline |
| clf_B3 | [BERT + topic + meta] | MLP | weighted CE | Non-linear weighted fusion |

### Techniques Explained
- **class_weight:** increase loss for rare labels.  
- **focal_loss:** down-weights easy samples to focus on hard P1/P2 cases (XGBoost custom objective).  
- **SMOTE:** Synthetic Minority Over-sampling Technique—creates synthetic minority (P1/P2) examples by interpolating neighbors; safe for moderate datasets (~50 k rows).  
- **weighted CE:** assign explicit per-class weights inside the cross-entropy loss (used in MLP and LR).

---

## 7 · How Weighted vs. Unweighted Cross-Entropy Works  

### Conceptual Diagram
```
        ┌────────────────────────────────────────────┐
        │              PRIORITY CLASSES              │
        ├──────────────────┬─────────────────────────┤
        │ P1/P2 (rare)     │ P3–P5 (frequent)        │
        ├──────────────────┼─────────────────────────┤
Unweighted │ weak gradient │ dominant gradient       │
Weighted   │ ↑ boosted w₁₂ │ ↓ reduced w₃₄₅          │
        └──────────────────┴─────────────────────────┘
   Gradient update per sample: ∇Lᵢ_weighted = w_(yᵢ) · ∇Lᵢ
```

### Mathematical Comparison
| Setting | Loss Formula | Effect |
|----------|---------------|--------|
| Unweighted | \( \mathcal{L}=-\frac1N\sum_i\sum_k y_{ik}\log p_{ik} \) | All classes equal |
| Weighted | \( \mathcal{L}=-\frac1N\sum_i\sum_k w_k y_{ik}\log p_{ik} \) | Rare classes ↑impact |

Weights: \( w_k = \frac{N}{K · n_k} \)

### Code Comparison (PyTorch)
```python
# Unweighted
criterion = nn.CrossEntropyLoss()
loss = criterion(preds, targets)

# Weighted
class_counts = torch.tensor([n1, n2, n3, n4, n5], dtype=torch.float32)
class_weights = (1.0 / class_counts) * (class_counts.sum() / len(class_counts))
criterion_w = nn.CrossEntropyLoss(weight=class_weights)
loss_w = criterion_w(preds, targets)
```
👉 Gradients for rare P1/P2 samples are amplified, driving higher recall (As an example).

| Metric | Unweighted | Weighted |
|---------|-------------|----------|
| Recall@P1 | 0.42 | **0.63** |
| Recall@P2 | 0.55 | **0.70** |
| Macro-F1 | 0.70 | **0.74** |
| Brier | 0.185 | **0.165 (better calibration)** |

---

## 8 · Evaluation Metrics and Interpretation

| Metric | Meaning | Purpose |
|---------|----------|----------|
| **Macro-F1** | Average F1 across classes | Fair to rare classes |
| **Recall@P1/P2** | % of true P1/P2 caught | Operational safety |
| **Macro-AUC** | Ranking quality | Threshold-independent check |
| **Brier Score** | Squared error of probabilities | Probability calibration |
| **PR Curves** | Precision-Recall trade-off per class | Visualization under imbalance |

**Ranking quality (AUC):** measures if critical bugs rank above others.  
**Calibration (Brier):** how well confidence matches reality.  
**PR Curves:** show precision drop as recall rises for P1/P2.  

Because Brier is imbalance-sensitive, compare only models with identical splits.

---

## 9 · Evaluation Plan and Benchmark Outputs

Each variant (A1–B3) produces:

- Full metrics (Macro-F1, Recall@P1/P2, AUC, Brier).  
- Explainability (SHAP/LIME).  
- Runtime cost (RAM, latency).  
- Track A vs B comparison for topic-feature stability.  

Results go into a comparison table and visual dashboard highlighting feature-fusion and imbalance effects.

---

## 10 · Summary for New Contributors

| Component | Role | Key Idea | Actions |
|------------|------|----------|-----------------------|
| **BERT Embedding** | Semantic foundation | Encode once for all modules | Test new encoders, optimize batching |
| **BERTopic (Tracks A/B)** | Interpretability + topic features | Compare full vs centroid assignment | Tune clustering, analyze A/B gap |
| **Classification** | Priority prediction | Fuse text + meta + topic | Add models, balance methods |
| **Imbalance Handling** | Fair learning for P1/P2 | Weighting + SMOTE | Experiment with focal or effective-num weights |
| **Benchmarking** | Evaluation & trust | Standard metrics + explainability | Add plots, threshold sweeps |
| **Explainability** | Transparency | SHAP/LIME interpretations | Integrate dashboard visuals |

---

**In summary:**  
This framework builds a reproducible, interpretable, and efficient pipeline for bug-priority prediction—anchored on shared BERT embeddings, topic-aware fusion, explicit imbalance control for P1/P2, and a standardized metric suite that future contributors can extend and compare against.
