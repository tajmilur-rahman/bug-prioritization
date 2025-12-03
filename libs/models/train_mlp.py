import os, sys
import time
import json
import math
import argparse
from pathlib import Path

import numpy as np
import pandas as pd
import torch
from torch import nn

ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

# ---------------- Repo libs ----------------
from libs.models.io_config import (
    load_yaml,
    load_prep_artifacts,
    read_arrow_matrix,
)
from libs.utils.reporting import (
    make_run_dir,
    evaluate_and_save,
    save_input_data_information,
    save_label_map,
)
from libs.utils.imbalance import (
    choose_imbalance_strategy,
)

from libs.utils.importance import (
    get_block_schema,
    compute_all_importance,
)

from libs.utils.losses import WeightedCrossEntropy, FocalLoss, LossFactory
from libs.utils.calibration import fit_temperature, apply_temperature
from libs.utils.metrics import brier_multiclass, ece

# architecture
from libs.models.mlp_arch import (
    build_mlp,
    BlockDropout,
)

# ---------------- Ordinal helpers ----------------
import torch.nn.functional as F


def ordinal_targets(y_long: torch.Tensor, K: int):
    """
    CORAL: For class y, produce (K-1) binary thresholds.
    """
    y = y_long.view(-1, 1)  # (N,1)
    c = torch.arange(K - 1, device=y.device).view(1, -1)  # (1,K-1)
    return (y > c).float()


def ordinal_logits_to_probs(logits_ord: torch.Tensor):
    """
    Convert K-1 ordinal logits → K class probabilities.
    """
    s = torch.sigmoid(logits_ord)  # p(y>c)
    N, Km1 = s.shape
    left = torch.cat([torch.ones(N, 1, device=s.device), s], dim=1)
    right = torch.cat([s, torch.zeros(N, 1, device=s.device)], dim=1)
    probs = (left - right).clamp_min(1e-8)
    probs = probs / probs.sum(dim=1, keepdim=True)
    return probs


# ===============================================================
# PART 1 — Config & Arguments
# ===============================================================
def resolve_cfg(args, yaml_cfg):
    """Merge precedence: CLI > ENV > YAML"""
    def yamlget(key, default=None):
        cur = yaml_cfg
        for p in key.split("."):
            if not isinstance(cur, dict) or p not in cur:
                return default
            cur = cur[p]
        return cur

    # YAML defaults
    y_epochs = yamlget("algorithms.mlp.params.epochs", 120)
    y_bs = yamlget("algorithms.mlp.params.batch_size", 512)
    y_lr = yamlget("algorithms.mlp.params.lr", 5e-4)
    y_wd = yamlget("algorithms.mlp.params.weight_decay", 1e-3)
    y_pat = yamlget("algorithms.mlp.params.early_stop_patience", 40)
    y_min_delta = yamlget("algorithms.mlp.params.early_stop_min_delta", 0.0001)
    y_dropout = yamlget("algorithms.mlp.params.dropout", 0.45)
    y_hidden = yamlget("algorithms.mlp.params.hidden_sizes", [512, 256])
    y_act = yamlget("algorithms.mlp.params.activation", "gelu")
    y_norm = yamlget("algorithms.mlp.params.norm", "layernorm_per_block")
    y_scheduler = yamlget("algorithms.mlp.params.scheduler", "onecycle")
    y_grad_clip = yamlget("algorithms.mlp.params.grad_clip", 1.0)
    y_dp = yamlget("algorithms.mlp.params.droppath", 0.05)
    y_dp_sched = yamlget("algorithms.mlp.params.droppath_schedule", "linear")

    # ENV → fallback
    def env_int(k, d): return int(os.getenv(k, d))
    def env_float(k, d): return float(os.getenv(k, d))
    def env_str(k, d):
        v = os.getenv(k)
        return v if v not in [None, ""] else d

    cfg = {
        "epochs": args.epochs or env_int("EPOCHS", y_epochs),
        "batch_size": args.batch_size or env_int("BATCH_SIZE", y_bs),
        "lr": args.lr or env_float("LR", y_lr),
        "weight_decay": args.weight_decay or env_float("WEIGHT_DECAY", y_wd),
        "early_stop": args.early_stop or env_int("EARLY_STOP", y_pat),
        "early_stop_min_delta": args.early_stop_min_delta or env_float("EARLY_STOP_MIN_DELTA", y_min_delta),
        "scheduler": args.scheduler or env_str("SCHEDULER", y_scheduler),
        "grad_clip": args.grad_clip or env_float("GRAD_CLIP", y_grad_clip),
        "hidden": [int(x) for x in (args.mlp_hidden or env_str("MLP_HIDDEN", ",".join(str(x) for x in y_hidden))).split(",")],
        "dropout": args.mlp_dropout or env_float("MLP_DROPOUT", y_dropout),
        "act": args.mlp_act or env_str("MLP_ACT", y_act),
        "norm": args.mlp_norm or env_str("MLP_NORM", y_norm),
        "droppath": args.mlp_droppath or env_float("MLP_DROPPATH", y_dp),
        "droppath_schedule": args.mlp_droppath_schedule or env_str("MLP_DROPPATH_SCHEDULE", y_dp_sched),
        "amp": (args.amp if args.amp is not None else os.getenv("AMP", "true").lower() in ["1","true","yes"]),
    }
    print(cfg)
    return cfg


# ===============================================================
# PART 2 — Main
# ===============================================================
def main():

    # ---------------------------------------------------------
    # CLI arguments
    # ---------------------------------------------------------
    ap = argparse.ArgumentParser()
    ap.add_argument("--prep-root", default="artifacts/prep")
    ap.add_argument("--prep-id", required=True)
    ap.add_argument("--features-config", default="configs/features.yaml")

    # training knobs
    ap.add_argument("--epochs", type=int)
    ap.add_argument("--batch-size", type=int)
    ap.add_argument("--lr", type=float)
    ap.add_argument("--weight-decay", type=float)
    ap.add_argument("--early-stop", type=int)
    ap.add_argument("--early-stop-min-delta", type=float)
    ap.add_argument("--grad-clip", type=float),
    ap.add_argument("--scheduler", choices=["none", "onecycle"])
    ap.add_argument("--amp", type=lambda s: s.lower() in ["1","true","yes"], default=None)

    # architecture
    ap.add_argument("--mlp-hidden")
    ap.add_argument("--mlp-dropout", type=float)
    ap.add_argument("--mlp-act")
    ap.add_argument("--mlp-norm")
    ap.add_argument("--mlp-droppath", type=float)
    ap.add_argument("--mlp-droppath-schedule")

    # ordinal vs softmax
    ap.add_argument("--ordinal", action="store_true", default=os.getenv("ORDINAL","true").lower()=="true")
    args = ap.parse_args()

    # ---------------------------------------------------------
    # Load YAML, PREP
    # ---------------------------------------------------------
    yaml_cfg = load_yaml(args.features_config)
    art = load_prep_artifacts(args.prep_root, args.prep_id)

    X_train = read_arrow_matrix(art["X_train"])
    X_val   = read_arrow_matrix(art["X_val"])
    ytr = np.load(art["y_train"], allow_pickle=True)
    yva = np.load(art["y_val"], allow_pickle=True)

    # ---------------------------------------------------------
    # Build run directory + label stats
    # ---------------------------------------------------------
    run_dir = make_run_dir("MLP", artifacts_root=os.getenv("ARTIFACTS_DIR", "artifacts"))
    save_input_data_information(run_dir, ytr, yva, args.prep_id)

    # ---------------------------------------------------------
    # Index mapping
    # ---------------------------------------------------------
    classes = sorted(pd.unique(pd.concat([pd.Series(ytr), pd.Series(yva)], ignore_index=True)))
    cls_to_idx = {c: i for i, c in enumerate(classes)}

    y_train = torch.tensor([cls_to_idx[c] for c in ytr], dtype=torch.long)
    y_val   = torch.tensor([cls_to_idx[c] for c in yva], dtype=torch.long)

    K = len(classes)
    label_names = [str(c) for c in classes]
    save_label_map(run_dir, labels=np.arange(K), label_names=label_names)

    # ---------------------------------------------------------
    # Convert input matrices to tensors
    # ---------------------------------------------------------
    X_train = torch.tensor(X_train, dtype=torch.float32)
    X_val   = torch.tensor(X_val, dtype=torch.float32)

    in_dim = X_train.shape[1]
    out_dim = K - 1 if args.ordinal else K

    # ---------------------------------------------------------
    # Build block dropout spans from schema
    # ---------------------------------------------------------
    spans = []
    schema = art["schema"]
    block_schema = get_block_schema(schema)
    print(block_schema)

    env_dropout = {
        "text_pca": float(os.getenv("BLOCK_DROPOUT_TEXT", "0")),
        "topics_emb": float(os.getenv("BLOCK_DROPOUT_TOPICS", "0")),
        "categorical": float(os.getenv("BLOCK_DROPOUT_CATEGORICAL", "0")),
        "numeric": float(os.getenv("BLOCK_DROPOUT_NUMERIC", "0")),
        "actors_hash": float(os.getenv("BLOCK_DROPOUT_ACTORS", "0")),
    }

    for blk in block_schema:
        p = env_dropout.get(blk["name"], 0.0)
        spans.append((blk["start"], blk["end"], p))

    # ---------------------------------------------------------
    # Load training configuration
    # ---------------------------------------------------------
    cfg = resolve_cfg(args, yaml_cfg)

    # ---------------------------------------------------------
    # Model
    # ---------------------------------------------------------
    model = nn.Sequential(
        BlockDropout(spans),
        build_mlp(
            in_dim=in_dim,
            out_dim=out_dim,
            hidden_list=cfg["hidden"],
            act_name=cfg["act"],
            dropout=cfg["dropout"],
            norm=cfg["norm"],
            droppath_rate=cfg["droppath"],
            droppath_schedule=cfg["droppath_schedule"],
        )
    )

    device = torch.device("cuda" if torch.cuda.is_available() else "cpu")
    model.to(device)

    # ===============================================================
    # PART 3 — Loss (Softmax or Ordinal)
    # ===============================================================
    # imbalance

    imb = choose_imbalance_strategy(y_train.tolist(), algo="mlp")

    if args.ordinal:
        print("\nWith ordinal head.")
        # Force ordinal loss — ignore imbalance strategies
        loss_fn = LossFactory.make(
            loss_name="coral",
            num_classes=K
        )
    else:
        # Use imbalance strategy's softmax loss
        loss_fn = LossFactory.make(
            loss_name=imb["loss_name"],
            num_classes=K,
            class_counts=imb["loss_params"].get("class_counts"),
            class_weights=imb["loss_params"].get("class_weights"),
            focal_gamma=imb["loss_params"].get("gamma", 1.5),
            cb_beta=imb["loss_params"].get("beta", 0.999)
        )


    # ===============================================================
    # PART 4 — Optimizer & Scheduler
    # ===============================================================
    opt = torch.optim.AdamW(model.parameters(), lr=cfg["lr"], weight_decay=cfg["weight_decay"])
    print(cfg)
    
    if cfg["scheduler"] == "onecycle":
        # steps_per_epoch = math.ceil(len(X_train) / cfg["batch_size"])
        # total_steps = cfg["epochs"] * steps_per_epoch
        sched = torch.optim.lr_scheduler.OneCycleLR(
            opt, max_lr=cfg["lr"], total_steps=cfg["epochs"],
        )
    else:
        sched = None

    scaler = torch.cuda.amp.GradScaler(enabled=(cfg["amp"] and device.type=="cuda"))


    # ===============================================================
    # PART 4.5 — Balanced / Hybrid Samplers
        # Mix of
        #     50–70% strictly balanced batches
        #     30–50% inverse-frequency sampling
        # This gives zero collapse on minority classes and stable gradients.
    # ===============================================================
    def batches(X, y, bs):
        for i in range(0, len(X), bs):
            yield X[i:i+bs], y[i:i+bs]

    def make_inverse_sampling_probs(y):
        cls, cnt = np.unique(y, return_counts=True)
        inv = {int(c): 1.0/float(n) for c, n in zip(cls, cnt)}
        p = np.array([inv[int(c)] for c in y], dtype=float)
        p /= p.sum()
        return p

    def balanced_batch_iter(X, y, bs):
        y_np = y
        classes = np.unique(y_np)
        per_cls = max(1, bs // len(classes))
        pools = {c: np.where(y_np == c)[0] for c in classes}

        while True:
            idxs = []
            for c in classes:
                if len(pools[c]):
                    sel = np.random.choice(pools[c], size=per_cls, replace=True)
                    idxs.extend(sel.tolist())
            if not idxs:
                break
            np.random.shuffle(idxs)
            yield X[idxs], y[idxs]

    def hybrid_batch_iter(X, y, bs, ratio=0.6):
        inv_p = make_inverse_sampling_probs(y)
        N = len(X)
        balanced_bs = int(bs * ratio)
        inv_bs = bs - balanced_bs
        y_np = y
        classes = np.unique(y_np)
        pools = {c: np.where(y_np == c)[0] for c in classes}

        while True:
            # balanced part
            idx_bal = []
            per_cls = max(1, balanced_bs // len(classes))
            for c in classes:
                if len(pools[c]):
                    sel = np.random.choice(pools[c], size=per_cls, replace=True)
                    idx_bal.extend(sel.tolist())

            # inverse-frequency part
            idx_inv = np.random.choice(N, size=inv_bs, replace=True, p=inv_p)

            idx = np.array(idx_bal + idx_inv.tolist())
            if len(idx) == 0:
                break

            yield X[idx], y[idx]

    # choose batch iterator
    if imb.get("smote", False):
        # SMOTE already balanced → no sampler needed
        batch_iter = lambda: batches(X_train, y_train, cfg["batch_size"])
    else:
        sampler = os.getenv("MLP_SAMPLER", "hybrid").lower()
        print(f"Sampler: {sampler}")
        if sampler == "balanced":
            batch_iter = lambda: balanced_batch_iter(X_train, y_train, cfg["batch_size"])
        elif sampler == "invfreq":
            p = make_inverse_sampling_probs(y_train.numpy())
            def inv_iter():
                N = len(X_train)
                idx = np.random.choice(N, size=N, replace=True, p=p)
                for i in range(0, N, cfg["batch_size"]):
                    sel = idx[i:i+cfg["batch_size"]]
                    yield X_train[sel], y_train[sel]
            batch_iter = inv_iter
        elif sampler == "hybrid":
            batch_iter = lambda: hybrid_batch_iter(X_train, y_train, cfg["batch_size"])
        else:
            batch_iter = lambda: batches(X_train, y_train, cfg["batch_size"])

    print(f"batch_iter - {batch_iter.__code__}")
    # ===============================================================
    # PART 5 — Training Loop
    # ===============================================================

    best_f1 = -1
    best_state = None
    patience = cfg["early_stop"]

    for epoch in range(1, cfg["epochs"]+1):
        model.train()
        total_loss = 0

        for xb, yb in batch_iter():
            xb, yb = xb.to(device), yb.to(device)
            opt.zero_grad()

            with torch.cuda.amp.autocast(enabled=(cfg["amp"] and device.type=="cuda")):
                logits = model(xb)
                loss = loss_fn(logits, yb)

            scaler.scale(loss).backward()

            if cfg["grad_clip"] > 0:
                scaler.unscale_(opt)
                torch.nn.utils.clip_grad_norm_(model.parameters(), cfg["grad_clip"])

            scaler.step(opt)
            scaler.update()

            #if sched: sched.step()

            total_loss += float(loss) * len(xb)

        if sched: sched.step()
        
        total_loss /= len(X_train)

        # ----- validation -----
        model.eval()
        logits_val = []
        with torch.no_grad():
            for xb, _ in batches(X_val, y_val, cfg["batch_size"]):
                logits_val.append(model(xb.to(device)).cpu())
        logits_val = torch.cat(logits_val, dim=0)

        if args.ordinal:
            probs = ordinal_logits_to_probs(logits_val).numpy()
        else:
            probs = torch.softmax(logits_val, dim=1).numpy()

        preds = probs.argmax(1)
        from sklearn.metrics import f1_score
        val_f1 = f1_score(y_val.numpy(), preds, average="macro")

        if epoch % 5 == 0:
            print(f"[MLP] Epoch {epoch} | Loss={total_loss:.4f} | F1={val_f1:.4f}")

        if val_f1 > best_f1 + cfg["early_stop_min_delta"]:
            best_f1 = val_f1
            best_state = {k: v.cpu().clone() for k, v in model.state_dict().items()}
            patience = cfg["early_stop"]
        else:
            patience -= 1
            if patience <= 0:
                print(f"[MLP] Early stopping at epoch {epoch}")
                break

    # restore best
    if best_state:
        model.load_state_dict(best_state)
        model.to(device)

    # ===============================================================
    # PART 5.5 — Feature Importance (optional)
    # ===============================================================
    if os.getenv("IMPORTANCE", "true").lower() in ("1","true","yes"):
        print("[MLP] Computing feature importance…")
        compute_all_importance(
            model=model,
            X=X_val.numpy(),
            y=y_val.numpy(),
            schema=schema,
            metric_fn=lambda logits, y_true: f1_score(y_true, logits.argmax(1), average="macro"),
            device=device,
            save_dir=run_dir / "importance"
        )


    # ===============================================================
    # PART 6 — Evaluation (Pre-calibration)
    # ===============================================================
    model.eval()
    logits_val = []
    with torch.no_grad():
        for xb, _ in batches(X_val, y_val, cfg["batch_size"]):
            logits_val.append(model(xb.to(device)).cpu())
    logits_val = torch.cat(logits_val, dim=0)

    if args.ordinal:
        probs_pre = ordinal_logits_to_probs(logits_val).numpy()
    else:
        probs_pre = torch.softmax(logits_val, dim=1).numpy()

    _ = evaluate_and_save(
        run_dir,
        y_val.numpy(),
        probs_pre.argmax(1),
        labels=np.arange(K),
        label_names=np.array(label_names),
        probs=probs_pre,
        model_tag="MLP (pre-calib)"
    )

    # ===============================================================
    # PART 7 — Temperature Scaling (Softmax only)
    # ===============================================================
    if not args.ordinal:
        T = fit_temperature(logits_val, torch.tensor(y_val.numpy()))
        (run_dir / "calibration.json").write_text(json.dumps({"T": float(T)}, indent=2))

        logits_cal = apply_temperature(logits_val, T)
        probs_cal = torch.softmax(logits_cal, dim=1).numpy()

        _ = evaluate_and_save(
            run_dir,
            y_val.numpy(),
            probs_cal.argmax(1),
            labels=np.arange(K),
            label_names=np.array(label_names),
            probs=probs_cal,
            model_tag="MLP (calibrated)"
        )

    print(f"[MLP] Finished. Artifacts at {run_dir}")


if __name__ == "__main__":
    main()
