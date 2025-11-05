import os, argparse, json, time, random, math, sys
import numpy as np
import pandas as pd
import torch
from torch import nn
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from dotenv import load_dotenv
load_dotenv(dotenv_path=".env", override=False)

from libs.utils.reporting import make_run_dir, evaluate_and_save, save_label_map, save_input_data_information
from libs.utils.imbalance import class_weights_from_counts, choose_imbalance_strategy
from libs.utils.losses import WeightedCrossEntropy, FocalLoss
from libs.utils.calibration import fit_temperature, apply_temperature
from libs.utils.metrics import brier_multiclass, ece
from libs.models.io_config import (
    load_yaml, getenv_bool, load_prep_artifacts, read_arrow_matrix, build_test_features_from_csv
)

# ----------------------- Ordinal -----------------------
import torch.nn.functional as F

def ordinal_targets(y_long: torch.Tensor, num_classes: int) -> torch.Tensor:
    """
    CORAL targets: for class y in [0..K-1], targets are length K-1:
    t[c] = 1 if y > c else 0
    """
    K = int(num_classes)
    y = y_long.view(-1, 1)                         # (N,1)
    c = torch.arange(K - 1, device=y.device).view(1, -1)  # (1,K-1)
    return (y > c).to(torch.float32)               # (N,K-1)

def ordinal_logits_to_probs(logits_ord: torch.Tensor) -> torch.Tensor:
    """
    Convert K-1 ordinal logits to K class probabilities.
    p(y>c) = sigmoid(l_c). Then:
      P(y=0)   = 1 - p(y>0)
      P(y=c)   = p(y>c-1) - p(y>c) for 1<=c<=K-2
      P(y=K-1) = p(y>K-2)
    """
    s = torch.sigmoid(logits_ord)                  # (N,K-1)  = p(y>c)
    N, Km1 = s.shape
    K = Km1 + 1
    # build [1, s_0, s_1, ..., s_{K-2}] and [s_0, s_1, ..., s_{K-2}, 0]
    left  = torch.cat([torch.ones(N,1, device=s.device), s], dim=1)
    right = torch.cat([s, torch.zeros(N,1, device=s.device)], dim=1)
    probs = (left - right).clamp_min(1e-8)         # (N,K)
    probs = probs / probs.sum(dim=1, keepdim=True) # normalize for safety
    return probs


# ----------------------- Norms + DropPath -----------------------
class DropPath(nn.Module):
    def __init__(self, drop_prob: float = 0.0):
        super().__init__()
        self.drop_prob = float(drop_prob)
    def forward(self, x):
        if (not self.training) or self.drop_prob == 0.0:
            return x
        keep = 1.0 - self.drop_prob
        mask = torch.empty((x.shape[0],) + (1,)*(x.ndim-1), device=x.device, dtype=x.dtype).bernoulli_(keep)
        return x * mask / keep

class BlockDropout(nn.Module):
    def __init__(self, spans):
        super().__init__(); self.spans = spans or []
    def forward(self, x):
        if (not self.training) or (not self.spans): return x
        out = x
        for (s, e, p) in self.spans:
            if p and p > 0:
                keep = 1.0 - float(p)
                mask = x.new_zeros((x.size(0), e - s)).bernoulli_(keep) / keep
                out[:, s:e] = out[:, s:e] * mask
        return out

def _parse_hidden(s: str):
    return [int(x.strip()) for x in s.split(",") if x.strip()]

def _get_act(name: str):
    name = (name or "gelu").lower()
    return {
        "relu": nn.ReLU(),
        "gelu": nn.GELU(),
        "silu": nn.SiLU(),
        "mish": nn.Mish(),
        "tanh": nn.Tanh(),
    }.get(name, nn.GELU())

def _maybe_norm(norm_kind: str, width: int):
    if norm_kind == "layernorm_per_block":
        return nn.LayerNorm(width)
    if norm_kind == "batchnorm_per_block":
        return nn.BatchNorm1d(width)
    return None

def _eval_macro_f1(y_true, y_pred):
    from sklearn.metrics import f1_score
    return float(f1_score(y_true, y_pred, average="macro"))

def build_mlp(in_dim: int,
              num_classes: int,
              hidden: list[int],
              act_name: str,
              dropout: float,
              norm: str,
              droppath_rate: float = 0.0,
              droppath_schedule: str = "linear"):
    act = _get_act(act_name)
    layers = []
    prev = in_dim

    ln_io = (norm == "layernorm_input_output")
    if ln_io: layers.append(nn.LayerNorm(in_dim))

    n_blocks = len(hidden)
    for bi, h in enumerate(hidden):
        layers.append(nn.Linear(prev, h))
        n = _maybe_norm(norm, h)
        if n is not None:
            layers.append(n)
        layers.append(act)
        if dropout and dropout > 0:
            layers.append(nn.Dropout(dropout))
        if droppath_rate > 0.0:
            p = (droppath_rate * (bi + 1) / n_blocks) if (droppath_schedule == "linear" and n_blocks > 1) else droppath_rate
            layers.append(DropPath(p))
        prev = h

    if ln_io: layers.append(nn.LayerNorm(prev))
    layers.append(nn.Linear(prev, num_classes))
    return nn.Sequential(*layers)

# ----------------------------- Config helpers -----------------------------
def _env_bool(k, default=False):
    v = os.getenv(k)
    if v is None: return default
    return str(v).lower() in ("1","true","yes","y","on")

def _env_int(k, default):
    try: return int(os.getenv(k, default))
    except: return default

def _env_float(k, default):
    try: return float(os.getenv(k, default))
    except: return default

def _env_str(k, default):
    v = os.getenv(k)
    return v if (v is not None and v != "") else default

def _yaml_get(dct, path, default=None):
    cur = dct
    for p in path.split("."):
        if not isinstance(cur, dict) or p not in cur: return default
        cur = cur[p]
    return cur

# Merge precedence: CLI > ENV > YAML > default
def resolve_cfg(args, yaml_cfg):
    # YAML defaults (safe if missing)
    y_epochs       = _yaml_get(yaml_cfg, "algorithms.mlp.params.epochs",       100)
    y_bs           = _yaml_get(yaml_cfg, "algorithms.mlp.params.batch_size",   512)
    y_lr           = _yaml_get(yaml_cfg, "algorithms.mlp.params.lr",           1e-3)
    y_wd           = _yaml_get(yaml_cfg, "algorithms.mlp.params.weight_decay", 1e-4)
    y_pat          = _yaml_get(yaml_cfg, "algorithms.mlp.params.early_stop_patience", 10)
    y_min_delta    = _yaml_get(yaml_cfg, "algorithms.mlp.params.early_stop_min_delta", 0.0)
    y_dropout      = _yaml_get(yaml_cfg, "algorithms.mlp.params.dropout", 0.30)
    y_hidden       = _yaml_get(yaml_cfg, "algorithms.mlp.params.hidden_sizes", [512,256])
    y_act          = _yaml_get(yaml_cfg, "algorithms.mlp.params.activation", "gelu")
    y_norm         = _yaml_get(yaml_cfg, "algorithms.mlp.params.norm", "layernorm_per_block")
    y_scheduler    = _yaml_get(yaml_cfg, "algorithms.mlp.params.scheduler", "onecycle")
    y_grad_clip    = _yaml_get(yaml_cfg, "algorithms.mlp.params.grad_clip", 1.0)
    y_droppath     = _yaml_get(yaml_cfg, "algorithms.mlp.params.droppath", 0.0)
    y_dpsched      = _yaml_get(yaml_cfg, "algorithms.mlp.params.droppath_schedule", "linear")

    # ENV (your .env keys)
    e_epochs       = _env_int("EPOCHS", y_epochs)
    e_bs           = _env_int("BATCH_SIZE", y_bs)
    e_lr           = _env_float("LR", y_lr)
    e_wd           = _env_float("WEIGHT_DECAY", y_wd)
    e_pat          = _env_int("EARLY_STOP", y_pat)
    e_min_delta    = _env_float("EARLY_STOP_MIN_DELTA", y_min_delta)
    e_dropout      = _env_float("MLP_DROPOUT", y_dropout)
    e_hidden_str   = _env_str("MLP_HIDDEN", ",".join(str(x) for x in y_hidden))
    e_act          = _env_str("MLP_ACT", y_act)
    e_norm         = _env_str("MLP_NORM", y_norm)
    e_scheduler    = _env_str("SCHEDULER", y_scheduler)
    e_grad_clip    = _env_float("GRAD_CLIP", y_grad_clip)
    e_droppath     = _env_float("MLP_DROPPATH", y_droppath)
    e_dpsched      = _env_str("MLP_DROPPATH_SCHEDULE", y_dpsched)

    # CLI overrides (if provided)
    cfg = {
        "epochs":                args.epochs                if args.epochs is not None else e_epochs,
        "batch_size":            args.batch_size            if args.batch_size is not None else e_bs,
        "lr":                    args.lr                    if args.lr is not None else e_lr,
        "weight_decay":          args.weight_decay          if args.weight_decay is not None else e_wd,
        "early_stop":            args.early_stop            if args.early_stop is not None else e_pat,
        "early_stop_min_delta":  args.early_stop_min_delta  if args.early_stop_min_delta is not None else e_min_delta,
        "scheduler":             args.scheduler             if args.scheduler is not None else e_scheduler,
        "amp":                   args.amp                   if args.amp is not None else getenv_bool("AMP", True),
        "grad_clip":             args.grad_clip             if args.grad_clip is not None else e_grad_clip,
        "mlp_hidden":            _parse_hidden(args.mlp_hidden) if args.mlp_hidden is not None else _parse_hidden(e_hidden_str),
        "mlp_dropout":           args.mlp_dropout           if args.mlp_dropout is not None else e_dropout,
        "mlp_act":               args.mlp_act               if args.mlp_act is not None else e_act,
        "mlp_norm":              args.mlp_norm              if args.mlp_norm is not None else e_norm,
        "mlp_droppath":          args.mlp_droppath          if args.mlp_droppath is not None else e_droppath,
        "mlp_droppath_schedule": args.mlp_droppath_schedule if args.mlp_droppath_schedule is not None else e_dpsched,
    }
    return cfg

# ----------------------------- Main -----------------------------
def main():
    # Determinism
    seed = int(os.getenv("RANDOM_SEED", 42))
    torch.manual_seed(seed); np.random.seed(seed); random.seed(seed)
    torch.backends.cudnn.deterministic = True
    torch.backends.cudnn.benchmark = False

    ap = argparse.ArgumentParser()
    ap.add_argument("--features-config", default="configs/features.yaml")
    ap.add_argument("--prep-root", default=os.getenv("PREP_CACHE_ROOT","artifacts/prep"))
    ap.add_argument("--prep-id", required=True)
    ap.add_argument("--fusion", default=os.getenv("FUSION","B1"))

    # training knobs (CLI optional; ENV/YAML resolve later)
    ap.add_argument("--epochs", type=int)
    ap.add_argument("--batch-size", type=int)
    ap.add_argument("--lr", type=float)
    ap.add_argument("--weight-decay", type=float)
    ap.add_argument("--early-stop", type=int, help="patience on macro-F1")
    ap.add_argument("--early-stop-min-delta", type=float, help="minimum F1 improvement to reset patience")
    ap.add_argument("--scheduler", choices=["none","onecycle"])
    ap.add_argument("--amp", type=lambda s: str(s).lower() in ("1","true","yes","y","on"),
                default=None, help="Enable AMP mixed precision (env: AMP)")
    ap.add_argument("--no-amp", dest="amp", action="store_false")
    ap.add_argument("--grad-clip", type=float)

    # architecture
    ap.add_argument("--mlp-hidden")
    ap.add_argument("--mlp-dropout",  type=float)
    ap.add_argument("--mlp-act")
    ap.add_argument("--mlp-norm",     choices=["none","layernorm_per_block","batchnorm_per_block","layernorm_input_output"])
    ap.add_argument("--mlp-droppath", type=float)
    ap.add_argument("--mlp-droppath-schedule", choices=["linear","constant"])

    # loss/imbalance
    ap.add_argument("--loss", choices=["auto","weighted_ce","focal"])
    ap.add_argument("--gamma", type=float)

    # ordinal
    ap.add_argument("--ordinal", type=lambda s: str(s).lower() in ("1","true","yes","y","on"),
                default=getenv_bool("ORDINAL", True))

    # inference
    ap.add_argument("--test-csv", default=None)
    args = ap.parse_args()

    device = torch.device("cuda" if (torch.cuda.is_available() and not getenv_bool("FORCE_CPU", False) and _env_str("DEVICE","gpu")=="gpu") else "cpu")

    yaml_cfg = load_yaml(args.features_config)
    art = load_prep_artifacts(args.prep_root, args.prep_id)
    cfg = resolve_cfg(args, yaml_cfg)

    print(f"[MLP] cfg: epochs={cfg['epochs']} bs={cfg['batch_size']} lr={cfg['lr']} wd={cfg['weight_decay']} "
          f"patience={cfg['early_stop']} min_delta={cfg['early_stop_min_delta']} "
          f"strategy={os.getenv("IMBALANCE", "weighted_ce")} "
          f"dropout={cfg['mlp_dropout']} droppath={cfg['mlp_droppath']}({cfg['mlp_droppath_schedule']}) "
          f"norm={cfg['mlp_norm']} act={cfg['mlp_act']} scheduler={cfg['scheduler']} amp={cfg['amp']} grad_clip={cfg['grad_clip']}")

    # matrices
    Xtr = read_arrow_matrix(art["X_train"])
    Xva = read_arrow_matrix(art["X_val"])
    ytr = np.load(art["y_train"])
    yva = np.load(art["y_val"])

    run_dir = make_run_dir("MLP", artifacts_root=os.getenv("ARTIFACTS_DIR","artifacts"))
    save_input_data_information(run_dir, ytr, yva, args.prep_id)

    X_train = torch.tensor(Xtr, dtype=torch.float32)
    X_val   = torch.tensor(Xva, dtype=torch.float32)
    classes = sorted(pd.unique(pd.concat([pd.Series(ytr), pd.Series(yva)], ignore_index=True)))
    cls_to_idx = {c:i for i,c in enumerate(classes)}
    y_train = torch.tensor([cls_to_idx[c] for c in ytr], dtype=torch.long)
    y_val   = torch.tensor([cls_to_idx[c] for c in yva], dtype=torch.long)

    in_dim = int(X_train.shape[1])
    label_space = np.unique(np.concatenate([y_train.numpy(), y_val.numpy()])); label_space.sort()
    num_classes = int(label_space.max() + 1)
    idx_to_name = {v: k for k, v in cls_to_idx.items()}
    label_names = [idx_to_name[i] for i in label_space]   # ['P1','P2','P3','P4','P5']


    # schema-aware block dropout spans
    spans = []
    try:
        schema_path = art.get("schema")
        if schema_path and Path(schema_path).exists():
            schema = json.loads(Path(schema_path).read_text())
            name2p = {
                "text_pca": float(os.getenv("BLOCK_DROPOUT_TEXT", "0.0")),
                "topics_emb": float(os.getenv("BLOCK_DROPOUT_TOPICS", "0.0")),
                "categorical": float(os.getenv("BLOCK_DROPOUT_CATEGORICAL", "0.0")),
                "actors_hash": float(os.getenv("BLOCK_DROPOUT_ACTORS", "0.0")),
                "numeric": float(os.getenv("BLOCK_DROPOUT_NUMERIC", "0.0")),
            }
            for blk in schema.get("blocks", []):
                p = float(name2p.get(blk["name"], 0.0))
                spans.append((int(blk["start"]), int(blk["end"]), p))
    except Exception:
        spans = []


    # model: output dims depend on ordinal flag
    out_dim = (num_classes - 1) if args.ordinal else num_classes
    model = nn.Sequential(
        BlockDropout(spans),
        build_mlp(
            in_dim, out_dim,
            hidden=cfg["mlp_hidden"],
            act_name=cfg["mlp_act"],
            dropout=cfg["mlp_dropout"],
            norm=cfg["mlp_norm"],
            droppath_rate=cfg["mlp_droppath"],
            droppath_schedule=cfg["mlp_droppath_schedule"],
        )
    ).to(device)


    # imbalance / loss
    class LogitAdjustedCrossEntropy(nn.Module):
        def __init__(self, class_counts, tau=1.0):
            super().__init__()
            counts = torch.as_tensor(class_counts, dtype=torch.float32)
            priors = counts / counts.sum()
            self.register_buffer("bias", -tau * priors.log())  # subtract tau*log(pi)
        def forward(self, logits, targets):
            return nn.functional.cross_entropy(logits + self.bias, targets)

    strategy = args.loss if args.loss else choose_imbalance_strategy(y_train.tolist(), algo="mlp")[0]
    gamma = args.gamma if args.gamma is not None else _env_float("FOCAL_GAMMA", 1.5)
    current_epoch = 0
    if args.ordinal:
        # CORAL-style: BCEWithLogitsLoss over K-1 thresholds
        # Optional: per-threshold pos_weight from train distribution (kept simple here)
        if strategy == "weighted_ce":
            # compute once globally from all of y_train
            cw = class_weights_from_counts(y_train.tolist())
            # === Low-end boost ===
            LOW_END_BOOST = float(os.getenv("LOW_END_WEIGHT_BOOST", 1.2))  # 30% stronger by default
            for c in [0, 1]:  # P1, P2 (assuming label indices 0=P1, 1=P2)
                if c in cw:
                    cw[c] *= LOW_END_BOOST
            # Re-normalize mean to 1 for stability
            mean_w = np.mean(list(cw.values()))
            cw = {k: v / mean_w for k, v in cw.items()}


            def ordinal_bce_loss(logits, y_long):
                """
                logits: (N, K-1)
                targets: (N, K-1) binary matrix where t[i,c] = 1 if y_i > c else 0
                sample_weights: (N,) or None
                """
                lambda_thresh=3e-3
                tgt = ordinal_targets(y_long, num_classes)         # (N,K-1)
                #eps = float(os.getenv("LABEL_SMOOTH_EPS", 0.05))
                eps = 0.05 if current_epoch < 20 else 0.02
                tgt = (1 - eps) * tgt + eps * 0.5
                # Build per-sample weights from class weights
                sample_weights = torch.tensor([cw[int(c)] for c in y_long], dtype=torch.float32, device=device)
                # Compute standard BCE per threshold
                bce = F.binary_cross_entropy_with_logits(logits, tgt, reduction='none')  # (N, K-1)
                loss_per_sample = bce.mean(dim=1)  # average over thresholds for each sample
                
                if sample_weights is not None:
                    loss_per_sample = loss_per_sample * sample_weights
                base_loss = loss_per_sample.mean()

                # === Threshold-spacing regularizer ===
                # Encourage ordered, evenly spaced thresholds
                diffs = logits[:, :-1] - logits[:, 1:]  # (N, K-2)
                spacing_penalty = torch.relu(-diffs).mean()  # penalize violations of monotonic order

                return base_loss + lambda_thresh * spacing_penalty

            loss_fn = ordinal_bce_loss
        else:
            loss_bce = nn.BCEWithLogitsLoss()
            def loss_fn_ord(logits_ord, y_long):
                tgt = ordinal_targets(y_long, num_classes)         # (N,K-1)
                return loss_bce(logits_ord, tgt)
            loss_fn = loss_fn_ord
    elif os.getenv("IMBALANCE", "weighted_ce") == "logit_adjust":
        # natural sampling + logit-adjusted CE
        # build counts in label index space 0..K-1
        y_np = y_train.numpy()
        counts = np.bincount(y_np, minlength=num_classes)
        tau = float(os.getenv("LA_TAU", "1.0"))
        loss_fn = LogitAdjustedCrossEntropy(class_counts=counts, tau=tau).to(device)
    elif strategy == "weighted_ce":
        cw = class_weights_from_counts(y_train.tolist())
        weights = torch.ones(num_classes, dtype=torch.float32)
        for c, w in cw.items():
            if c < num_classes:
                weights[c] = w
        loss_fn = WeightedCrossEntropy(class_weights=weights.to(device))
    elif strategy == "focal":
        loss_fn = FocalLoss(gamma=gamma, alpha=None)
    else:
        loss_fn = WeightedCrossEntropy(class_weights=None)

    # Optimization
    opt = torch.optim.AdamW(model.parameters(), lr=cfg["lr"], weight_decay=cfg["weight_decay"])

    # Steps
    steps_per_epoch = int(math.ceil(len(X_train) / max(1, cfg["batch_size"])))
    if cfg["scheduler"] == "onecycle":
        sched = torch.optim.lr_scheduler.OneCycleLR(
            opt, max_lr=cfg["lr"], epochs=cfg["epochs"], steps_per_epoch=max(1, steps_per_epoch),
            pct_start=0.25, anneal_strategy="cos", div_factor=25.0, final_div_factor=1e3
        )
    else:
        sched = None

    scaler = torch.cuda.amp.GradScaler(enabled=(cfg["amp"] and device.type == "cuda"))

    # -- option: natural batch sampling
    def iterate_batches(X, y, bs):
        for i in range(0, len(X), bs):
            yield X[i:i+bs], y[i:i+bs]

    # --- option: balanced sampling probs (build once from y_train) ---
    y_np = y_train.numpy()
    _cls, _cnt = np.unique(y_np, return_counts=True)
    _inv = {int(c): 1.0/float(n) for c, n in zip(_cls, _cnt)}
    _samp_p = np.array([_inv[int(c)] for c in y_np], dtype=np.float64)
    _samp_p /= _samp_p.sum()

    def iterate_balanced(X, y, bs):
        # sample with replacement to form one 'epoch' of size len(X)
        idx = np.random.choice(len(X), size=len(X), replace=True, p=_samp_p)
        for i in range(0, len(idx), bs):
            sel = idx[i:i+bs]
            yield X[sel], y[sel]

    # ---------- train loop with min_delta ----------
    best_state, best_f1 = None, -1.0
    patience_left = int(cfg["early_stop"])
    for epoch in range(1, cfg["epochs"]+1):
        current_epoch = epoch
        model.train()
        tr_loss = 0.0
        for xb, yb in iterate_batches(X_train, y_train, cfg["batch_size"]):
            xb = xb.to(device); yb = yb.to(device)
            opt.zero_grad(set_to_none=True)
            with torch.cuda.amp.autocast(enabled=(cfg["amp"] and device.type=="cuda")):
                logits = model(xb)
                loss = loss_fn(logits, yb)
            scaler.scale(loss).backward()
            if cfg["grad_clip"] and cfg["grad_clip"] > 0:
                scaler.unscale_(opt); torch.nn.utils.clip_grad_norm_(model.parameters(), cfg["grad_clip"])
            scaler.step(opt); scaler.update()
            if sched is not None: sched.step()
            tr_loss += float(loss.detach().cpu()) * len(xb)
        tr_loss /= len(X_train)

        model.eval()
        with torch.no_grad():
            val_logits = []
            for xb, _ in iterate_batches(X_val, y_val, cfg["batch_size"]):
                val_logits.append(model(xb.to(device)).detach().cpu())
            val_logits = torch.cat(val_logits, dim=0)
            if args.ordinal:
                val_probs  = ordinal_logits_to_probs(val_logits).numpy()
            else:
                val_probs = torch.softmax(val_logits, dim=1).numpy()
            y_val_pred = val_probs.argmax(1)
            val_f1 = _eval_macro_f1(y_val.numpy(), y_val_pred)

        if epoch % 5 == 0 or epoch == 1:
            print(f"[MLP] epoch {epoch} train_loss {tr_loss:.4f}  val_macroF1 {val_f1:.4f}")

        improved = (val_f1 - best_f1) > float(cfg["early_stop_min_delta"])
        if improved:
            best_f1 = val_f1
            best_state = {k: v.detach().cpu().clone() for k, v in model.state_dict().items()}
            patience_left = int(cfg["early_stop"])
        else:
            patience_left -= 1
            if patience_left <= 0:
                print(f"[MLP] Early stop at epoch {epoch} (best macro-F1={best_f1:.4f})")
                break

    # restore best
    if best_state is not None:
        model.load_state_dict(best_state)
    model.to(device)

    # Save label map (order == label_space)
    save_label_map(run_dir, labels=label_space, label_names=label_names)

    # After training and before validation
    if args.ordinal:
        # === Post-training fine-tune: only biases ===
        for name, param in model.named_parameters():
            param.requires_grad = "bias" in name

        opt = torch.optim.SGD(filter(lambda p: p.requires_grad, model.parameters()), lr=1e-3)
        for epoch in range(5):
            for xb, yb in iterate_batches(X_train, y_train, cfg["batch_size"]):
                xb, yb = xb.to(device), yb.to(device)
                opt.zero_grad()
                logits = model(xb)
                loss = loss_fn(logits, yb)
                loss.backward()
                opt.step()




    # PRE-CALIB (validation)
    model.eval()
    with torch.no_grad():
        val_logits = []
        for xb, _ in iterate_batches(X_val, y_val, cfg["batch_size"]):
            val_logits.append(model(xb.to(device)).detach().cpu())
        val_logits = torch.cat(val_logits, dim=0)   
        if args.ordinal:
            val_probs  = ordinal_logits_to_probs(val_logits).numpy()
        else:
            val_probs  = torch.softmax(val_logits, dim=1).numpy()
        y_pred_pre = val_probs.argmax(1)
        y_val_np   = y_val.numpy()

    _ = evaluate_and_save(run_dir, y_val_np, y_pred_pre,
                          np.array([int(v) for k,v in cls_to_idx.items()], dtype=np.int32), 
                          np.array([str(k) for k,v in cls_to_idx.items()], dtype=str),
                          val_probs, model_tag="MLP (pre-calib)")

    # Temperature scaling on validation
    # Temperature scaling on ordinal heads is trickier. Easiest path: skip T-scaling when ordinal is on
    if not args.ordinal:
        with torch.no_grad():
            T = fit_temperature(val_logits, torch.from_numpy(y_val_np))
        (run_dir/"calibration.json").write_text(json.dumps({"temperature": float(T)}, indent=2), encoding="utf-8")

        # POST-CALIB
        with torch.no_grad():
            val_probs_cal = torch.softmax(apply_temperature(val_logits, T), dim=1).numpy()
            y_pred_cal    = val_probs_cal.argmax(1)

        # --- per-class threshold tuning on calibrated probs (macro-F1) ---
        probs = val_probs_cal  # shape (N, K)
        y_true = y_val.numpy()
        K = probs.shape[1]
        thr = np.full(K, 1.0 / K, dtype=np.float32)  # base
        cand = np.linspace(0.25, 0.55, 13)           # light grid for minority classes

        # assume label indices 0..K-1 align with sorted ["P1", "P2", ...]
        best_f1 = -1.0
        best_thr = thr.copy()

        from sklearn.metrics import f1_score

        def predict_with_thresholds(P, thr_vec):
            pred = P.argmax(1)
            for c in range(len(thr_vec)):
                mask = P[:, c] >= thr_vec[c]
                pred[mask] = c
            return pred

        # tune thresholds for first two tail classes (adjust if needed)
        for t1 in cand:
            for t2 in cand:
                t = thr.copy()
                if K > 0: t[0] = t1  # class 0 (P1)
                if K > 1: t[1] = t2  # class 1 (P2)
                pred = predict_with_thresholds(probs.copy(), t)
                f1 = f1_score(y_true, pred, average="macro")
                if f1 > best_f1:
                    best_f1, best_thr = f1, t.copy()

        # persist and report
        (Path(run_dir) / "thresholds.json").write_text(
            json.dumps({"thresholds": best_thr.tolist(), "val_macro_f1": float(best_f1)}, indent=2),
            encoding="utf-8"
        )
        print(f"[MLP] tuned thresholds → val_macroF1={best_f1:.4f}")

        y_pred_thr = predict_with_thresholds(probs.copy(), best_thr)

        _ = evaluate_and_save(run_dir, y_true, y_pred_thr, 
                            np.array([int(v) for k, v in cls_to_idx.items()], dtype=np.int32),
                            np.array([str(k) for k, v in cls_to_idx.items()], dtype=str), 
                            probs, model_tag="MLP (calibrated+thr)")


        _ = evaluate_and_save(run_dir, y_val_np, y_pred_cal,
                            np.array([int(v) for k,v in cls_to_idx.items()], dtype=np.int32), 
                            np.array([str(k) for k,v in cls_to_idx.items()], dtype=str),
                            val_probs_cal, model_tag="MLP (calibrated)")
    
    print(f"[MLP] Wrote outputs to {run_dir}")

    # === optional cached test ===
    if art["X_test"] and art["y_test"] is not None:
        X_test = torch.tensor(read_arrow_matrix(art["X_test"]), dtype=torch.float32).to(device)
        y_test = torch.tensor(np.load(art["y_test"]), dtype=torch.long).cpu().numpy()
        model.eval()
        with torch.no_grad():
            test_logits = []
            for i in range(0, len(X_test), cfg["batch_size"]):
                test_logits.append(model(X_test[i:i+cfg["batch_size"]]).detach().cpu())
            test_logits = torch.cat(test_logits, dim=0)
            test_probs = torch.softmax(apply_temperature(test_logits, T), dim=1).numpy()
            # load tuned thresholds (if present)
            thr_path = Path(run_dir) / "thresholds.json"
            if thr_path.exists():
                best_thr = np.array(json.loads(thr_path.read_text())["thresholds"], dtype=np.float32)
                def _predict_with_thresholds(P, thr_vec):
                    pred = P.argmax(1)
                    for c in range(len(thr_vec)):
                        mask = P[:, c] >= thr_vec[c]
                        pred[mask] = c
                    return pred
                y_pred_test = _predict_with_thresholds(test_probs.copy(), best_thr)
            else:
                y_pred_test = test_probs.argmax(1)

        test_metrics = {
            "macro_f1": float(__import__("sklearn.metrics").metrics.f1_score(y_test, y_pred_test, average="macro")),
            "p1_recall": float(((y_test==1)&(y_pred_test==1)).sum()/max(1,(y_test==1).sum())) if (1 in np.unique(y_test)) else float("nan"),
            "brier": brier_multiclass(test_probs, y_test),
            "ece": ece(test_probs, y_test, n_bins=15),
        }
        (run_dir/"test_metrics.json").write_text(json.dumps(test_metrics, indent=2), encoding="utf-8")

    # === external CSV test ===
    if args.test_csv:
        schema = art["schema"] or {}
        text_cols = _yaml_get(yaml_cfg, "data.text_cols", [])
        emb_model = _yaml_get(yaml_cfg, "embeddings.text.model", "sentence-transformers/all-MiniLM-L6-v2")
        max_len = _yaml_get(yaml_cfg, "embeddings.text.truncate_tokens", 192)
        X_new, ids = build_test_features_from_csv(
            args.test_csv, schema, art["pca"], art["scaler"], art["topics_centroids"],
            text_cols, emb_model, max_len
        )
        model.eval()
        with torch.no_grad():
            probs_new = torch.softmax(apply_temperature(model(torch.tensor(X_new, dtype=torch.float32, device=device)), T), dim=1).cpu().numpy()
        out = Path("artifacts/predictions"); out.mkdir(parents=True, exist_ok=True)
        pd.DataFrame({"id": ids, "pred": probs_new.argmax(1)}).to_csv(out / f"mlp_preds_{args.prep_id}.csv", index=False)
        print(f"[MLP] Wrote predictions to {out / f'mlp_preds_{args.prep_id}.csv'}")

if __name__ == "__main__":
    main()
