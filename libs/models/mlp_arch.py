# libs/models/mlp_arch.py
# Clean, reusable MLP architecture module
# ------------------------------------------------------------
import torch
import torch.nn as nn
import torch.nn.functional as F


# ---------------------- DropPath ----------------------
class DropPath(nn.Module):
    def __init__(self, drop_prob: float = 0.0):
        super().__init__()
        self.drop_prob = float(drop_prob)

    def forward(self, x):
        if (not self.training) or self.drop_prob == 0.0:
            return x
        keep = 1.0 - self.drop_prob
        shape = (x.shape[0],) + (1,) * (x.ndim - 1)
        mask = torch.empty(shape, device=x.device, dtype=x.dtype).bernoulli_(keep)
        return x * mask / keep


# ---------------------- BlockDropout ----------------------
class BlockDropout(nn.Module):
    """
    Drops specific feature blocks based on schema spans.
    spans = [(start, end, prob), ...]
    """
    def __init__(self, spans):
        super().__init__()
        self.spans = spans or []

    def forward(self, x):
        if not self.training or not self.spans:
            return x
        out = x
        for (s, e, p) in self.spans:
            if p and p > 0:
                keep = 1 - float(p)
                mask = (
                    x.new_zeros((x.size(0), e - s))
                    .bernoulli_(keep)
                    / keep
                )
                out[:, s:e] = out[:, s:e] * mask
        return out


# ---------------------- helpers ----------------------
def get_activation(name: str):
    n = (name or "gelu").lower()
    return {
        "relu": nn.ReLU(),
        "gelu": nn.GELU(),
        "silu": nn.SiLU(),
        "mish": nn.Mish(),
        "tanh": nn.Tanh(),
    }.get(n, nn.GELU())


def maybe_norm(norm: str, width: int):
    if norm == "layernorm_per_block":
        return nn.LayerNorm(width)
    if norm == "batchnorm_per_block":
        return nn.BatchNorm1d(width)
    return None


def parse_hidden_string(s: str):
    return [int(x.strip()) for x in s.split(",") if x.strip()]


# ---------------------- MLP ----------------------
def build_mlp(
    in_dim: int,
    out_dim: int,
    hidden_list: list[int],
    act_name: str,
    dropout: float,
    norm: str,
    droppath_rate: float = 0.0,
    droppath_schedule: str = "linear",
):
    act = get_activation(act_name)
    layers = []
    prev = in_dim

    n_blocks = len(hidden_list)
    ln_io = norm == "layernorm_input_output"
    if ln_io:
        layers.append(nn.LayerNorm(in_dim))

    # hidden layers
    for idx, h in enumerate(hidden_list):
        layers.append(nn.Linear(prev, h))

        n = maybe_norm(norm, h)
        if n is not None:
            layers.append(n)

        layers.append(act)

        if dropout > 0:
            layers.append(nn.Dropout(dropout))

        if droppath_rate > 0:
            # schedule droppath per block
            p = (
                droppath_rate * (idx + 1) / n_blocks
                if droppath_schedule == "linear" and n_blocks > 1
                else droppath_rate
            )
            layers.append(DropPath(p))

        prev = h

    if ln_io:
        layers.append(nn.LayerNorm(prev))

    #layers.append(nn.Linear(prev, out_dim))
    return nn.Sequential(*layers)


# ---------------------- Custom MLP Wrapper ----------------------
class DropoutMLP(nn.Module):
    """
    Wrapper that combines:
        - BlockDropout for feature-block stochastic masking
        - build_mlp() for the actual MLP layers
    This makes the model safely savable via torch.save(model)
    and loadable via torch.load(...) in IG & inference scripts.
    """

    def __init__(self, in_dim, out_dim, cfg, spans, ordinal):
        super().__init__()

        # optional block dropout (already handles spans=None internally)
        self.blockdrop = BlockDropout(spans)
        self.ordinal = ordinal

        # main MLP built from config
        self.backbone = build_mlp(
            in_dim=in_dim,
            out_dim=cfg["hidden"][-1],
            hidden_list=cfg["hidden"],
            act_name=cfg["act"],
            dropout=cfg["dropout"],
            norm=cfg["norm"],
            droppath_rate=cfg.get("droppath", 0.0),
            droppath_schedule=cfg.get("droppath_schedule", "linear"),
        )

        self.head_linear = nn.Linear(cfg["hidden"][-1], out_dim)

        if ordinal:
            # ordinal head
            self.head_ord = nn.Linear(cfg["hidden"][-1], out_dim)
            # regression head
            self.head_reg = nn.Linear(cfg["hidden"][-1], 1)



    def forward(self, x):
        x = self.blockdrop(x)
        h = self.backbone(x)
        if self.ordinal:
            logits_ord = self.head_ord(h)
            pred_reg = self.head_reg(h).squeeze(1)
            return logits_ord, pred_reg
        else:
            return self.head_linear(h), None
