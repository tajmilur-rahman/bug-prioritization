
import numpy as np

__all__ = ["brier_multiclass", "ece"]

def brier_multiclass(probs: np.ndarray, y_true: np.ndarray) -> float:
    N, C = probs.shape
    y1 = np.eye(C, dtype=float)[y_true]
    return float(np.mean(np.sum((probs - y1) ** 2, axis=1)))

def ece(probs: np.ndarray, y_true: np.ndarray, n_bins: int = 15) -> float:
    conf = probs.max(axis=1)
    y_pred = probs.argmax(axis=1)
    acc = (y_pred == y_true).astype(float)
    bins = np.linspace(0.0, 1.0, n_bins + 1)
    ece_val = 0.0
    for i in range(n_bins):
        mask = (conf > bins[i]) & (conf <= bins[i+1])
        if not np.any(mask):
            continue
        bin_acc = acc[mask].mean()
        bin_conf = conf[mask].mean()
        ece_val += (mask.mean()) * abs(bin_acc - bin_conf)
    return float(ece_val)
