import numpy as np
import torch
from tqdm import tqdm
from pathlib import Path
import sys
ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from libs.utils.importance import integrated_gradients_compare


def run_batch_ig(
    model,
    X,
    y_true,
    limit=None,
    only_errors=False,
    device="cpu"
):
    """
    Computes IG across many samples.

    Args:
        model: trained classifier
        X: torch tensor [N, D]
        y_true: torch tensor [N]
        limit: max number of samples
        only_errors: if True, IG only for misclassified samples
    Returns:
        records: list of dicts with sample info & IG vectors
    """

    model.eval()
    N = len(X)
    idxs = range(N) if limit is None else range(min(limit, N))

    records = []

    for i in tqdm(idxs, desc="Computing IG for batch"):
        xs = X[i]
        yt = int(y_true[i])

        pred, true, ig_pred, ig_true = integrated_gradients_compare(
            model,
            xs,
            true_class=yt,
            device=device
        )

        if only_errors and pred == yt:
            continue

        records.append({
            "index": i,
            "true": yt,
            "pred": pred,
            "ig_pred": ig_pred,
            "ig_true": ig_true
        })

    return records
