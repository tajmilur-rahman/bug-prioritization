import numpy as np


def blockwise_ig(ig_vec, block_schema):
    """
    Returns block → sum(abs IG) dict.
    """
    block_importance = {}

    for blk in block_schema:
        name = blk["name"]
        s, e = blk["start"], blk["end"]
        block_importance[name] = float(np.sum(np.abs(ig_vec[s:e])))

    return block_importance


def compare_blocks(ig_pred, ig_true, block_schema):
    """
    Compare block-level IG contributions between predicted vs true classes.
    """
    bp = blockwise_ig(ig_pred, block_schema)
    bt = blockwise_ig(ig_true, block_schema)

    diff = {k: abs(bp[k] - bt[k]) for k in bp}

    return bp, bt, diff
