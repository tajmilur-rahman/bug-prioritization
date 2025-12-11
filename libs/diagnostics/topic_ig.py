import numpy as np

def topic_block_from_schema(block_schema):
    """
    Returns (start, end) of topic block.
    Assumes block name contains 'topic'.
    """
    for blk in block_schema:
        if "topic" in blk["name"].lower():
            return blk["start"], blk["end"]
    return None


def compute_topic_ig(records, block_schema):
    """
    Computes IG contribution of each topic dimension.
    """
    tspan = topic_block_from_schema(block_schema)
    if tspan is None:
        return {}

    s,e = tspan
    out = []

    for rec in records:
        ig = np.abs(rec["ig_pred"])
        out.append(ig[s:e])

    return np.mean(np.stack(out), axis=0)
