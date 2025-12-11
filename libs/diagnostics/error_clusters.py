import numpy as np
from collections import defaultdict


def cluster_misclassifications(records):
    """
    Groups IG results by (true, pred) pairs.
    """
    clusters = defaultdict(list)

    for rec in records:
        t = rec["true"]
        p = rec["pred"]
        key = f"{t}->{p}"
        clusters[key].append(rec)

    return clusters


def summarize_cluster(cluster, block_schema):
    """
    Computes avg block-level IG for errors in this cluster.
    """
    import numpy as np
    from .block_summary import blockwise_ig

    if len(cluster) == 0:
        return {}

    agg = None

    for rec in cluster:
        bi = blockwise_ig(rec["ig_pred"], block_schema)
        if agg is None:
            agg = {k:0.0 for k in bi}
        for k, v in bi.items():
            agg[k] += v

    # average
    for k in agg:
        agg[k] /= len(cluster)

    return agg
