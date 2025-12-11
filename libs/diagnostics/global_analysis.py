import numpy as np

def global_feature_ig(records):
    """
    Averages |IG| per feature across all samples.
    records: output from run_batch_ig()
    """
    igs = [np.abs(rec["ig_pred"]) for rec in records]
    return np.mean(igs, axis=0)


def global_block_ig(records, block_schema):
    """
    Computes mean |IG| for each block across misclassifications.
    """
    block_vals = {blk["name"]: [] for blk in block_schema}

    for rec in records:
        ig = np.abs(rec["ig_pred"])
        for blk in block_schema:
            s,e = blk["start"], blk["end"]
            block_vals[blk["name"]].append(np.sum(ig[s:e]))

    return {name: float(np.mean(vals)) for name, vals in block_vals.items()}


def per_class_ig(records, block_schema):
    """
    Computes block-level IG per true class.
    """
    out = {}
    for rec in records:
        clazz = rec["true"]
        if clazz not in out:
            out[clazz] = {blk["name"]: [] for blk in block_schema}

        ig = np.abs(rec["ig_pred"])
        for blk in block_schema:
            s,e = blk["start"], blk["end"]
            out[clazz][blk["name"]].append(np.sum(ig[s:e]))

    # average
    for c in out:
        for blk in out[c]:
            out[c][blk] = float(np.mean(out[c][blk]))

    return out
