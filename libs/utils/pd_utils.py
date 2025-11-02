import pandas as pd
from typing import Tuple

def normalize_indices(*dfs: pd.DataFrame | pd.Series) -> Tuple:
    """
    Return copies of inputs with RangeIndex(0..n-1) so assignments are positional-safe.
    Works for both DataFrames and Series.
    """
    out = []
    for obj in dfs:
        if isinstance(obj, (pd.DataFrame, pd.Series)):
            out.append(obj.reset_index(drop=True))
        else:
            out.append(obj)
    return tuple(out)

def assign_positional(df: pd.DataFrame, **cols: pd.Series) -> None:
    """
    Assign columns by position (no index alignment).
    Usage: assign_positional(out, priority=part["priority"], foo=part["foo"])
    """
    for k, v in cols.items():
        df[k] = getattr(v, "to_numpy", lambda: v)()  # Series->ndarray; passthrough for arrays
