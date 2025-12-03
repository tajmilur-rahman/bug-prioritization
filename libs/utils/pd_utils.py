"""
pd_utils.py — High-reliability DataFrame utilities for the Bug Triage pipeline.

Includes:
  - normalize_indices
  - assign_positional (strict length checks)
  - ensure_numeric / ensure_categorical
  - safe_merge (enforces row integrity)
  - validate_embeddings
  - reorder_columns
"""

import pandas as pd
import numpy as np
from typing import Tuple, Dict, List, Optional


# -------------------------------------------------------------------
# 1. Normalize index to RangeIndex
# -------------------------------------------------------------------
def normalize_indices(*dfs: pd.DataFrame | pd.Series) -> Tuple:
    """
    Ensures all incoming objects have RangeIndex starting at zero.
    Avoids pandas alignment bugs in assignment and concat.
    """
    out = []
    for obj in dfs:
        if isinstance(obj, (pd.DataFrame, pd.Series)):
            out.append(obj.reset_index(drop=True))
        else:
            out.append(obj)
    return tuple(out)


# -------------------------------------------------------------------
# 2. Strict (safe) positional assignment
# -------------------------------------------------------------------
def assign_positional(df: pd.DataFrame, **cols: pd.Series | np.ndarray) -> None:
    """
    Positional-safe assignment:
      assign_positional(df, summary_len=arr)

    Ensures:
      - length matches df.shape[0]
      - dtype is preserved (no implicit casting)
      - raises hard error if mismatch

    Prevents silent overwriting bugs from pandas alignment rules.
    """
    n = len(df)
    for name, col in cols.items():
        if isinstance(col, pd.Series):
            col = col.to_numpy()

        col = np.asarray(col)
        if len(col) != n:
            raise ValueError(
                f"assign_positional Error: column '{name}' length {len(col)} "
                f"does not match df length {n}."
            )

        df[name] = col


# -------------------------------------------------------------------
# 3. Ensure numeric / categorical validity
# -------------------------------------------------------------------
def ensure_numeric(df: pd.DataFrame, cols: List[str]) -> pd.DataFrame:
    """
    Cast selected columns to float32 safely.
    Raises error if casting fails.
    """
    for c in cols:
        if c not in df.columns:
            continue
        try:
            df[c] = pd.to_numeric(df[c], errors="raise").astype("float32")
        except Exception as e:
            raise ValueError(f"Column '{c}' cannot be converted to numeric: {e}")
    return df


def ensure_categorical(df: pd.DataFrame, cols: List[str]) -> pd.DataFrame:
    """
    Ensure selected columns are string categories.
    """
    for c in cols:
        if c not in df.columns:
            continue
        df[c] = df[c].astype(str)
    return df


# -------------------------------------------------------------------
# 4. Safe DataFrame merge (prevents data corruption)
# -------------------------------------------------------------------
def safe_merge(
    left: pd.DataFrame,
    right: pd.DataFrame,
    on: str = "id",
    how: str = "left",
    check_unique: bool = True,
    allow_extra_rows: bool = False,
) -> pd.DataFrame:
    """
    Safer version of merge():
      - checks duplicate keys
      - checks for unexpected row increases
      - checks for new NaN introduction
    """
    if check_unique:
        if right[on].duplicated().any():
            dups = right[right[on].duplicated()][on].head()
            raise ValueError(f"safe_merge Error: Duplicate keys in right dataframe: {dups}")

    before = len(left)
    out = left.merge(right, on=on, how=how, sort=False, validate=None)

    after = len(out)
    if not allow_extra_rows and after > before:
        raise ValueError(
            f"safe_merge Error: Row count increased from {before} to {after}, "
            f"unexpected for left merge."
        )

    return out


# -------------------------------------------------------------------
# 5. Validate embedding vector column integrity
# -------------------------------------------------------------------
def validate_embeddings(df: pd.DataFrame, col: str = "embedding") -> Tuple[int, int]:
    """
    Ensures:
      - column exists
      - each row holds a numpy array
      - all arrays have identical dims
      - no NaN values
    Returns: (n_rows, emb_dim)
    """
    if col not in df.columns:
        raise ValueError(f"validate_embeddings Error: Column '{col}' missing.")

    dims = []
    for i, row in enumerate(df[col]):
        if not isinstance(row, np.ndarray):
            raise ValueError(f"Row {i} in '{col}' is not a numpy array.")
        if row.ndim != 1:
            raise ValueError(f"Row {i} in '{col}' is not 1D. Shape={row.shape}")
        if np.isnan(row).any():
            raise ValueError(f"Row {i} in '{col}' contains NaN values.")
        dims.append(len(row))

    if len(set(dims)) != 1:
        raise ValueError(f"Embedding dimension mismatch detected: {set(dims)}")

    return len(df), dims[0]


# -------------------------------------------------------------------
# 6. Reorder columns in a predictable order
# -------------------------------------------------------------------
def reorder_columns(df: pd.DataFrame, groups: Dict[str, List[str]]) -> pd.DataFrame:
    """
    Reorders columns based on ordered groups.

    Example:
      reorder_columns(df, groups={
          "raw": ["id", "summary", "description"],
          "engineered": ["summary_len", "desc_len"],
          "embedding": ["embedding"],
          "topics": ["topic_id_B_clean"]
      })
    """
    ordered = []
    for grp, cols in groups.items():
        for c in cols:
            if c in df.columns:
                ordered.append(c)

    # add any remaining unspecified columns at the end
    remainder = [c for c in df.columns if c not in ordered]
    return df[ordered + remainder]
