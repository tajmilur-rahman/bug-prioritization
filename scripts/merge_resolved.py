import pandas as pd
from pathlib import Path
import argparse


KNOWN_PRIORITIES = ["P1", "P2", "P3", "P4", "P5",
                    "Blocker", "Critical", "Major",
                    "Normal", "Minor", "Trivial"]

def clean_training_data(df: pd.DataFrame,
                        label_col: str = "priority",
                        max_missing_frac: float = 0.3):
    """Drop rows with excessive missing values or invalid labels."""
    n0 = len(df)

    # 1️⃣ Drop rows with too many missing features
    miss_frac = df.isna().mean(axis=1)
    df = df.loc[miss_frac <= max_missing_frac].copy()
    n1 = len(df)
    dropped = n0 - n1
    print(f"[clean_training_data] Dropped {dropped} / {n0} rows "
          f"({dropped/n0:.1%}) with invalid/missing data.")

    # 2️⃣ Drop rows with invalid labels
    df[label_col] = df[label_col].astype(str).str.strip()
    df = df.loc[df[label_col] != ""]

    df = df.loc[df[label_col] != "nan"] 

    df = df.loc[df[label_col] != "--"]
    
    #df = df.loc[df[label_col].isin(KNOWN_PRIORITIES)]

    n1 = len(df)
    dropped = n0 - n1
    print(f"[clean_training_data] Dropped {dropped} / {n0} rows "
          f"({dropped/n0:.1%}) with invalid/missing labels.")
    return df.reset_index(drop=True)

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--input", default="data/clean")
    ap.add_argument("--output", default="data/bugs_resolved.csv")
    ap.add_argument("--resolved", choices=["true","false"], default="true")
    args = ap.parse_args()

    clean_dir = Path(args.input)
    out_file = args.output
    resolved = args.resolved.lower() == "true"

    dfs = []
    classes = []
    unresolved = 0
    invalid = 0
    n0 = 0
    for f in sorted(clean_dir.glob("bugs_clean_*.csv")):
        df = pd.read_csv(f)
        # filter resolved
        n0 += len(df)
        if resolved:
            n1 = len(df)
            df = df[df["status"] == "RESOLVED"]
            n2 = len(df)
            unresolved += (n1 - n2)

        n3 = len(df)    
        df = clean_training_data(df, label_col="priority", max_missing_frac=0.6)
        invalid += n3 - len(df)
        dfs.append(df)
        present = pd.unique(pd.concat([pd.Series(df["priority"]), pd.Series(classes)], ignore_index=True)) 
        order = [c for c in KNOWN_PRIORITIES  if c in present]
        extras = sorted([c for c in present if c not in set(KNOWN_PRIORITIES)])
        classes = order + extras

    merged = pd.concat(dfs, ignore_index=True)
    merged.to_csv(out_file, index=False)

    print("Classes: ", classes)
    print(f"Dropped {unresolved} / {n0} rows "
                    f"({unresolved/n0:.1%}) for UNRESOLVED.")
    print(f"Dropped {invalid} / {n0} rows "
                    f"({invalid/n0:.1%}) with invalid/missing data or labels.")
    print(f"Merged {len(merged)} bugs with valid labels -> {out_file}")

if __name__ == "__main__":
    main()