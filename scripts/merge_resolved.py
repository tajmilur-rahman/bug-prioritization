import pandas as pd
from pathlib import Path

clean_dir = Path("data/clean")
out_file = Path("data/bugs_resolved.csv")

dfs = []
for f in sorted(clean_dir.glob("bugs_clean_*.csv")):
    df = pd.read_csv(f)
    # filter resolved
    df = df[df["status"] == "RESOLVED"]
    dfs.append(df)

merged = pd.concat(dfs, ignore_index=True)
merged.to_csv(out_file, index=False)

print(f"Merged {len(merged)} resolved bugs -> {out_file}")
