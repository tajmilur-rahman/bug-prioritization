import os

def check_time_colunm(df, time_col):
    if time_col not in df.columns:
        raise SystemExit(f"TIME_COL={time_col} missing from input")


def make_time_split(df, label_col):
    time_col = os.getenv("TIME_COL", "creation_time")
    check_time_colunm(df, time_col=time_col)

    if label_col.startswith("severity"):
        val_frac = 0.20
        test_frac = 0.20
    else:
        val_frac = float(os.getenv("VAL_FRAC", "0.10"))
        test_frac = float(os.getenv("TEST_FRAC", "0.10"))

    df = df.sort_values(time_col).reset_index(drop=True)
    n = len(df)
    n_test = int(n * test_frac)
    n_val = int(n * val_frac)

    test_df = df.iloc[-n_test:] if n_test > 0 else df.iloc[:0]
    val_df = df.iloc[-n_test - n_val: -n_test] if n_val > 0 else df.iloc[:0]
    train_df = df.iloc[: -n_test - n_val] if (n_test + n_val) > 0 else df

    return train_df, val_df, test_df