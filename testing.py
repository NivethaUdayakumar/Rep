import os, itertools, pandas as pd
import numpy as np

def export_combinations_and_summary(
    df: pd.DataFrame,
    filter_cols: list[str],
    outdir: str = "combo_exports",
    summary_csv: str = "summary_counts.csv",
    export_filtered_csvs: bool = True,
):
    """
    - Builds a summary of counts, min_slack, sum_slack for ALL combinations across filter_cols
      where each column's domain is its unique values plus 'all'.
    - Optionally writes each filtered subset to its own CSV.
    """

    os.makedirs(outdir, exist_ok=True)

    # Value domain for each filter column: unique + 'all'
    value_lists = [sorted(df[c].dropna().unique().tolist()) + ["all"] for c in filter_cols]

    # Ensure slack is numeric
    df["slack"] = pd.to_numeric(df["slack"], errors="coerce")

    # Pre-compute group aggregations (fast single pass)
    agg = df.groupby(filter_cols, dropna=False).agg(
        rows=("slack", "size"),
        min_slack=("slack", "min"),
        sum_slack=("slack", "sum"),
    )
    agg = agg.sort_index()

    summary_rows = []

    for combo in itertools.product(*value_lists):
        filters = dict(zip(filter_cols, combo))

        # slicer: exact value or slice(None) for 'all'
        slicer = tuple(slice(None) if v == "all" else v for v in combo)

        try:
            sub = agg.loc[slicer]
            if isinstance(sub, pd.Series):  # single row
                row_count = int(sub["rows"])
                min_sl = sub["min_slack"]
                sum_sl = sub["sum_slack"]
            else:
                row_count = int(sub["rows"].sum())
                min_sl = sub["min_slack"].min()
                sum_sl = sub["sum_slack"].sum()
        except KeyError:
            row_count, min_sl, sum_sl = 0, np.nan, 0.0

        summary_rows.append({**filters,
                             "rows": row_count,
                             "min_slack": min_sl,
                             "sum_slack": sum_sl})

        # Optionally export the filtered subset
        if export_filtered_csvs and row_count > 0:
            subset = df
            for c, v in filters.items():
                if v != "all":
                    subset = subset[subset[c] == v]
            fname_parts = [f"{c}-{str(v).replace('/','_')}" for c, v in filters.items()]
            out_path = os.path.join(outdir, "_".join(fname_parts) + ".csv")
            subset.to_csv(out_path, index=False)

    # Build summary dataframe
    summary_df = pd.DataFrame(summary_rows)
    summary_df = summary_df[filter_cols + ["rows", "min_slack", "sum_slack"]]
    summary_df = summary_df.sort_values(by=filter_cols, ascending=[True]*len(filter_cols))

    # Save summary
    summary_df.to_csv(os.path.join(outdir, summary_csv), index=False)
    return summary_df

# --------- Example wiring ---------
if __name__ == "__main__":
    df = pd.read_csv(r"C:\Users\nivet\Downloads\master_data.csv")
    # Choose the 5 columns to generate combinations over
    filter_cols = ["path_last", "endpoint", "beginpoint", "pathgroup", "corner"]
    summary = export_combinations_and_summary(
        df,
        filter_cols,
        outdir="PARSED",
        summary_csv="summary_counts.csv",
        export_filtered_csvs=True,  # set False if you only want the summary
    )
    print("Summary preview:")
    print(summary.head())