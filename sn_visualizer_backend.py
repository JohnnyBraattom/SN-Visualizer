
"""Backend logic for SN Visualizer: keyword export analysis and Excel enrichment."""

import io
import re
from typing import List, Dict, Any, Tuple

import pandas as pd


MONTH_PATTERN = re.compile(r"^(Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)\s(\d{2})$")
MONTH_NAME_TO_NUM = {
    "Jan": 1, "Feb": 2, "Mar": 3, "Apr": 4,
    "May": 5, "Jun": 6, "Jul": 7, "Aug": 8,
    "Sep": 9, "Oct": 10, "Nov": 11, "Dec": 12,
}


def load_keywords_dataframe(file_obj) -> pd.DataFrame:
    """Load the 'Keywords' sheet from the uploaded Excel file."""
    try:
        xls = pd.ExcelFile(file_obj)
    except Exception as exc:
        raise ValueError(f"Unable to read Excel file: {exc}") from exc

    if "Keywords" not in xls.sheet_names:
        raise ValueError("No 'Keywords' sheet found in the uploaded file.")

    df = pd.read_excel(xls, sheet_name="Keywords")
    return df


def _find_topic_index(columns: List[str]) -> int:
    for i, c in enumerate(columns):
        if str(c).strip().lower() == "topic":
            return i
    raise ValueError("Column 'Topic' not found in Keywords sheet.")


def _parse_month_col(col_name: str) -> Tuple[int, int]:
    """Parse column like 'Oct 24' to (year, month)."""
    m = MONTH_PATTERN.match(str(col_name).strip())
    if not m:
        raise ValueError(f"Column name does not match month pattern: {col_name}")
    month_str, year_suffix = m.groups()
    month = MONTH_NAME_TO_NUM[month_str]
    year_suffix_int = int(year_suffix)
    # Assume 2000-2099 range
    year = 2000 + year_suffix_int
    return year, month


def detect_columns(df: pd.DataFrame) -> Dict[str, Any]:
    """Detect traffic, segment, and monthly columns based on agreed rules."""
    cols = list(df.columns)

    # Traffic columns
    traffic_cols = [c for c in cols if "Estimated Monthly Traffic" in str(c)]
    if not traffic_cols:
        raise ValueError("No 'Estimated Monthly Traffic' columns found.")
    client_traffic_col = traffic_cols[0]

    # Monthly columns
    month_cols = [c for c in cols if MONTH_PATTERN.match(str(c).strip())]
    if not month_cols:
        raise ValueError("No monthly search volume columns found (e.g. 'Oct 24').")

    # Sort month columns chronologically
    month_cols_sorted = sorted(
        month_cols,
        key=lambda c: _parse_month_col(str(c).strip()),
    )

    # Topic -> first month = segment columns
    topic_idx = _find_topic_index(cols)
    first_month_idx = min(cols.index(c) for c in month_cols)
    if first_month_idx <= topic_idx:
        raise ValueError("First month column appears before 'Topic' column, which is unexpected.")

    segment_cols = cols[topic_idx:first_month_idx]

    # Last 24 months (or fewer if not available)
    if len(month_cols_sorted) > 24:
        last_24_month_cols = month_cols_sorted[-24:]
    else:
        last_24_month_cols = month_cols_sorted

    return {
        "traffic_cols": traffic_cols,
        "client_traffic_col": client_traffic_col,
        "segment_cols": segment_cols,
        "month_cols_sorted": month_cols_sorted,
        "last_24_month_cols": last_24_month_cols,
    }


def compute_competitive(df: pd.DataFrame, traffic_cols: List[str]) -> pd.DataFrame:
    """Sum estimated traffic per domain (each traffic column)."""
    rows = []
    for col in traffic_cols:
        # Domain name = everything before 'Estimated Monthly Traffic'
        name = str(col)
        domain = name.split("Estimated Monthly Traffic")[0].strip()
        total_traffic = df[col].fillna(0).astype(float).sum()
        rows.append({"Domain": domain, "Estimated Monthly Traffic": total_traffic})
    result = pd.DataFrame(rows)
    result = result.sort_values("Estimated Monthly Traffic", ascending=False).reset_index(drop=True)
    return result


def compute_segments(df: pd.DataFrame, segment_cols: List[str], client_traffic_col: str) -> pd.DataFrame:
    """
    For each segment column:
    - Sum Search Volume for rows where the column is non-empty.
    - Sum client Estimated Monthly Traffic for the same rows.
    """
    if "Search Volume" not in df.columns:
        raise ValueError("Column 'Search Volume' not found in Keywords sheet.")

    rows = []
    for seg in segment_cols:
        series = df[seg]
        mask = series.notna() & (series.astype(str).str.strip() != "")
        if not mask.any():
            # Skip completely empty segment columns to avoid noise
            continue
        seg_volume = df.loc[mask, "Search Volume"].fillna(0).astype(float).sum()
        seg_client_traffic = df.loc[mask, client_traffic_col].fillna(0).astype(float).sum()
        rows.append(
            {
                "Segment": str(seg),
                "Search Volume": seg_volume,
                "Client Estimated Monthly Traffic": seg_client_traffic,
            }
        )
    result = pd.DataFrame(rows)
    if not result.empty:
        result = result.sort_values("Search Volume", ascending=False).reset_index(drop=True)
    return result


def compute_seasonality(df: pd.DataFrame, month_cols: List[str]) -> pd.DataFrame:
    """Total search volume per month across all keywords."""
    rows = []
    for col in month_cols:
        total = df[col].fillna(0).astype(float).sum()
        rows.append({"Month": str(col), "Total Search Volume": total})
    result = pd.DataFrame(rows)
    return result


def build_enriched_excel(
    df_keywords: pd.DataFrame,
    competitive_df: pd.DataFrame,
    segment_df: pd.DataFrame,
    seasonality_df: pd.DataFrame,
) -> bytes:
    """Build an in-memory Excel file with original plus summary sheets."""
    output = io.BytesIO()
    with pd.ExcelWriter(output, engine="openpyxl") as writer:
        df_keywords.to_excel(writer, sheet_name="Keywords", index=False)
        competitive_df.to_excel(writer, sheet_name="Summary_Competitive", index=False)
        segment_df.to_excel(writer, sheet_name="Summary_Segments", index=False)
        seasonality_df.to_excel(writer, sheet_name="Summary_Seasonality", index=False)
    output.seek(0)
    return output.getvalue()
