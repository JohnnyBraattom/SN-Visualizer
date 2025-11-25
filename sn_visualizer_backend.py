
"""Backend logic for SN Visualizer: keyword export analysis and Excel enrichment."""

import io
import re
from typing import List, Dict, Any, Tuple, Optional

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


def _extract_domain_from_traffic_col(col_name: str) -> str:
    """Extract domain part from a traffic column header."""
    name = str(col_name)
    return name.split("Estimated Monthly Traffic")[0].strip()


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

    # Map traffic columns to domain-friendly names
    traffic_domain_map = {
        c: _extract_domain_from_traffic_col(c) for c in traffic_cols
    }

    return {
        "traffic_cols": traffic_cols,
        "client_traffic_col": client_traffic_col,
        "segment_cols": segment_cols,
        "month_cols_sorted": month_cols_sorted,
        "last_24_month_cols": last_24_month_cols,
        "traffic_domain_map": traffic_domain_map,
    }


def compute_competitive(df: pd.DataFrame, traffic_cols: List[str]) -> pd.DataFrame:
    """Sum estimated traffic per domain (each traffic column)."""
    rows = []
    for col in traffic_cols:
        domain = _extract_domain_from_traffic_col(col)
        total_traffic = df[col].fillna(0).astype(float).sum()
        rows.append({"Domain": domain, "Estimated Monthly Traffic": total_traffic})
    result = pd.DataFrame(rows)
    result = result.sort_values("Estimated Monthly Traffic", ascending=False).reset_index(drop=True)
    return result


def compute_segments(df: pd.DataFrame, segment_cols: List[str], traffic_cols: List[str]) -> pd.DataFrame:
    """
    For each segment column:
    - Compute Traffic Potential (Search Volume * 0.3) for rows where the column is non-empty.
    - Sum Estimated Monthly Traffic for each traffic column for the same rows.
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

        search_volume_sum = df.loc[mask, "Search Volume"].fillna(0).astype(float).sum()
        row = {
            "Segment": str(seg),
            "Traffic Potential": search_volume_sum * 0.3,
        }

        # Add traffic per domain
        for traffic_col in traffic_cols:
            domain = _extract_domain_from_traffic_col(traffic_col)
            value = df.loc[mask, traffic_col].fillna(0).astype(float).sum()
            row[domain] = value

        rows.append(row)

    result = pd.DataFrame(rows)
    if not result.empty:
        result = result.sort_values("Traffic Potential", ascending=False).reset_index(drop=True)
    return result


def compute_seasonality(df: pd.DataFrame, month_cols: List[str]) -> pd.DataFrame:
    """Total search volume per month across all keywords."""
    rows = []
    for col in month_cols:
        total = df[col].fillna(0).astype(float).sum()
        rows.append({"Month": str(col), "Total Search Volume": total})
    result = pd.DataFrame(rows)
    return result


def compute_aio_summary(df: pd.DataFrame) -> pd.DataFrame:
    """Summarise AIO presence as counts and percentages."""
    if "AIO Present" not in df.columns:
        return pd.DataFrame()

    series = df["AIO Present"].astype(str).str.strip().str.lower()
    total = len(series)
    if total == 0:
        return pd.DataFrame()

    rows = []
    for label, condition in [
        ("AIO triggered", series == "yes"),
        ("AIO not triggered", series == "no"),
    ]:
        count = int(condition.sum())
        if count == 0:
            continue
        percentage = (count / total) * 100.0
        rows.append(
            {
                "Status": label,
                "Count": count,
                "Percentage": percentage,
            }
        )

    return pd.DataFrame(rows)


def compute_category_breakdown(df: pd.DataFrame, category_col: str, client_traffic_col: str) -> pd.DataFrame:
    """
    For a given category column, compute traffic potential and client traffic breakdown per tag.

    Traffic Potential = Search Volume * 0.3.

    Returns columns: Tag, Keyword_Count, Traffic_Potential, Client_Traffic.
    """
    if "Search Volume" not in df.columns:
        raise ValueError("Column 'Search Volume' not found in Keywords sheet.")
    if category_col not in df.columns:
        raise ValueError(f"Category column '{category_col}' not found in dataframe.")
    if client_traffic_col not in df.columns:
        raise ValueError(f"Client traffic column '{client_traffic_col}' not found in dataframe.")

    series = df[category_col]
    mask = series.notna() & (series.astype(str).str.strip() != "")
    if not mask.any():
        return pd.DataFrame(columns=["Tag", "Keyword_Count", "Traffic_Potential", "Client_Traffic"])

    temp = df.loc[mask].copy()
    temp["__tag__"] = temp[category_col].astype(str).str.strip()

    if "Keyword" in temp.columns:
        keyword_col_for_count = "Keyword"
    else:
        keyword_col_for_count = "__tag__"

    agg = (
        temp.groupby("__tag__")
        .agg(
            Keyword_Count=(keyword_col_for_count, "size"),
            Search_Volume=("Search Volume", "sum"),
            Client_Traffic=(client_traffic_col, "sum"),
        )
        .reset_index()
        .rename(columns={"__tag__": "Tag"})
    )

    agg["Search_Volume"] = agg["Search_Volume"].astype(float)
    agg["Client_Traffic"] = agg["Client_Traffic"].fillna(0).astype(float)
    agg["Traffic_Potential"] = agg["Search_Volume"] * 0.3
    agg = agg.drop(columns=["Search_Volume"])
    agg = agg.sort_values("Traffic_Potential", ascending=False).reset_index(drop=True)
    return agg



def compute_aio_visibility(df: pd.DataFrame, traffic_domain_map: Optional[Dict[str, str]] = None) -> pd.DataFrame:
    """
    Compute AIO visibility per domain.

    - Only considers rows where AIO Present == 'yes' (case-insensitive).
    - For each domain AIO Rank column (* AIO Rank):
        - Converts rank to a weight (1 -> 1.0, 2 -> 0.8, 3 -> 0.5, >=4 -> 0.1).
        - Adds Search Volume * weight to that domain's visibility score.
        - Tracks in how many unique keywords the domain appears in AIO.
    - Theoretical maximum is: sum of Search Volume over all AIO-triggering keywords
      (i.e. assuming a domain could own rank 1 for every AIO keyword).
    - If traffic_domain_map is provided, all traffic domains will be included in the
      output, even if they have zero AIO visibility.
    """
    if "AIO Present" not in df.columns or "Search Volume" not in df.columns:
        return pd.DataFrame()

    series_aio = df["AIO Present"].astype(str).str.strip().str.lower()
    mask_aio = series_aio == "yes"

    # Data restricted to AIO-triggering keywords
    df_aio = df.loc[mask_aio].copy()

    # Identify AIO rank columns
    rank_cols = [c for c in df.columns if str(c).endswith(" AIO Rank")]

    visibility_scores: Dict[str, float] = {}
    keyword_sets: Dict[str, set] = {}

    # Compute weighted visibility scores only where we have AIO-triggering rows and rank columns
    if not df_aio.empty and rank_cols:
        for _, row in df_aio.iterrows():
            vol_val = row.get("Search Volume", 0)
            try:
                vol = float(vol_val) if pd.notna(vol_val) else 0.0
            except Exception:
                vol = 0.0
            if vol <= 0:
                continue

            keyword = str(row.get("Keyword", ""))

            for col in rank_cols:
                rank_val = row.get(col)
                if pd.isna(rank_val):
                    continue
                try:
                    rank = int(rank_val)
                except Exception:
                    continue
                if rank <= 0:
                    continue

                if rank == 1:
                    weight = 1.0
                elif rank == 2:
                    weight = 0.8
                elif rank == 3:
                    weight = 0.5
                else:
                    weight = 0.1

                domain = str(col).rsplit(" AIO Rank", 1)[0].strip()
                if not domain:
                    continue

                visibility_scores[domain] = visibility_scores.get(domain, 0.0) + vol * weight
                keyword_sets.setdefault(domain, set()).add(keyword)

    # Theoretical maximum: sum of Search Volume across all AIO-triggering keywords
    theoretical_max = 0.0
    if not df_aio.empty:
        for _, row in df_aio.iterrows():
            vol_val = row.get("Search Volume", 0)
            try:
                vol = float(vol_val) if pd.notna(vol_val) else 0.0
            except Exception:
                vol = 0.0
            if vol <= 0:
                continue
            theoretical_max += vol

    if theoretical_max <= 0:
        # Avoid division by zero; scores will be zero anyway.
        theoretical_max = 1.0

    # Collect all domains we should show:
    # - Any domain that appeared in AIO ranks (visibility_scores keys)
    # - Any domain inferred from rank column names
    # - Any traffic domain from traffic_domain_map
    domains = set(visibility_scores.keys())

    for col in rank_cols:
        domain = str(col).rsplit(" AIO Rank", 1)[0].strip()
        if domain:
            domains.add(domain)

    if traffic_domain_map:
        for domain in traffic_domain_map.values():
            if domain:
                domains.add(str(domain))

    if not domains:
        return pd.DataFrame()

    rows = []
    for domain in sorted(domains):
        score = visibility_scores.get(domain, 0.0)
        unique_kw = len(keyword_sets.get(domain, set()))
        share_pct = (score / theoretical_max) * 100.0 if theoretical_max > 0 else 0.0
        rows.append(
            {
                "Domain": domain,
                "Unique AIO Keywords": unique_kw,
                "AIO Visibility (Weighted Volume)": score,
                "AIO Visibility Score (%)": share_pct,
            }
        )

    result = pd.DataFrame(rows)
    result = result.sort_values("AIO Visibility (Weighted Volume)", ascending=False).reset_index(drop=True)
    return result


def compute_aio_segments(df: pd.DataFrame, segment_cols: List[str]) -> pd.DataFrame:
    """
    For each segment column, restricted to AIO-triggering keywords, compute:
    - AIO_Keyword_Count: number of AIO-triggering keywords with a non-empty value in the segment.
    - AIO_Theoretical_Max: sum of Search Volume for those keywords.
    """
    if "AIO Present" not in df.columns or "Search Volume" not in df.columns:
        return pd.DataFrame()
    if not segment_cols:
        return pd.DataFrame()

    series_aio = df["AIO Present"].astype(str).str.strip().str.lower()
    mask_aio = series_aio == "yes"
    if not mask_aio.any():
        return pd.DataFrame()

    df_aio = df.loc[mask_aio].copy()

    rows = []
    for seg in segment_cols:
        if seg not in df_aio.columns:
            continue
        series = df_aio[seg]
        mask = series.notna() & (series.astype(str).str.strip() != "")
        if not mask.any():
            continue

        subset = df_aio.loc[mask].copy()
        kw_count = len(subset)

        sv_sum = subset["Search Volume"].fillna(0).astype(float).sum()
        rows.append(
            {
                "Segment": str(seg),
                "AIO_Keyword_Count": int(kw_count),
                "AIO_Theoretical_Max": float(sv_sum),
            }
        )

    if not rows:
        return pd.DataFrame()

    result = pd.DataFrame(rows)
    result = result.sort_values("AIO_Theoretical_Max", ascending=False).reset_index(drop=True)
    return result


def compute_aio_category_breakdown(df: pd.DataFrame, category_col: str) -> pd.DataFrame:
    """
    For a given category column, restricted to AIO-triggering keywords, compute:
    - AIO_Keyword_Count per tag.
    - AIO_Theoretical_Max per tag (sum of Search Volume for AIO-triggering keywords).
    """
    if "AIO Present" not in df.columns or "Search Volume" not in df.columns:
        return pd.DataFrame(columns=["Tag", "AIO_Keyword_Count", "AIO_Theoretical_Max"])
    if category_col not in df.columns:
        return pd.DataFrame(columns=["Tag", "AIO_Keyword_Count", "AIO_Theoretical_Max"])

    series_aio = df["AIO Present"].astype(str).str.strip().str.lower()
    mask_aio = series_aio == "yes"

    series_cat = df[category_col]
    mask_cat = series_cat.notna() & (series_cat.astype(str).str.strip() != "")

    mask = mask_aio & mask_cat
    if not mask.any():
        return pd.DataFrame(columns=["Tag", "AIO_Keyword_Count", "AIO_Theoretical_Max"])

    temp = df.loc[mask].copy()
    temp["__tag__"] = temp[category_col].astype(str).str.strip()

    if "Keyword" in temp.columns:
        keyword_col_for_count = "Keyword"
    else:
        keyword_col_for_count = "__tag__"

    grouped = temp.groupby("__tag__", dropna=False)

    rows = []
    for tag, grp in grouped:
        kw_count = grp[keyword_col_for_count].nunique() if keyword_col_for_count in grp.columns else len(grp)
        sv_sum = grp["Search Volume"].fillna(0).astype(float).sum()
        rows.append(
            {
                "Tag": str(tag),
                "AIO_Keyword_Count": int(kw_count),
                "AIO_Theoretical_Max": float(sv_sum),
            }
        )

    result = pd.DataFrame(rows)
    result = result.sort_values("AIO_Theoretical_Max", ascending=False).reset_index(drop=True)
    return result


def build_enriched_excel(
    df_keywords: pd.DataFrame,
    competitive_df: pd.DataFrame,
    segment_df: pd.DataFrame,
    seasonality_df: pd.DataFrame,
    aio_summary_df: Optional[pd.DataFrame] = None,
    aio_visibility_df: Optional[pd.DataFrame] = None,
    category_breakdowns: Optional[Dict[str, pd.DataFrame]] = None,
    aio_segments_df: Optional[pd.DataFrame] = None,
    aio_category_breakdowns: Optional[Dict[str, pd.DataFrame]] = None,
) -> bytes:
    """Build an in-memory Excel file with original plus summary sheets."""
    output = io.BytesIO()
    with pd.ExcelWriter(output, engine="openpyxl") as writer:
        # Original keywords
        df_keywords.to_excel(writer, sheet_name="Keywords", index=False)

        # Core summaries
        competitive_df.to_excel(writer, sheet_name="Summary_Competitive", index=False)
        segment_df.to_excel(writer, sheet_name="Summary_TrafficPotential", index=False)
        seasonality_df.to_excel(writer, sheet_name="Summary_Seasonality", index=False)

        # AIO summaries
        if aio_summary_df is not None and not aio_summary_df.empty:
            aio_summary_df.to_excel(writer, sheet_name="Summary_AIO", index=False)

        if aio_visibility_df is not None and not aio_visibility_df.empty:
            aio_visibility_df.to_excel(writer, sheet_name="Summary_AIO_Visibility", index=False)

        if aio_segments_df is not None and not aio_segments_df.empty:
            aio_segments_df.to_excel(writer, sheet_name="Summary_AIO_Segments", index=False)

        # Category-level breakdowns (traffic / client)
        if category_breakdowns:
            for category, df_cat in category_breakdowns.items():
                sheet_name = f"Cat_{str(category)[:25]}"
                df_cat.to_excel(writer, sheet_name=sheet_name, index=False)

        # Category-level AIO breakdowns
        if aio_category_breakdowns:
            for category, df_cat in aio_category_breakdowns.items():
                sheet_name = f"AIO_Cat_{str(category)[:20]}"
                df_cat.to_excel(writer, sheet_name=sheet_name, index=False)

    output.seek(0)
    return output.getvalue()
