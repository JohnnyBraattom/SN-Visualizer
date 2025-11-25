
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
    """Load the `Keywords` sheet from an uploaded Excel file-like object."""
    # Streamlit's uploaded_file can be passed directly to pandas
    df = pd.read_excel(file_obj, sheet_name="Keywords")
    # Normalize column names: strip whitespace
    df.columns = [str(c).strip() for c in df.columns]
    return df


def _find_topic_index(columns: List[str]) -> int:
    """Return index of 'Topic' column, or -1 if not found."""
    for idx, col in enumerate(columns):
        if str(col).strip().lower() == "topic":
            return idx
    return -1


def _parse_month_col(col_name: str) -> Tuple[int, int]:
    """
    Parse a month column like 'Oct 24' into (year, month).
    Assumes year is two digits 00–99 representing 2000–2099.
    """
    m = MONTH_PATTERN.match(str(col_name).strip())
    if not m:
        raise ValueError(f"Invalid month column format: {col_name!r}")
    month_name, yy = m.groups()
    month = MONTH_NAME_TO_NUM[month_name]
    year = 2000 + int(yy)
    return year, month


def _extract_domain_from_traffic_col(col_name: str) -> str:
    """
    Extract a readable domain label from a traffic column name.

    Examples:
        'Estimated Monthly Traffic - systembolaget.se' -> 'systembolaget.se'
        'Estimated Monthly Traffic (client.com)' -> 'client.com'
    """
    text = str(col_name)
    # After dash
    if " - " in text:
        return text.split(" - ", 1)[1].strip()
    # Inside parentheses
    if "(" in text and ")" in text:
        inner = text.split("(", 1)[1].rsplit(")", 1)[0]
        return inner.strip()
    # Fallback: return the whole column name
    return text.strip()


def detect_columns(df: pd.DataFrame) -> Dict[str, Any]:
    """Infer search volume, traffic, segment, and month columns."""
    cols = list(df.columns)

    # Search volume column
    search_volume_col = None
    for c in cols:
        if str(c).strip().lower() == "search volume":
            search_volume_col = c
            break

    # Traffic columns
    traffic_cols: List[str] = [
        c for c in cols if "estimated monthly traffic" in str(c).lower()
    ]
    client_traffic_col: Optional[str] = traffic_cols[0] if traffic_cols else None

    # Month columns
    month_cols = [c for c in cols if MONTH_PATTERN.match(str(c).strip())]
    month_cols_sorted = sorted(month_cols, key=_parse_month_col)

    # Segment columns: between Topic and first month column
    segment_cols: List[str] = []
    topic_idx = _find_topic_index(cols)
    if topic_idx != -1 and month_cols_sorted:
        first_month = month_cols_sorted[0]
        first_month_idx = cols.index(first_month)
        if first_month_idx > topic_idx + 1:
            segment_cols = cols[topic_idx + 1 : first_month_idx]

    # Last 24 months
    if len(month_cols_sorted) > 24:
        last_24_month_cols = month_cols_sorted[-24:]
    else:
        last_24_month_cols = month_cols_sorted

    traffic_domain_map: Dict[str, str] = {
        col: _extract_domain_from_traffic_col(col) for col in traffic_cols
    }

    return {
        "search_volume_col": search_volume_col,
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
        if col not in df.columns:
            continue
        domain = _extract_domain_from_traffic_col(col)
        total_traffic = df[col].fillna(0).astype(float).sum()
        rows.append({"Domain": domain, "Estimated Monthly Traffic": total_traffic})
    result = pd.DataFrame(rows)
    if not result.empty:
        result = result.sort_values("Estimated Monthly Traffic", ascending=False)
    return result


def compute_segments(
    df: pd.DataFrame,
    segment_cols: List[str],
    traffic_cols: List[str],
) -> pd.DataFrame:
    """
    Compute traffic potential and per-domain estimated traffic per segment.

    Assumes there is a 'Search Volume' column.
    Traffic potential = 0.3 * sum(Search Volume) per segment.
    """
    if "Search Volume" not in df.columns:
        return pd.DataFrame()

    rows: List[Dict[str, Any]] = []
    for seg_col in segment_cols:
        if seg_col not in df.columns:
            continue

        # Consider rows where this segment column is non-empty
        seg_df = df[df[seg_col].notna() & (df[seg_col].astype(str).str.strip() != "")]
        if seg_df.empty:
            continue

        # Each unique value within segment
        for value, group in seg_df.groupby(seg_col):
            if pd.isna(value) or str(value).strip() == "":
                continue

            sv = group["Search Volume"].fillna(0).astype(float).sum()
            traffic_potential = sv * 0.3
            row: Dict[str, Any] = {
                "Segment Column": seg_col,
                "Segment": value,
                "Traffic_Potential": traffic_potential,
            }

            # Add per-domain traffic sums
            for col in traffic_cols:
                if col not in group.columns:
                    continue
                domain = _extract_domain_from_traffic_col(col)
                domain_traffic = group[col].fillna(0).astype(float).sum()
                row[domain] = domain_traffic

            rows.append(row)

    result = pd.DataFrame(rows)
    if not result.empty:
        result = result.sort_values("Traffic_Potential", ascending=False)
    return result


def compute_seasonality(df: pd.DataFrame, month_cols: List[str]) -> pd.DataFrame:
    """Compute total search volume per month across all keywords."""
    month_cols = [c for c in month_cols if c in df.columns]
    if not month_cols:
        return pd.DataFrame()

    rows = []
    for col in month_cols:
        total_sv = df[col].fillna(0).astype(float).sum()
        rows.append({"Month": col, "Search Volume": total_sv})

    result = pd.DataFrame(rows)
    if not result.empty:
        # keep chronological order based on parsing helper
        result["__order"] = result["Month"].apply(lambda x: _parse_month_col(x))
        result = result.sort_values("__order").drop(columns="__order")
    return result


def _normalize_aio_present(series: pd.Series) -> pd.Series:
    """Return a boolean series indicating whether AIO is present for each row."""
    def _is_yes(val: Any) -> bool:
        if pd.isna(val):
            return False
        s = str(val).strip().lower()
        return s in {"yes", "y", "true", "1"}
    return series.map(_is_yes)


def compute_aio_summary(df: pd.DataFrame) -> pd.DataFrame:
    """Summarize how many keywords trigger AIO vs not."""
    if "AIO Present" not in df.columns:
        return pd.DataFrame()

    aio_bool = _normalize_aio_present(df["AIO Present"])
    total = len(df)
    if total == 0:
        return pd.DataFrame()

    aio_triggered = int(aio_bool.sum())
    not_aio = total - aio_triggered

    rows = [
        {"Status": "AIO triggered", "Count": aio_triggered},
        {"Status": "No AIO", "Count": not_aio},
    ]
    result = pd.DataFrame(rows)
    result["Percentage"] = (result["Count"] / total) * 100.0
    return result


def compute_category_breakdown(
    df: pd.DataFrame,
    category_col: str,
    client_traffic_col: str,
) -> pd.DataFrame:
    """
    Break down traffic potential and client traffic per tag in a category column.

    Only rows with non-empty category values are counted.
    Traffic potential = 0.3 * sum(Search Volume) per tag.
    """
    if "Search Volume" not in df.columns:
        return pd.DataFrame()
    if category_col not in df.columns:
        return pd.DataFrame()
    if client_traffic_col not in df.columns:
        return pd.DataFrame()

    cat_series = df[category_col].astype(str).str.strip()
    mask = cat_series != ""
    work_df = df[mask].copy()
    if work_df.empty:
        return pd.DataFrame()

    grouped = work_df.groupby(category_col)

    rows: List[Dict[str, Any]] = []
    for tag, group in grouped:
        sv = group["Search Volume"].fillna(0).astype(float).sum()
        traffic_potential = sv * 0.3
        client_traffic = group[client_traffic_col].fillna(0).astype(float).sum()
        rows.append(
            {
                category_col: tag,
                "Keyword_Count": len(group),
                "Traffic_Potential": traffic_potential,
                "Client_Traffic": client_traffic,
            }
        )

    result = pd.DataFrame(rows)
    if not result.empty:
        result = result.sort_values("Traffic_Potential", ascending=False)
    return result


def compute_aio_visibility(df: pd.DataFrame) -> pd.DataFrame:
    """
    Compute AIO visibility by domain.

    Domain list is inferred from traffic columns (Estimated Monthly Traffic ...).
    Visibility is based on AIO rank columns (`* AIO Rank`) and Search Volume.

    Theoretical max visibility (for all domains) is:
        sum(Search Volume over all AIO-triggering keywords)

    Domain visibility score (%) = domain_weighted_score / total_AIO_SV * 100
    where domain_weighted_score is the sum over all AIO keywords of:
        Search Volume * weight(rank)
    and rank weights are:
        1 -> 1.0
        2 -> 0.8
        3 -> 0.5
        >=4 -> 0.1
    """
    if "Search Volume" not in df.columns or "AIO Present" not in df.columns:
        return pd.DataFrame()

    # AIO-triggering keywords
    aio_mask = _normalize_aio_present(df["AIO Present"])
    aio_df = df[aio_mask].copy()
    if aio_df.empty:
        return pd.DataFrame()

    # All traffic domains (even those with 0 AIO)
    traffic_cols = [
        c for c in df.columns if "estimated monthly traffic" in str(c).lower()
    ]
    traffic_domain_map: Dict[str, str] = {
        col: _extract_domain_from_traffic_col(col) for col in traffic_cols
    }
    all_domains = set(traffic_domain_map.values())

    # Rank columns
    rank_cols = [c for c in df.columns if str(c).endswith(" AIO Rank")]
    if not rank_cols:
        # No rank columns -> cannot compute visibility, but still return domains with 0
        return pd.DataFrame(
            {
                "Domain": sorted(all_domains),
                "Unique AIO Keywords": [0] * len(all_domains),
                "AIO Visibility (Weighted Volume)": [0.0] * len(all_domains),
                "AIO Visibility Score (%)": [0.0] * len(all_domains),
            }
        )

    # Map AIO rank column -> domain label
    aio_rank_domain_map: Dict[str, str] = {}
    for col in rank_cols:
        # Usually pattern "<domain> AIO Rank"
        base = str(col).rsplit(" AIO Rank", 1)[0].strip()
        aio_rank_domain_map[col] = base

    # Weighted scores and keyword sets
    domain_scores: Dict[str, float] = {d: 0.0 for d in all_domains}
    domain_keywords: Dict[str, set] = {d: set() for d in all_domains}

    def _rank_weight(r: Any) -> float:
        try:
            r_int = int(r)
        except Exception:
            return 0.0
        if r_int == 1:
            return 1.0
        if r_int == 2:
            return 0.8
        if r_int == 3:
            return 0.5
        if r_int >= 4:
            return 0.1
        return 0.0

    # Theoretical max: sum of Search Volume over all AIO-triggering keywords
    total_aio_sv = aio_df["Search Volume"].fillna(0).astype(float).sum()
    if total_aio_sv <= 0:
        # Avoid division by zero; no visibility possible but keep domains
        total_aio_sv = 1.0

    # Iterate rows
    for idx, row in aio_df.iterrows():
        sv = row.get("Search Volume", 0)
        try:
            sv = float(sv)
        except Exception:
            sv = 0.0

        # Use a keyword identifier if present, else row index
        kw_id = row.get("Keyword", idx)

        for col in rank_cols:
            rank_val = row.get(col)
            if pd.isna(rank_val) or rank_val == "":
                continue
            w = _rank_weight(rank_val)
            if w <= 0:
                continue
            domain = aio_rank_domain_map.get(col)
            if not domain:
                continue
            # If domain is not in all_domains (no traffic col), still include it
            if domain not in domain_scores:
                domain_scores[domain] = 0.0
                domain_keywords[domain] = set()
                all_domains.add(domain)

            domain_scores[domain] += sv * w
            domain_keywords[domain].add(kw_id)

    rows: List[Dict[str, Any]] = []
    for domain in sorted(all_domains):
        score = domain_scores.get(domain, 0.0)
        kw_count = len(domain_keywords.get(domain, set()))
        visibility_pct = (score / total_aio_sv) * 100.0
        rows.append(
            {
                "Domain": domain,
                "Unique AIO Keywords": kw_count,
                "AIO Visibility (Weighted Volume)": score,
                "AIO Visibility Score (%)": visibility_pct,
            }
        )

    result = pd.DataFrame(rows)
    if not result.empty:
        # Sort by weighted volume descending
        result = result.sort_values("AIO Visibility (Weighted Volume)", ascending=False)
    return result


def compute_aio_segments(
    df: pd.DataFrame,
    segment_cols: List[str],
) -> pd.DataFrame:
    """
    AIO performance per segment (column/value):

    For each segment column and each non-empty value (tag), restricted to AIO-triggering keywords:
        - AIO_Keyword_Count: number of AIO-triggering keywords
        - AIO_Theoretical_Max: sum(Search Volume) for those AIO keywords
    """
    if "Search Volume" not in df.columns or "AIO Present" not in df.columns:
        return pd.DataFrame()

    aio_mask = _normalize_aio_present(df["AIO Present"])
    aio_df = df[aio_mask].copy()
    if aio_df.empty:
        return pd.DataFrame()

    rows: List[Dict[str, Any]] = []
    for seg_col in segment_cols:
        if seg_col not in aio_df.columns:
            continue

        series = aio_df[seg_col].astype(str).str.strip()
        mask = series != ""
        seg_work = aio_df[mask].copy()
        if seg_work.empty:
            continue

        for value, group in seg_work.groupby(seg_col):
            if pd.isna(value) or str(value).strip() == "":
                continue
            sv = group["Search Volume"].fillna(0).astype(float).sum()
            rows.append(
                {
                    "Segment Column": seg_col,
                    "Segment": value,
                    "AIO_Keyword_Count": len(group),
                    "AIO_Theoretical_Max": sv,
                }
            )

    result = pd.DataFrame(rows)
    if not result.empty:
        result = result.sort_values("AIO_Theoretical_Max", ascending=False)
    return result


def compute_aio_category_breakdown(
    df: pd.DataFrame,
    category_col: str,
) -> pd.DataFrame:
    """
    AIO deep-dive per category/tag, analogous to compute_category_breakdown but AIO-only:

    For each tag in `category_col`, restricted to AIO-triggering keywords:
        - AIO_Keyword_Count
        - AIO_Theoretical_Max (sum Search Volume)
    """
    if "Search Volume" not in df.columns or "AIO Present" not in df.columns:
        return pd.DataFrame()
    if category_col not in df.columns:
        return pd.DataFrame()

    aio_mask = _normalize_aio_present(df["AIO Present"])
    aio_df = df[aio_mask].copy()
    if aio_df.empty:
        return pd.DataFrame()

    series = aio_df[category_col].astype(str).str.strip()
    mask = series != ""
    work_df = aio_df[mask].copy()
    if work_df.empty:
        return pd.DataFrame()

    grouped = work_df.groupby(category_col)

    rows: List[Dict[str, Any]] = []
    for tag, group in grouped:
        sv = group["Search Volume"].fillna(0).astype(float).sum()
        rows.append(
            {
                category_col: tag,
                "AIO_Keyword_Count": len(group),
                "AIO_Theoretical_Max": sv,
            }
        )

    result = pd.DataFrame(rows)
    if not result.empty:
        result = result.sort_values("AIO_Theoretical_Max", ascending=False)
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
        df_keywords.to_excel(writer, sheet_name="Keywords", index=False)
        competitive_df.to_excel(writer, sheet_name="Summary_Competitive", index=False)
        segment_df.to_excel(writer, sheet_name="Summary_TrafficPotential", index=False)
        seasonality_df.to_excel(writer, sheet_name="Summary_Seasonality", index=False)

        if aio_summary_df is not None and not aio_summary_df.empty:
            aio_summary_df.to_excel(writer, sheet_name="Summary_AIO", index=False)

        if aio_visibility_df is not None and not aio_visibility_df.empty:
            aio_visibility_df.to_excel(writer, sheet_name="Summary_AIO_Visibility", index=False)

        if category_breakdowns:
            for category, df_cat in category_breakdowns.items():
                sheet_name = f"Cat_{str(category)[:25]}"
                df_cat.to_excel(writer, sheet_name=sheet_name, index=False)

        if aio_segments_df is not None and not aio_segments_df.empty:
            aio_segments_df.to_excel(writer, sheet_name="Summary_AIO_Segments", index=False)

        if aio_category_breakdowns:
            for category, df_cat in aio_category_breakdowns.items():
                sheet_name = f"AIO_Cat_{str(category)[:22]}"
                df_cat.to_excel(writer, sheet_name=sheet_name, index=False)

    output.seek(0)
    return output.getvalue()
