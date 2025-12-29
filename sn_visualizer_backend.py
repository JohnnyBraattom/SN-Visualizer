
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

    Output rows:
    - Segment (the column name)
    - Traffic Potential
    - One column per traffic domain with summed traffic
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
        row: Dict[str, Any] = {
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



def _aio_present_mask(df: pd.DataFrame) -> pd.Series:
    """Return boolean mask for rows where AIO Present is truthy.
    Supports TRUE/FALSE, Yes/No, 1/0, True/False (strings or booleans).
    """
    if df is None or df.empty or "AIO Present" not in df.columns:
        return pd.Series([False] * (0 if df is None else len(df)), index=(None if df is None else df.index))

    s = df["AIO Present"]

    # If it's already boolean, use it directly
    if s.dtype == bool:
        return s.fillna(False)

    v = s.astype(str).str.strip().str.lower()
    return v.isin({"true", "yes", "y", "1", "t"})

def compute_aio_summary(df: pd.DataFrame) -> pd.DataFrame:
    """Summarise AIO presence as counts and percentages."""
    if "AIO Present" not in df.columns:
        return pd.DataFrame()

    total = len(df)
    if total == 0:
        return pd.DataFrame()

    mask_true = _aio_present_mask(df)
    count_true = int(mask_true.sum())
    count_false = int((~mask_true).sum())

    rows = []
    if count_true > 0:
        rows.append({"Status": "AIO triggered", "Count": count_true, "Percentage": (count_true / total) * 100.0})
    if count_false > 0:
        rows.append({"Status": "AIO not triggered", "Count": count_false, "Percentage": (count_false / total) * 100.0})

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


def compute_aio_visibility(df: pd.DataFrame) -> pd.DataFrame:
    """
    Compute AIO visibility per domain.

    - Only considers rows where AIO Present == 'yes' (case-insensitive).
    - For each domain AIO Rank column (* AIO Rank):
        - Converts rank to a weight (1 -> 1.0, 2 -> 0.8, 3 -> 0.5, >=4 -> 0.1).
        - Adds Search Volume * weight to that domain's visibility score.
        - Tracks in how many unique keywords the domain appears in AIO.
    - Theoretical maximum is: all AIO-triggering keywords ranking #1 for a given domain.
    - All traffic domains (from Estimated Monthly Traffic columns) are included in the result,
      even if they have zero AIO visibility.
    """
    if "AIO Present" not in df.columns or "Search Volume" not in df.columns:
        return pd.DataFrame()

    mask_aio = _aio_present_mask(df)
    df_aio = df.loc[mask_aio].copy()
    if df_aio.empty:
        return pd.DataFrame()

    # Identify AIO rank columns of the form "<domain> AIO Rank"
    aio_rank_cols = [c for c in df.columns if str(c).endswith(" AIO Rank")]

    visibility_scores: Dict[str, float] = {}
    keyword_sets: Dict[str, set] = {}

    for idx, row in df_aio.iterrows():
        vol_val = row.get("Search Volume", 0)
        try:
            vol = float(vol_val) if pd.notna(vol_val) else 0.0
        except Exception:
            vol = 0.0
        if vol <= 0:
            continue

        keyword = row.get("Keyword", idx)

        for col in aio_rank_cols:
            aio_rank_val = row.get(col, None)
            if aio_rank_val is None or (isinstance(aio_rank_val, float) and pd.isna(aio_rank_val)):
                continue
            try:
                rank_i = int(aio_rank_val)
            except Exception:
                continue

            if rank_i == 1:
                weight = 1.0
            elif rank_i == 2:
                weight = 0.8
            elif rank_i == 3:
                weight = 0.5
            else:
                weight = 0.1

            domain = str(col).replace(" AIO Rank", "").strip()
            visibility_scores[domain] = visibility_scores.get(domain, 0.0) + vol * weight
            keyword_sets.setdefault(domain, set()).add(keyword)

    # Build domain set from AIO ranks and traffic columns, to include zero-AIO domains
    traffic_cols = [c for c in df.columns if "Estimated Monthly Traffic" in str(c)]
    traffic_domains = {
        _extract_domain_from_traffic_col(c) for c in traffic_cols
    }

    all_domains = set(visibility_scores.keys()) | traffic_domains

    if not all_domains:
        return pd.DataFrame()

    # Theoretical maximum: each AIO keyword ranking #1
    total_aio_sv = 0.0
    for idx, row in df_aio.iterrows():
        vol_val = row.get("Search Volume", 0)
        try:
            vol = float(vol_val) if pd.notna(vol_val) else 0.0
        except Exception:
            vol = 0.0
        if vol <= 0:
            continue
        total_aio_sv += vol

    if total_aio_sv <= 0:
        total_aio_sv = 1.0

    rows = []
    for domain in sorted(all_domains):
        score = visibility_scores.get(domain, 0.0)
        unique_kw = len(keyword_sets.get(domain, set()))
        share_pct = (score / total_aio_sv) * 100.0
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



def compute_aio_visibility_for_category_tag(
    df: pd.DataFrame,
    category_col: str,
    tag_value: str,
) -> pd.DataFrame:
    """Compute AIO visibility by domain for a specific tag within a category.

    Filters rows where `category_col` matches `tag_value` (case-insensitive, trimmed),
    then runs `compute_aio_visibility` on that subset.
    """
    if df is None or df.empty:
        return pd.DataFrame()
    if not category_col or category_col not in df.columns:
        raise ValueError(f"Category column '{category_col}' not found in dataframe.")
    tag_norm = str(tag_value).strip().lower()
    if not tag_norm:
        return pd.DataFrame()
    series = df[category_col].astype(str).str.strip().str.lower()
    mask = series == tag_norm
    df_sub = df.loc[mask].copy()
    if df_sub.empty:
        return pd.DataFrame()
    return compute_aio_visibility(df_sub)




def compute_aio_visibility_for_category(
    df: pd.DataFrame,
    category_col: str,
) -> pd.DataFrame:
    """Compute AIO visibility by domain for a category column (non-empty values only).

    Filters rows where `category_col` is present and non-empty, then runs `compute_aio_visibility`
    on that subset.
    """
    if df is None or df.empty:
        return pd.DataFrame()
    if not category_col or category_col not in df.columns:
        raise ValueError(f"Category column '{category_col}' not found in dataframe.")
    series = df[category_col]
    mask = series.notna() & (series.astype(str).str.strip() != "")
    df_sub = df.loc[mask].copy()
    if df_sub.empty:
        return pd.DataFrame()
    return compute_aio_visibility(df_sub)


def _get_client_domain_and_rank_col(df: pd.DataFrame) -> Tuple[Optional[str], Optional[str]]:
    """Infer client domain from first traffic column and find matching AIO Rank column, if any."""
    traffic_cols = [c for c in df.columns if "Estimated Monthly Traffic" in str(c)]
    if not traffic_cols:
        return None, None
    client_traffic_col = traffic_cols[0]
    client_domain = _extract_domain_from_traffic_col(client_traffic_col)

    aio_rank_cols = [c for c in df.columns if str(c).endswith(" AIO Rank")]
    client_rank_col = None
    for col in aio_rank_cols:
        domain = str(col).replace(" AIO Rank", "").strip()
        if domain == client_domain:
            client_rank_col = col
            break

    return client_domain, client_rank_col


def compute_aio_performance_by_segment(
    df: pd.DataFrame,
    segment_cols: List[str],
) -> pd.DataFrame:
    """
    For each segment column (category), compute:

    - AIO Keywords (All Domains): unique count of AIO-triggering keywords
      where this segment column is non-empty.
    - AIO Keywords (Client): unique count of AIO-triggering keywords where
      this segment column is non-empty AND the client domain has an AIO rank.

    Uses 'Keyword' column for uniqueness if present; otherwise row index.
    """
    if "AIO Present" not in df.columns:
        return pd.DataFrame()

    client_domain, client_rank_col = _get_client_domain_and_rank_col(df)

    mask_aio = _aio_present_mask(df)
    if not mask_aio.any():
        return pd.DataFrame()

    df_aio = df.loc[mask_aio].copy()

    if "Keyword" in df_aio.columns:
        keyword_col = "Keyword"
    else:
        keyword_col = None

    rows = []
    for seg in segment_cols:
        if seg not in df_aio.columns:
            continue

        seg_series = df_aio[seg]
        mask_seg = seg_series.notna() & (seg_series.astype(str).str.strip() != "")
        if not mask_seg.any():
            continue

        df_seg = df_aio.loc[mask_seg]

        if keyword_col is not None:
            unique_all = df_seg[keyword_col].nunique()
        else:
            unique_all = df_seg.index.nunique()

        if client_rank_col is not None and client_rank_col in df_seg.columns:
            mask_client = df_seg[client_rank_col].notna()
            df_client_seg = df_seg.loc[mask_client]
            if keyword_col is not None:
                unique_client = df_client_seg[keyword_col].nunique()
            else:
                unique_client = df_client_seg.index.nunique()
        else:
            unique_client = 0

        rows.append(
            {
                "Segment": str(seg),
                "AIO Keywords (All Domains)": int(unique_all),
                "AIO Keywords (Client)": int(unique_client),
            }
        )

    result = pd.DataFrame(rows)
    if not result.empty:
        result = result.sort_values("AIO Keywords (All Domains)", ascending=False).reset_index(drop=True)
    return result


def compute_aio_category_client_breakdown(
    df: pd.DataFrame,
    category_col: str,
    include_competitors: bool = True,
) -> pd.DataFrame:
    """
    For a given category column, restricted to AIO-triggering keywords, compute:

    - AIO_Keyword_Count: unique number of AIO-triggering keywords per tag.
    - Client_AIO_Keyword_Count: unique number of AIO-triggering keywords per tag
      where the client domain has an AIO rank.

    Uses 'Keyword' column for uniqueness if present; otherwise row index.
    """
    if "AIO Present" not in df.columns:
        return pd.DataFrame()
    if category_col not in df.columns:
        raise ValueError(f"Category column '{category_col}' not found in dataframe.")

    client_domain, client_rank_col = _get_client_domain_and_rank_col(df)

    mask_aio = _aio_present_mask(df)
    if not mask_aio.any():
        return pd.DataFrame()

    df_aio = df.loc[mask_aio].copy()

    series = df_aio[category_col]
    mask = series.notna() & (series.astype(str).str.strip() != "")
    if not mask.any():
        return pd.DataFrame(columns=["Tag", "AIO_Keyword_Count", "Client_AIO_Keyword_Count"])

    temp = df_aio.loc[mask].copy()
    temp["__tag__"] = temp[category_col].astype(str).str.strip()

    if "Keyword" in temp.columns:
        keyword_col = "Keyword"
    else:
        keyword_col = None

    # All AIO keywords per tag
    if keyword_col is not None:
        all_counts = (
            temp.groupby("__tag__")[keyword_col]
            .nunique()
            .reset_index(name="AIO_Keyword_Count")
        )
    else:
        all_counts = (
            temp.groupby("__tag__")
            .size()
            .reset_index(name="AIO_Keyword_Count")
        )

    # Client AIO keywords per tag
    if client_rank_col is not None and client_rank_col in temp.columns:
        temp_client = temp[temp[client_rank_col].notna()].copy()
        if keyword_col is not None:
            client_counts = (
                temp_client.groupby("__tag__")[keyword_col]
                .nunique()
                .reset_index(name="Client_AIO_Keyword_Count")
            )
        else:
            client_counts = (
                temp_client.groupby("__tag__")
                .size()
                .reset_index(name="Client_AIO_Keyword_Count")
            )
    else:
        # No client AIO column -> zeros
        client_counts = all_counts[["__tag__"]].copy()
        client_counts["Client_AIO_Keyword_Count"] = 0

    
    competitor_counts_frames = []
    if include_competitors:
        # Competitor domains are inferred from available "<domain> AIO Rank" columns in the file.
        aio_rank_cols = [c for c in df.columns if str(c).endswith(" AIO Rank")]
        for col in aio_rank_cols:
            if client_rank_col is not None and col == client_rank_col:
                continue  # client already handled separately
            domain = str(col).replace(" AIO Rank", "").strip()
            # Only count where this domain has an AIO rank value
            temp_dom = temp[temp[col].notna()].copy()
            if temp_dom.empty:
                continue
            if keyword_col is not None:
                dom_counts = (
                    temp_dom.groupby("__tag__")[keyword_col]
                    .nunique()
                    .reset_index(name=f"{domain}_AIO_Keyword_Count")
                )
            else:
                dom_counts = (
                    temp_dom.groupby("__tag__")
                    .size()
                    .reset_index(name=f"{domain}_AIO_Keyword_Count")
                )
            competitor_counts_frames.append(dom_counts)

    merged = pd.merge(all_counts, client_counts, on="__tag__", how="left")

    # Merge competitor counts (if any)
    for frame in competitor_counts_frames:
        merged = pd.merge(merged, frame, on="__tag__", how="left")
    merged["Client_AIO_Keyword_Count"] = merged["Client_AIO_Keyword_Count"].fillna(0).astype(int)

    # Fill competitor count columns (if any)
    for col in merged.columns:
        if col.endswith("_AIO_Keyword_Count") and col not in ("Client_AIO_Keyword_Count", "AIO_Keyword_Count"):
            merged[col] = merged[col].fillna(0).astype(int)
    merged = merged.rename(columns={"__tag__": "Tag"})
    merged = merged.sort_values("AIO_Keyword_Count", ascending=False).reset_index(drop=True)
    return merged


def compute_dynamic_competitors(
    df_keywords: pd.DataFrame,
    top5_domains_col: str = "Top 5 Domains",
    search_volume_col: str = "Search Volume",
    top_n: int = 20,
) -> pd.DataFrame:
    """Compute top competitors by estimated traffic from the 'Top 5 Domains' list.

    For each keyword row, domains listed in `top5_domains_col` are assigned CTR weights
    based on their position (1..5). Estimated traffic contribution per domain is:
        Search Volume * CTR(position)

    Returns a dataframe with columns: ['Domain', 'Estimated Monthly Traffic']
    sorted descending, limited to `top_n`.
    """
    import urllib.parse

    if df_keywords is None or df_keywords.empty:
        return pd.DataFrame(columns=["Domain", "Estimated Monthly Traffic"])

    if top5_domains_col not in df_keywords.columns or search_volume_col not in df_keywords.columns:
        return pd.DataFrame(columns=["Domain", "Estimated Monthly Traffic"])

    ctrs = [0.2703, 0.1275, 0.0779, 0.0530, 0.0377]

    def _normalize_domain(raw: str) -> str:
        if raw is None:
            return ""
        s = str(raw).strip()
        if not s or s.lower() == "nan":
            return ""
        s = s.strip().strip(",;")
        if "://" in s:
            try:
                parsed = urllib.parse.urlparse(s)
                s = parsed.netloc or parsed.path
            except Exception:
                pass
        if "/" in s:
            s = s.split("/")[0]
        s = s.lower()
        if s.startswith("www."):
            s = s[4:]
        return s

    volumes = pd.to_numeric(df_keywords[search_volume_col], errors="coerce").fillna(0.0).astype(float).to_numpy()
    domains_series = df_keywords[top5_domains_col].fillna("").astype(str).to_numpy()

    traffic_by_domain: dict[str, float] = {}

    for cell, vol in zip(domains_series, volumes):
        if vol <= 0:
            continue
        parts = [p for p in re.split(r"\s*,\s*", cell) if p]
        if not parts:
            continue
        for idx, raw_dom in enumerate(parts[:5]):
            dom = _normalize_domain(raw_dom)
            if not dom:
                continue
            traffic_by_domain[dom] = traffic_by_domain.get(dom, 0.0) + (vol * ctrs[idx])

    if not traffic_by_domain:
        return pd.DataFrame(columns=["Domain", "Estimated Monthly Traffic"])

    out = (
        pd.DataFrame(
            {
                "Domain": list(traffic_by_domain.keys()),
                "Estimated Monthly Traffic": list(traffic_by_domain.values()),
            }
        )
        .sort_values("Estimated Monthly Traffic", ascending=False)
        .head(int(top_n))
        .reset_index(drop=True)
    )
    return out



def _make_unique_sheet_name(base: str, used: set, max_len: int = 31) -> str:
    """
    Sanitize and uniquify an Excel sheet name.

    - Excel forbids: : \ / ? * [ ]
    - Max length is 31 chars.
    - Names must be unique within a workbook.
    """
    if base is None:
        base = "Sheet"
    name = str(base)

    # Replace illegal characters
    name = re.sub(r'[\[\]\*:/\\\?]', "_", name)

    # Collapse whitespace/underscores a bit
    name = re.sub(r"\s+", " ", name).strip()
    if not name:
        name = "Sheet"

    # Truncate initially
    name = name[:max_len]

    # Uniquify if needed
    if name not in used:
        used.add(name)
        return name

    stem = name
    # Ensure suffix fits
    i = 2
    while True:
        suffix = f"_{i}"
        cut = max_len - len(suffix)
        cand = (stem[:cut]).rstrip() + suffix
        if cand not in used:
            used.add(cand)
            return cand
        i += 1


def build_enriched_excel(
    df_keywords: pd.DataFrame,
    competitive_df: pd.DataFrame,
    segment_df: pd.DataFrame,
    seasonality_df: pd.DataFrame,
    aio_summary_df: Optional[pd.DataFrame] = None,
    aio_visibility_df: Optional[pd.DataFrame] = None,
    aio_tag_visibility_df: Optional[pd.DataFrame] = None,
    category_aio_visibility: Optional[Dict[str, pd.DataFrame]] = None,
    category_breakdowns: Optional[Dict[str, pd.DataFrame]] = None,
    aio_category_breakdowns: Optional[Dict[str, pd.DataFrame]] = None,
) -> bytes:
    """Build an in-memory Excel file with original plus summary sheets."""
    output = io.BytesIO()
    with pd.ExcelWriter(output, engine="openpyxl") as writer:
        used_sheet_names = set()
        df_keywords.to_excel(writer, sheet_name=_make_unique_sheet_name("Keywords", used_sheet_names), index=False)
        competitive_df.to_excel(writer, sheet_name=_make_unique_sheet_name("Summary_Competitive", used_sheet_names), index=False)
        segment_df.to_excel(writer, sheet_name=_make_unique_sheet_name("Summary_TrafficPotential", used_sheet_names), index=False)
        seasonality_df.to_excel(writer, sheet_name=_make_unique_sheet_name("Summary_Seasonality", used_sheet_names), index=False)

        if aio_summary_df is not None and not aio_summary_df.empty:
            aio_summary_df.to_excel(writer, sheet_name=_make_unique_sheet_name("Summary_AIO", used_sheet_names), index=False)

        if aio_visibility_df is not None and not aio_visibility_df.empty:
            aio_visibility_df.to_excel(writer, sheet_name=_make_unique_sheet_name("Summary_AIO_Visibility", used_sheet_names), index=False)

        if aio_tag_visibility_df is not None and not aio_tag_visibility_df.empty:
            aio_tag_visibility_df.to_excel(writer, sheet_name=_make_unique_sheet_name("AIO_Tag_Visibility", used_sheet_names), index=False)


        if category_aio_visibility:
            for category, df_cat in category_aio_visibility.items():
                sheet_name = f"AIOVis_Cat_{str(category)[:20]}"
                df_cat.to_excel(writer, sheet_name=_make_unique_sheet_name(sheet_name, used_sheet_names), index=False)

        if category_breakdowns:
            for category, df_cat in category_breakdowns.items():
                sheet_name = f"Cat_{str(category)[:25]}"
                df_cat.to_excel(writer, sheet_name=_make_unique_sheet_name(sheet_name, used_sheet_names), index=False)

        if aio_category_breakdowns:
            for category, df_cat in aio_category_breakdowns.items():
                sheet_name = f"AIO_Cat_{str(category)[:22]}"
                df_cat.to_excel(writer, sheet_name=_make_unique_sheet_name(sheet_name, used_sheet_names), index=False)

    output.seek(0)
    return output.getvalue()