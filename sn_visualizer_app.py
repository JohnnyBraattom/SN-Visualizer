
"""Streamlit frontend for SN Visualizer."""

import os
from datetime import datetime

import streamlit as st
import pandas as pd
import altair as alt

from sn_visualizer_backend import (
    load_keywords_dataframe,
    detect_columns,
    compute_competitive,
    compute_segments,
    compute_seasonality,
    compute_aio_summary,
    compute_category_breakdown,
    compute_aio_visibility,
    compute_aio_segments,
    compute_aio_category_breakdown,
    build_enriched_excel,
)


st.set_page_config(
    page_title="SN Visualizer",
    layout="wide",
)


def main() -> None:
    st.title("Search Navigator Visualizer")

    uploaded_file = st.file_uploader(
        "Upload a Search Navigator Excel export (.xlsx)",
        type=["xlsx"],
        help="Upload the export that contains a 'Keywords' sheet.",
    )

    if not uploaded_file:
        st.info("Upload a Search Navigator Excel export to get started.")
        return

    try:
        df_keywords = load_keywords_dataframe(uploaded_file)
    except Exception as exc:
        st.error(f"Error loading Excel file: {exc}")
        return

    if df_keywords.empty:
        st.warning("The 'Keywords' sheet is empty.")
        return

    col_info = detect_columns(df_keywords)
    search_volume_col = col_info.get("search_volume_col")
    traffic_cols = col_info.get("traffic_cols", [])
    client_traffic_col = col_info.get("client_traffic_col")
    segment_cols = col_info.get("segment_cols", [])
    month_cols_sorted = col_info.get("month_cols_sorted", [])
    last_24_month_cols = col_info.get("last_24_month_cols", [])

    # Top-level KPIs
    st.subheader("Overview")

    col1, col2, col3 = st.columns(3)
    with col1:
        st.metric("Keywords", f"{len(df_keywords):,}")
    with col2:
        if search_volume_col and search_volume_col in df_keywords.columns:
            total_sv = df_keywords[search_volume_col].fillna(0).astype(float).sum()
            st.metric("Total Search Volume", f"{int(total_sv):,}")
        else:
            st.metric("Total Search Volume", "n/a")
    with col3:
        st.metric("Traffic Domains", f"{len(traffic_cols):,}")

    st.divider()

    # AIO summary
    st.subheader("AIO summary")
    aio_df = compute_aio_summary(df_keywords)
    if aio_df.empty:
        st.info("No AIO data available (column 'AIO Present' missing or empty).")
    else:
        aio_trigger_row = aio_df[aio_df["Status"] == "AIO triggered"]
        if not aio_trigger_row.empty:
            pct = float(aio_trigger_row["Percentage"].iloc[0])
            st.metric("Keywords triggering AIO", f"{pct:.1f}%")
        else:
            st.metric("Keywords triggering AIO", "0.0%")

        chart = (
            alt.Chart(aio_df)
            .mark_bar()
            .encode(
                x=alt.X("Status:N", title="Status"),
                y=alt.Y("Count:Q", title="Keywords"),
                color="Status:N",
                tooltip=["Status", "Count", alt.Tooltip("Percentage:Q", format=".1f")],
            )
        )
        st.altair_chart(chart, use_container_width=True)

        with st.expander("Show AIO summary data"):
            st.dataframe(aio_df, use_container_width=True)

    st.divider()

    # AIO visibility by domain
    st.subheader("AIO visibility by domain")

    aio_vis_df = compute_aio_visibility(df_keywords)
    if aio_vis_df.empty:
        st.info("No AIO visibility data available (no AIO ranks or AIO keywords).")
    else:
        # Combined bar (unique keywords) + line (visibility score %)
        base = alt.Chart(aio_vis_df).encode(x=alt.X("Domain:N", sort="-y", title="Domain"))

        bars = base.mark_bar().encode(
            y=alt.Y(
                "Unique AIO Keywords:Q",
                axis=alt.Axis(title="Unique AIO keywords"),
            ),
            tooltip=[
                "Domain",
                "Unique AIO Keywords",
                alt.Tooltip("AIO Visibility Score (%):Q", format=".1f"),
            ],
        )

        # Dynamic y-axis max for line
        max_score = float(aio_vis_df["AIO Visibility Score (%)"].max()) if not aio_vis_df.empty else 0.0
        line = base.mark_line(point=True).encode(
            y=alt.Y(
                "AIO Visibility Score (%):Q",
                axis=alt.Axis(title="AIO visibility score (%)"),
                scale=alt.Scale(domain=[0, max(5, max_score * 1.1)]),
            ),
            tooltip=[
                "Domain",
                "Unique AIO Keywords",
                alt.Tooltip("AIO Visibility Score (%):Q", format=".1f"),
            ],
        )

        chart = alt.layer(bars, line).resolve_scale(y="independent")
        st.altair_chart(chart, use_container_width=True)

        with st.expander("Show AIO visibility data"):
            st.dataframe(aio_vis_df, use_container_width=True)

    st.divider()

    # Competitive landscape
    st.subheader("1. Competitive landscape (estimated traffic per domain)")
    competitive_df = compute_competitive(df_keywords, traffic_cols)
    if competitive_df.empty:
        st.info("No traffic columns found to compute competitive landscape.")
    else:
        comp_chart = (
            alt.Chart(competitive_df)
            .mark_bar()
            .encode(
                x=alt.X("Domain:N", sort="-y", title="Domain"),
                y=alt.Y("Estimated Monthly Traffic:Q", title="Estimated monthly traffic"),
                tooltip=["Domain", alt.Tooltip("Estimated Monthly Traffic:Q", format=",.0f")],
            )
        )
        st.altair_chart(comp_chart, use_container_width=True)
        with st.expander("Show competitive summary data"):
            st.dataframe(competitive_df, use_container_width=True)

    st.divider()

    # Segments traffic potential (original)
    st.subheader("2. Traffic potential and estimated traffic per segment")
    segment_df = compute_segments(df_keywords, segment_cols, traffic_cols)
    if segment_df.empty:
        st.info("No segment data available (no segment columns or Search Volume missing).")
    else:
        # Long-form for charting
        value_vars = [c for c in segment_df.columns if c not in {"Segment Column", "Segment", "Traffic_Potential"}]
        seg_chart_df = segment_df.melt(
            id_vars=["Segment Column", "Segment"],
            value_vars=["Traffic_Potential"] + value_vars,
            var_name="Metric",
            value_name="Value",
        )

        seg_chart = (
            alt.Chart(seg_chart_df)
            .mark_bar()
            .encode(
                x=alt.X("Segment:N", title="Segment"),
                y=alt.Y("Value:Q", title="Value"),
                color=alt.Color("Metric:N", title="Metric"),
                column=alt.Column("Segment Column:N", title="Segment column"),
                tooltip=["Segment Column", "Segment", "Metric", alt.Tooltip("Value:Q", format=",.0f")],
            )
        )
        st.altair_chart(seg_chart, use_container_width=True)

        with st.expander("Show segment summary data"):
            st.dataframe(segment_df, use_container_width=True)

    st.divider()

    # AIO performance by segment
    st.subheader("AIO performance by segment")
    aio_segments_df = compute_aio_segments(df_keywords, segment_cols)
    if aio_segments_df.empty:
        st.info("No AIO segment data available (no AIO-triggering keywords or segment columns).")
    else:
        aio_seg_chart_df = aio_segments_df.melt(
            id_vars=["Segment Column", "Segment"],
            value_vars=["AIO_Keyword_Count", "AIO_Theoretical_Max"],
            var_name="Metric",
            value_name="Value",
        )
        aio_seg_chart = (
            alt.Chart(aio_seg_chart_df)
            .transform_calculate(
                metric_type="datum.Metric === 'AIO_Keyword_Count' ? 'Count' : 'Max'"
            )
            .mark_bar()
            .encode(
                x=alt.X("Segment:N", title="Segment"),
                y=alt.Y("Value:Q", title="Value"),
                color=alt.Color("Metric:N", title="Metric"),
                column=alt.Column("Segment Column:N", title="Segment column"),
                tooltip=["Segment Column", "Segment", "Metric", alt.Tooltip("Value:Q", format=",.0f")],
            )
        )
        st.altair_chart(aio_seg_chart, use_container_width=True)

        with st.expander("Show AIO segment data"):
            st.dataframe(aio_segments_df, use_container_width=True)

    st.divider()

    # Seasonality (last 24 months)
    st.subheader("3. Seasonality (last 24 months)")
    seasonality_df = compute_seasonality(df_keywords, last_24_month_cols)
    if seasonality_df.empty:
        st.info("No monthly Search Volume columns found to compute seasonality.")
    else:
        season_chart = (
            alt.Chart(seasonality_df)
            .mark_line(point=True)
            .encode(
                x=alt.X("Month:N", title="Month"),
                y=alt.Y("Search Volume:Q", title="Total search volume"),
                tooltip=["Month", alt.Tooltip("Search Volume:Q", format=",.0f")],
            )
        )
        st.altair_chart(season_chart, use_container_width=True)

        with st.expander("Show seasonality data"):
            st.dataframe(seasonality_df, use_container_width=True)

    st.divider()

    # Category deep-dive for traffic and AIO
    st.subheader("4. Category deep-dive")

    default_categories = "Search Intent"
    category_text = st.text_area(
        "Category columns (one per line)",
        value=default_categories,
        help="Specify which columns you want to break down by tag. Case-insensitive.",
    )
    requested_categories = [
        line.strip() for line in category_text.splitlines() if line.strip()
    ]

    # Map requested names to actual columns (case-insensitive)
    col_map = {c.lower(): c for c in df_keywords.columns}
    valid_categories = []
    for req in requested_categories:
        key = req.lower()
        if key in col_map:
            valid_categories.append(col_map[key])
        else:
            st.warning(f"Category column not found: {req}")

    # Always auto-include 'Search Intent' if present
    if "search intent" in col_map and col_map["search intent"] not in valid_categories:
        valid_categories.append(col_map["search intent"])

    if not valid_categories:
        st.info("No valid category columns found for deep-dive.")
    elif not client_traffic_col:
        st.info("Client traffic column not detected; category deep-dive cannot compute client traffic.")
    else:
        category_breakdowns: Dict[str, pd.DataFrame] = {}
        aio_category_breakdowns: Dict[str, pd.DataFrame] = {}

        for category in valid_categories:
            st.markdown(f"#### Category: {category}")

            traffic_df = compute_category_breakdown(df_keywords, category, client_traffic_col)
            if traffic_df.empty:
                st.info(f"No data for category '{category}' (traffic breakdown).")
            else:
                category_breakdowns[category] = traffic_df

                # Traffic potential + client traffic per tag
                traffic_melt = traffic_df.melt(
                    id_vars=[category],
                    value_vars=["Traffic_Potential", "Client_Traffic"],
                    var_name="Metric",
                    value_name="Value",
                )
                traffic_chart = (
                    alt.Chart(traffic_melt)
                    .mark_bar()
                    .encode(
                        x=alt.X(f"{category}:N", title="Tag"),
                        y=alt.Y("Value:Q", title="Value"),
                        color=alt.Color("Metric:N", title="Metric"),
                        tooltip=[category, "Metric", alt.Tooltip("Value:Q", format=",.0f")],
                    )
                )
                st.altair_chart(traffic_chart, use_container_width=True)

                with st.expander(f"Show traffic data for {category}"):
                    st.dataframe(traffic_df, use_container_width=True)

            # AIO deep-dive for the same category
            aio_cat_df = compute_aio_category_breakdown(df_keywords, category)
            if aio_cat_df.empty:
                st.info(f"No AIO data for category '{category}'.")
            else:
                aio_category_breakdowns[category] = aio_cat_df

                aio_cat_melt = aio_cat_df.melt(
                    id_vars=[category],
                    value_vars=["AIO_Keyword_Count", "AIO_Theoretical_Max"],
                    var_name="Metric",
                    value_name="Value",
                )
                aio_cat_chart = (
                    alt.Chart(aio_cat_melt)
                    .mark_bar()
                    .encode(
                        x=alt.X(f"{category}:N", title="Tag"),
                        y=alt.Y("Value:Q", title="Value"),
                        color=alt.Color("Metric:N", title="Metric"),
                        tooltip=[category, "Metric", alt.Tooltip("Value:Q", format=",.0f")],
                    )
                )
                st.altair_chart(aio_cat_chart, use_container_width=True)

                with st.expander(f"Show AIO data for {category}"):
                    st.dataframe(aio_cat_df, use_container_width=True)

    st.divider()

    # Enriched Excel download
    st.subheader("Download enriched Excel")

    if st.button("Build enriched Excel file"):
        try:
            # Recompute pieces if they were not available above
            if "competitive_df" not in locals():
                competitive_df_local = compute_competitive(df_keywords, traffic_cols)
            else:
                competitive_df_local = competitive_df

            if "segment_df" not in locals():
                segment_df_local = compute_segments(df_keywords, segment_cols, traffic_cols)
            else:
                segment_df_local = segment_df

            if "seasonality_df" not in locals():
                seasonality_df_local = compute_seasonality(df_keywords, last_24_month_cols)
            else:
                seasonality_df_local = seasonality_df

            if "aio_df" not in locals():
                aio_summary_df = compute_aio_summary(df_keywords)
            else:
                aio_summary_df = aio_df

            if "aio_vis_df" not in locals():
                aio_vis_df_local = compute_aio_visibility(df_keywords)
            else:
                aio_vis_df_local = aio_vis_df

            if "category_breakdowns" not in locals():
                category_breakdowns_local = {}
            else:
                category_breakdowns_local = category_breakdowns

            if "aio_segments_df" not in locals():
                aio_segments_df_local = compute_aio_segments(df_keywords, segment_cols)
            else:
                aio_segments_df_local = aio_segments_df

            if "aio_category_breakdowns" not in locals():
                aio_category_breakdowns_local = {}
            else:
                aio_category_breakdowns_local = aio_category_breakdowns

            excel_bytes = build_enriched_excel(
                df_keywords,
                competitive_df_local,
                segment_df_local,
                seasonality_df_local,
                aio_summary_df=aio_summary_df,
                aio_visibility_df=aio_vis_df_local,
                category_breakdowns=category_breakdowns_local,
                aio_segments_df=aio_segments_df_local,
                aio_category_breakdowns=aio_category_breakdowns_local,
            )

            base_name, _ = os.path.splitext(uploaded_file.name)
            timestamp = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
            out_filename = f"{base_name}_SN-Visualizer_{timestamp}.xlsx"

            st.download_button(
                label="Download enriched Excel",
                data=excel_bytes,
                file_name=out_filename,
                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            )
        except Exception as exc:
            st.error(f"Error building enriched Excel file: {exc}")


if __name__ == "__main__":
    main()
