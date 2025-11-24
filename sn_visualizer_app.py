
"""Streamlit frontend for SN Visualizer."""

import streamlit as st
import pandas as pd
import altair as alt

from sn_visualizer_backend import (
    load_keywords_dataframe,
    detect_columns,
    compute_competitive,
    compute_segments,
    compute_seasonality,
    build_enriched_excel,
)


st.set_page_config(page_title="SN Visualizer", layout="wide")


st.title("SN Visualizer")
st.write(
    "Upload an export from the main tool to generate summaries and graphs, and download an enriched Excel file."
)


uploaded_file = st.file_uploader("Upload Excel export", type=["xlsx"])


if uploaded_file is not None:
    try:
        df_keywords = load_keywords_dataframe(uploaded_file)
    except Exception as exc:
        st.error(f"Error reading file: {exc}")
        st.stop()

    st.success(f"'Keywords' sheet loaded. Rows: {len(df_keywords):,}, Columns: {len(df_keywords.columns):,}")

    # Detect columns and structures
    try:
        meta = detect_columns(df_keywords)
    except Exception as exc:
        st.error(f"Error detecting columns: {exc}")
        st.stop()

    traffic_cols = meta["traffic_cols"]
    client_traffic_col = meta["client_traffic_col"]
    segment_cols = meta["segment_cols"]
    last_24_month_cols = meta["last_24_month_cols"]

    # Basic KPIs
    col1, col2, col3 = st.columns(3)
    with col1:
        st.metric("Keywords", f"{len(df_keywords):,}")
    with col2:
        total_sv = df_keywords.get("Search Volume", pd.Series(dtype=float)).fillna(0).astype(float).sum()
        st.metric("Total Search Volume", f"{int(total_sv):,}")
    with col3:
        st.metric("Traffic Domains", f"{len(traffic_cols):,}")

    st.divider()

    # Compute data for graphs
    competitive_df = compute_competitive(df_keywords, traffic_cols)
    segment_df = compute_segments(df_keywords, segment_cols, client_traffic_col)
    seasonality_df = compute_seasonality(df_keywords, last_24_month_cols)

    # Competitive landscape graph
    st.subheader("1. Competitive landscape (estimated traffic per domain)")
    if competitive_df.empty:
        st.info("No competitive traffic data available.")
    else:
        chart_comp = (
            alt.Chart(competitive_df)
            .mark_bar()
            .encode(
                x=alt.X("Domain:N", sort="-y"),
                y=alt.Y("Estimated Monthly Traffic:Q"),
                tooltip=["Domain", "Estimated Monthly Traffic"],
            )
            .properties(height=400)
        )
        st.altair_chart(chart_comp, use_container_width=True)
        with st.expander("Show underlying data"):
            st.dataframe(competitive_df, use_container_width=True)

    st.divider()

    # Segment graph
    st.subheader("2. Search volume and client traffic per segment")
    if segment_df.empty:
        st.info("No segment data found between 'Topic' and the first monthly column.")
    else:
        seg_long = segment_df.melt(
            id_vars="Segment",
            value_vars=["Search Volume", "Client Estimated Monthly Traffic"],
            var_name="Metric",
            value_name="Value",
        )
        chart_seg = (
            alt.Chart(seg_long)
            .mark_bar()
            .encode(
                x=alt.X("Segment:N", sort="-y"),
                y=alt.Y("Value:Q"),
                color="Metric:N",
                tooltip=["Segment", "Metric", "Value"],
            )
            .properties(height=400)
        )
        st.altair_chart(chart_seg, use_container_width=True)
        with st.expander("Show underlying data"):
            st.dataframe(segment_df, use_container_width=True)

    st.divider()

    # Seasonality graph
    st.subheader("3. Seasonality (last 24 months total search volume)")
    if seasonality_df.empty:
        st.info("No seasonality data available.")
    else:
        chart_season = (
            alt.Chart(seasonality_df)
            .mark_line(point=True)
            .encode(
                x=alt.X("Month:N", sort=list(seasonality_df["Month"])),
                y=alt.Y("Total Search Volume:Q"),
                tooltip=["Month", "Total Search Volume"],
            )
            .properties(height=400)
        )
        st.altair_chart(chart_season, use_container_width=True)
        with st.expander("Show underlying data"):
            st.dataframe(seasonality_df, use_container_width=True)

    st.divider()

    # Build enriched Excel for download
    st.subheader("Download enriched Excel")
    try:
        excel_bytes = build_enriched_excel(df_keywords, competitive_df, segment_df, seasonality_df)
        st.download_button(
            label="Download enriched Excel",
            data=excel_bytes,
            file_name="sn_visualizer_enriched_export.xlsx",
            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        )
    except Exception as exc:
        st.error(f"Error building enriched Excel file: {exc}")
