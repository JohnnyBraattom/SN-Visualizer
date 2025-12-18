
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
    compute_aio_performance_by_segment,
    compute_aio_category_client_breakdown,
    build_enriched_excel,
    compute_dynamic_competitors,
)


st.set_page_config(page_title="SN Visualizer", layout="wide")


st.title("SN Visualizer")
st.write(
    "Upload an export from the main tool to generate summaries and graphs, and download an enriched Excel file."
)


uploaded_file = st.file_uploader("Upload Excel export", type=["xlsx"])


if uploaded_file is not None:
    original_filename = uploaded_file.name

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
    traffic_domain_map = meta["traffic_domain_map"]

    # Infer client domain (first traffic domain)
    client_domain = traffic_domain_map.get(client_traffic_col, None)

    # Basic KPIs
    col1, col2, col3 = st.columns(3)
    with col1:
        st.metric("Keywords", f"{len(df_keywords):,}")
    with col2:
        total_sv = df_keywords.get("Search Volume", pd.Series(dtype=float)).fillna(0).astype(float).sum()
        st.metric("Total Search Volume", f"{int(total_sv):,}")
    with col3:
        st.metric("Traffic Domains", f"{len(traffic_cols):,}")

    # Top dynamic competitors (estimated traffic from 'Top 5 Domains' x CTR x Search Volume)
    st.subheader("Top dynamic competitors")
    dyn_comp_df = compute_dynamic_competitors(df_keywords, top_n=20)
    if dyn_comp_df.empty:
        st.info("No dynamic competitor data available (requires columns 'Top 5 Domains' and 'Search Volume').")
    else:
        st.dataframe(dyn_comp_df, use_container_width=True, hide_index=True)

    st.divider()

    # AIO summary
    st.subheader("AIO summary")
    aio_df = compute_aio_summary(df_keywords)
    if aio_df.empty:
        st.info("No AIO data available (column 'AIO Present' missing or empty).")
    else:
        # Show a simple percentage metric for AIO triggered
        aio_trigger_row = aio_df[aio_df["Status"] == "AIO triggered"]
        if not aio_trigger_row.empty:
            aio_pct = aio_trigger_row["Percentage"].iloc[0]
            st.metric("AIO triggered share", f"{aio_pct:.1f}%")

        chart_aio = (
            alt.Chart(aio_df)
            .mark_bar()
            .encode(
                x=alt.X("Status:N"),
                y=alt.Y("Percentage:Q"),
                tooltip=["Status", "Count", "Percentage"],
                color="Status:N",
            )
            .properties(height=300)
        )
        st.altair_chart(chart_aio, use_container_width=True)

        with st.expander("Show underlying AIO data"):
            st.dataframe(aio_df, use_container_width=True)

    st.divider()

    # AIO Visibility Score by domain
    st.subheader("AIO visibility by domain")
    aio_vis_df = compute_aio_visibility(df_keywords)
    if aio_vis_df.empty:
        st.info("No AIO visibility data could be computed.")
    else:
        # Dynamic max for the percentage axis on the line chart
        max_score = aio_vis_df["AIO Visibility Score (%)"].max()
        if pd.isna(max_score) or max_score <= 0:
            max_score = 1.0
        if max_score <= 20:
            y_max_pct = 20
        elif max_score <= 50:
            y_max_pct = 50
        else:
            y_max_pct = 100

        # Combined bar (unique keywords) + line (visibility score %)
        bars = (
            alt.Chart(aio_vis_df)
            .mark_bar()
            .encode(
                x=alt.X("Domain:N", sort="-y"),
                y=alt.Y("Unique AIO Keywords:Q", axis=alt.Axis(title="Unique AIO Keywords")),
                tooltip=[
                    "Domain",
                    "Unique AIO Keywords",
                    "AIO Visibility Score (%)",
                ],
            )
        )

        line = (
            alt.Chart(aio_vis_df)
            .mark_line(point=True)
            .encode(
                x=alt.X("Domain:N", sort="-y"),
                y=alt.Y(
                    "AIO Visibility Score (%):Q",
                    scale=alt.Scale(domain=[0, y_max_pct]),
                    axis=alt.Axis(title="AIO Visibility Score (%)", orient="right"),
                ),
                tooltip=["Domain", "AIO Visibility Score (%)"],
                color=alt.value("red"),
            )
        )

        chart_aio_vis = (
            alt.layer(bars, line)
            .resolve_scale(y="independent")
            .properties(height=400)
        )

        st.altair_chart(chart_aio_vis, use_container_width=True)

        # Hide raw weighted volume from UI table, but keep it in the dataframe for export
        display_cols = [c for c in aio_vis_df.columns if c != "AIO Visibility (Weighted Volume)"]
        with st.expander("Show AIO visibility data"):
            st.dataframe(aio_vis_df[display_cols], use_container_width=True)

    st.divider()

    # AIO performance by segment (per category)
    st.subheader("AIO performance by segment")
    aio_seg_df = compute_aio_performance_by_segment(df_keywords, segment_cols)
    if aio_seg_df.empty:
        st.info("No AIO performance by segment could be computed.")
    else:
        seg_long_aio = aio_seg_df.melt(
            id_vars=["Segment"],
            value_vars=["AIO Keywords (All Domains)", "AIO Keywords (Client)"],
            var_name="Metric",
            value_name="Value",
        )

        chart_aio_seg = (
            alt.Chart(seg_long_aio)
            .mark_bar()
            .encode(
                x=alt.X("Segment:N"),
                xOffset=alt.XOffset("Metric:N"),
                y=alt.Y("Value:Q"),
                color="Metric:N",
                tooltip=["Segment", "Metric", "Value"],
            )
            .properties(height=400)
        )
        st.altair_chart(chart_aio_seg, use_container_width=True)
        with st.expander("Show underlying AIO segment data"):
            st.dataframe(aio_seg_df, use_container_width=True)

    st.divider()

    # Compute data for main graphs
    competitive_df = compute_competitive(df_keywords, traffic_cols)
    segment_df = compute_segments(df_keywords, segment_cols, traffic_cols)
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

    # Segment graph: Traffic Potential + traffic per domain, clustered (not stacked)
    st.subheader("2. Traffic potential and estimated traffic per segment")
    if segment_df.empty:
        st.info("No segment data found between 'Topic' and the first monthly column.")
    else:
        # Melt into long format for Altair
        value_vars = [c for c in segment_df.columns if c != "Segment"]
        seg_long = segment_df.melt(
            id_vars="Segment",
            value_vars=value_vars,
            var_name="Metric",
            value_name="Value",
        )

        chart_seg = (
            alt.Chart(seg_long)
            .mark_bar()
            .encode(
                x=alt.X("Segment:N"),
                xOffset=alt.XOffset("Metric:N"),
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

    # Category deep-dive
    st.subheader("4. Category deep-dive (traffic potential and client traffic per tag)")
    st.write(
        "Enter category column names from the export (one per line). "
        "'Search Intent' will be included by default if it exists."
    )

    default_categories_text = "Search Intent"
    user_text = st.text_area("Categories (one per line)", value=default_categories_text, height=120)

    requested_categories = [line.strip() for line in user_text.splitlines() if line.strip()]

    # Ensure 'Search Intent' included if present in df
    if "Search Intent" in df_keywords.columns and "Search Intent" not in requested_categories:
        requested_categories.append("Search Intent")

    # Map user input to actual columns (case-insensitive)
    available_cols_lower = {str(c).strip().lower(): c for c in df_keywords.columns}
    resolved_categories = []
    unknown_categories = []

    for cat in requested_categories:
        key = cat.strip().lower()
        if key in available_cols_lower:
            resolved_categories.append(available_cols_lower[key])
        else:
            unknown_categories.append(cat)

    if unknown_categories:
        st.warning(
            "The following categories were not found as columns and will be ignored: "
            + ", ".join(sorted(set(unknown_categories)))
        )

    category_breakdowns = {}
    aio_category_breakdowns = {}
    if resolved_categories:
        for category in dict.fromkeys(resolved_categories):  # preserve order, remove duplicates
            st.markdown(f"#### Category: {category}")
            try:
                cat_df = compute_category_breakdown(df_keywords, category, client_traffic_col)
            except Exception as exc:
                st.error(f"Error computing breakdown for '{category}': {exc}")
                continue

            if cat_df.empty:
                st.info(f"No non-empty tags found for category '{category}'.")
            else:
                # Melt to plot Traffic_Potential and Client_Traffic as clustered bars
                cat_long = cat_df.melt(
                    id_vars=["Tag", "Keyword_Count"],
                    value_vars=["Traffic_Potential", "Client_Traffic"],
                    var_name="Metric",
                    value_name="Value",
                )

                chart_cat = (
                    alt.Chart(cat_long)
                    .mark_bar()
                    .encode(
                        x=alt.X("Tag:N", sort="-y"),
                        xOffset=alt.XOffset("Metric:N"),
                        y=alt.Y("Value:Q"),
                        color="Metric:N",
                        tooltip=["Tag", "Keyword_Count", "Metric", "Value"],
                    )
                    .properties(height=400)
                )
                st.altair_chart(chart_cat, use_container_width=True)
                with st.expander(f"Show data for {category}"):
                    st.dataframe(cat_df, use_container_width=True)

                category_breakdowns[category] = cat_df

            # AIO breakdown for this category
            st.markdown("##### AIO breakdown for this category")
            try:
                aio_cat_df = compute_aio_category_client_breakdown(df_keywords, category)
            except Exception as exc:
                st.error(f"Error computing AIO breakdown for '{category}': {exc}")
                continue

            if aio_cat_df.empty:
                st.info(f"No AIO data found for category '{category}'.")
            else:
                aio_cat_long = aio_cat_df.melt(
                    id_vars=["Tag"],
                    value_vars=["AIO_Keyword_Count", "Client_AIO_Keyword_Count"],
                    var_name="Metric",
                    value_name="Value",
                )

                chart_aio_cat = (
                    alt.Chart(aio_cat_long)
                    .mark_bar()
                    .encode(
                        x=alt.X("Tag:N", sort="-y"),
                        xOffset=alt.XOffset("Metric:N"),
                        y=alt.Y("Value:Q"),
                        color="Metric:N",
                        tooltip=["Tag", "Metric", "Value"],
                    )
                    .properties(height=400)
                )
                st.altair_chart(chart_aio_cat, use_container_width=True)
                with st.expander(f"Show AIO data for {category}"):
                    st.dataframe(aio_cat_df, use_container_width=True)

                aio_category_breakdowns[category] = aio_cat_df
    else:
        st.info("No valid categories provided for deep-dive.")

    st.divider()

    # Build enriched Excel for download
    st.subheader("Download enriched Excel")
    try:
        excel_bytes = build_enriched_excel(
            df_keywords,
            competitive_df,
            segment_df,
            seasonality_df,
            aio_summary_df=aio_df,
            aio_visibility_df=aio_vis_df,
            category_breakdowns=category_breakdowns,
            aio_category_breakdowns=aio_category_breakdowns,
        )
        base_name, _ = os.path.splitext(original_filename)
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
