# =====================================================================================
# Project 4: Streamlit Dashboard
# =====================================================================================

import streamlit as st
import pandas as pd
import numpy as np
import boto3
import plotly.express as px
from datetime import datetime

# =====================================================================================
# ⚙️ Configuration
# =====================================================================================

# -----------------------------
# Streamlit Page Layout
# -----------------------------
st.set_page_config(page_title="Project4 Dashboard", layout="wide")
st.title("🎥 Wistia Video Analytics")
st.markdown("---")

# -----------------------------
# Sidebar
# ----------------------------
st.sidebar.title("🎞️ Wistia's Stats API")

menu = st.sidebar.radio(
    "🔍 Select Dashboard",
    [
        "1- CI/CD Configuration",
        "2- Basic Media Statistics",
        "3- Engagement Distribution",
        "4- Play Trends Over Time",
        "5- Media Classification",
        "6- Visitor Classification",
        "7- RF & Visitor Engagement"
    ]
)
# =====================================================================================
# 1️⃣ CI/CD Controls
# =====================================================================================
REGION = "us-east-1"
PIPELINE_NAME = "project4-github-codepipeline"
GLUE_WORKFLOW_NAME = "project4-bronze-silver-gold"

# -----------------------------
# Load Gold Layer Tables
# -----------------------------
@st.cache_data
def load_data():
    media_df = pd.read_parquet("s3://project4-gold-bucket/fact_media_performance/")
    audience_df = pd.read_parquet("s3://project4-gold-bucket/fact_audience_insights/")
    return media_df, audience_df

media_df, audience_df = load_data()

# -----------------------------
# Trigger CodePipeline
# -----------------------------
def trigger_codepipeline(pipeline_name):
    try:
        client = boto3.client('codepipeline', region_name=REGION)
        response = client.start_pipeline_execution(name=pipeline_name)
        return response['pipelineExecutionId']
    except Exception as e:
        st.error(f"❌ Failed to trigger CodePipeline: {e}")
        return None

# -----------------------------
# Trigger Glue Workflow
# -----------------------------
def trigger_glue_workflow(workflow_name):
    try:
        glue_client = boto3.client('glue', region_name=REGION)
        response = glue_client.start_workflow_run(Name=workflow_name)
        return response['RunId']
    except Exception as e:
        st.error(f"❌ Failed to trigger Glue workflow: {e}")
        return None

# -----------------------------
# Check menu option
# -----------------------------
if menu == "1- CI/CD Configuration":
    st.header("⚙️ CI/CD Configuration")
    col1, col2, col3 = st.columns(3)
    with col2:
        if st.button("💾 GitHub Deployment via CodePipeline"):
            execution_id = trigger_codepipeline(PIPELINE_NAME)
            if execution_id:
                st.success(f"✅ Pipeline triggered successfully!\nExecution ID: {execution_id}")
    with col3:
        if st.button("⏩ Trigger Glue Workflow Manually"):
            run_id = trigger_glue_workflow(GLUE_WORKFLOW_NAME)
            if run_id:
                st.success(f"✅ Glue Workflow triggered successfully!\nRun ID: {run_id}")
    st.markdown("---")

# =====================================================
# MEDIA KPIs
# =====================================================
elif menu == "2- Basic Media Statistics":
    st.header("📈 Basic Media Statistics")
    st.dataframe(media_df)

elif menu == "3- Engagement Distribution":
    st.header("📊 Engagement Distribution per Media")

    stats = (
        media_df.groupby("media_id")["total_engagement_count"]
        .agg(["mean", "median", "min", "max"])
        .reset_index()
    )

    percentiles = media_df.groupby("media_id")["total_engagement_count"].quantile(
        [0.25, 0.75]
    ).unstack().reset_index()
    percentiles.columns = ["media_id", "p25", "p75"]

    st.subheader("Summary Statistics")
    st.dataframe(stats)

    st.subheader("Engagement Percentiles")
    st.dataframe(percentiles)

elif menu == "4- Play Trends Over Time":
    st.header("📉 Play Trends Over Time")

    trend_df = media_df.sort_values("snapshot_ts")
    trend_df["play_count_ma"] = trend_df["play_count"].rolling(2).mean()

    fig = px.line(
        trend_df,
        x="snapshot_ts",
        y=["play_count", "play_count_ma"],
        title="Play Count Trend (with Moving Average)"
    )

    st.plotly_chart(fig, use_container_width=True)

elif menu == "5- Media Classification":
    st.header("🏷 Media Classification")

    media_df["engagement_class"] = np.where(
        (media_df["play_count"] > 1000) & (media_df["play_rate"] > 0.2),
        "High Engagement",
        np.where(
            media_df["play_count"].between(500, 1000),
            "Medium Engagement",
            "Low Engagement"
        )
    )

    st.dataframe(
        media_df[
            [
                "media_id",
                "play_count",
                "play_rate",
                "hours_watched",
                "engagement_class"
            ]
        ]
    )

# =====================================================
# AUDIENCE KPIs
# =====================================================
elif menu == "6- Visitor Classification":
    st.header("👥 Visitor Classification")

    col1, col2 = st.columns(2)

    with col1:
        platform_dist = audience_df["platform"].value_counts().reset_index()
        platform_dist.columns = ["platform", "count"]

        fig = px.pie(
            platform_dist,
            names="platform",
            values="count",
            title="Platform Distribution"
        )
        st.plotly_chart(fig)

    with col2:
        browser_dist = audience_df["browser"].value_counts().reset_index()
        browser_dist.columns = ["browser", "count"]

        fig = px.pie(
            browser_dist,
            names="browser",
            values="count",
            title="Browser Distribution"
        )
        st.plotly_chart(fig)

elif menu == "7- RF & Visitor Engagement":
    st.header("🔁 RF & Visitor Engagement")

    audience_df["rf_segment"] = np.where(
        audience_df["recency_days"] <= 7,
        "Active",
        np.where(
            audience_df["frequency"] >= 3,
            "Infrequent",
            "Dormant"
        )
    )

    st.subheader("RF Segmentation")
    st.dataframe(
        audience_df[
            ["visitor_key", "recency_days", "frequency", "rf_segment"]
        ]
    )

    st.subheader("Top Visitors by Frequency")
    st.dataframe(
        audience_df.sort_values("frequency", ascending=False)
        .head(10)[
            ["visitor_key", "frequency", "recency_days"]
        ]
    )
