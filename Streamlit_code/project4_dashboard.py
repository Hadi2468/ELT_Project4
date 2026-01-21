# =====================================================================================
# Project 4: Streamlit Dashboard (FINAL – SME READY)
# =====================================================================================

import streamlit as st
import pandas as pd
import numpy as np
import boto3
import plotly.express as px

# =====================================================================================
# ⚙️ Configuration
# =====================================================================================

st.set_page_config(page_title="Project4 Dashboard", layout="wide")
st.title("🎥 Wistia Video Analytics")
st.markdown("---")

# =====================================================================================
# Sidebar
# =====================================================================================

st.sidebar.title("🎞️ Wistia's Stats API")

menu = st.sidebar.radio(
    "🔍 Select Dashboard",
    [
        "1- CI/CD Configuration",
        "2- Basic Media Statistics",
        "3- Media Performance Comparison",
        "4- Media Classification",
        "5- Visitor Classification",
        "6- RF & Visitor Engagement"
    ]
)

# =====================================================================================
# CI/CD CONFIG
# =====================================================================================

REGION = "us-east-1"
PIPELINE_NAME = "project4-github-codepipeline"
GLUE_JOB_NAME = "project4-api-bronze"

# =====================================================================================
# Helper Functions: Color Coding for Tables
# =====================================================================================

def highlight_media(row):
    if "media_id" not in row:
        return [""] * len(row)
    if row["media_id"] == "gskhw4w4lm":
        return ["background-color: #ff4c4c; color: white"] * len(row)
    elif row["media_id"] == "v08dlrgr7v":
        return ["background-color: #4c4cff; color: white"] * len(row)
    else:
        return [""] * len(row)

def highlight_rf(row):
    if "rf_segment" not in row:
        return [""] * len(row)
    if row["rf_segment"] == "Active":
        return ["background-color: #4caf50; color: white"] * len(row)
    elif row["rf_segment"] == "Infrequent":
        return ["background-color: #ff9800; color: white"] * len(row)
    elif row["rf_segment"] == "Dormant":
        return ["background-color: #9e9e9e; color: white"] * len(row)
    else:
        return [""] * len(row)


# =====================================================================================
# Load Gold Layer
# =====================================================================================

@st.cache_data
def load_data():
    media_df = pd.read_parquet(
        "s3://project4-gold-bucket/fact_media_performance/"
    )
    audience_df = pd.read_parquet(
        "s3://project4-gold-bucket/fact_audience_insights/"
    )
    return media_df, audience_df

media_df, audience_df = load_data()

# =====================================================================================
# CI/CD CONTROLS
# =====================================================================================

def trigger_codepipeline(pipeline_name):
    try:
        client = boto3.client("codepipeline", region_name=REGION)
        response = client.start_pipeline_execution(name=pipeline_name)
        return response["pipelineExecutionId"]
    except Exception as e:
        st.error(f"❌ Failed to trigger CodePipeline: {e}")
        return None

def trigger_glue_job(job_name, job_args=None):
    try:
        glue_client = boto3.client("glue", region_name=REGION)
        response = glue_client.start_job_run(
            JobName=job_name,
            Arguments=job_args or {}
        )
        return response["JobRunId"]
    except Exception as e:
        st.error(f"❌ Failed to trigger Glue job: {e}")
        return None

if menu == "1- CI/CD Configuration":
    st.header("⚙️ CI/CD Configuration")

    col1, col2, col3 = st.columns(3)

    with col2:
        if st.button("💾 GitHub Deployment via CodePipeline"):
            execution_id = trigger_codepipeline(PIPELINE_NAME)
            if execution_id:
                st.success(f"✅ Pipeline triggered!\nExecution ID: {execution_id}")

    with col3:
        if st.button("⏩ Trigger Glue Job Manually"):
            run_id = trigger_glue_job(GLUE_JOB_NAME)
            if run_id:
                st.success(f"✅ Glue Job triggered!\nRun ID: {run_id}")

# =====================================================================================
# 2️⃣ BASIC MEDIA STATS
# =====================================================================================

elif menu == "2- Basic Media Statistics":
    st.header("📶 Basic Media Statistics")
    styled_media_df = media_df.style.apply(highlight_media, axis=1)
    st.dataframe(styled_media_df, use_container_width=True)

# =====================================================================================
# 3️⃣ MEDIA PERFORMANCE COMPARISON
# =====================================================================================

elif menu == "3- Media Performance Comparison":
    st.header("📊 Media Performance Comparison")

    summary = (
        media_df
        .groupby("media_id", as_index=False)
        .agg(
            total_plays=("play_count", "sum"),
            total_watch_time=("hours_watched", "sum"),
            avg_play_rate=("play_rate", "mean")
        )
    )

    summary["avg_watch_time_per_play"] = (
        summary["total_watch_time"] / summary["total_plays"]
    )

    styled_summary = summary.style.apply(highlight_media, axis=1)
    st.dataframe(styled_summary, use_container_width=True)
    st.markdown("---")
    
    col1, col2 = st.columns(2)

    with col1:
        fig = px.bar(
            summary,
            x="media_id",
            y="avg_watch_time_per_play",
            color="media_id",
            color_discrete_map={
                "gskhw4w4lm": "#ff4c4c",
                "v08dlrgr7v": "#4c4cff"
            },
            title="Average Watch Time per Play"
        )
        st.plotly_chart(fig, use_container_width=True)

    with col2:
        fig = px.bar(
            summary,
            x="media_id",
            y="avg_play_rate",
            color="media_id",
            color_discrete_map={
                "gskhw4w4lm": "#ff4c4c",
                "v08dlrgr7v": "#4c4cff"
            },
            title="Average Play Rate by Media"
        )
        fig.update_yaxes(tickformat=".0%")
        st.plotly_chart(fig, use_container_width=True)


# =====================================================================================
# 4️⃣ MEDIA CLASSIFICATION
# =====================================================================================

elif menu == "4- Media Classification":
    st.header("🗃️ Media Classification")

    media_df["engagement_class"] = np.where(
        (media_df["play_count"] > 1000) & (media_df["play_rate"] > 0.2),
        "High Engagement",
        np.where(
            media_df["play_count"].between(500, 1000),
            "Medium Engagement",
            "Low Engagement"
        )
    )

    styled_classification = media_df[["media_id", "play_count", "play_rate", "hours_watched", "engagement_class"]].style.apply(highlight_media, axis=1)
    st.dataframe(styled_classification, use_container_width=True)

# =====================================================================================
# 5️⃣ VISITOR CLASSIFICATION
# =====================================================================================

elif menu == "5- Visitor Classification":
    st.header("🙋🏻 Visitor Classification")

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

# =====================================================================================
# 6️⃣ RF & VISITOR ENGAGEMENT
# =====================================================================================
elif menu == "6- RF & Visitor Engagement":
    st.header("👫 RF & Visitor Engagement")

    # Assign RF segments
    audience_df["rf_segment"] = np.where(
        audience_df["recency_days"] <= 7,
        "Active",
        np.where(
            audience_df["frequency"] >= 3,
            "Infrequent",
            "Dormant"
        )
    )

    # Subsection: RF Segmentation (Top 100 rows for reference)
    st.subheader("RF Segmentation (Top 100 Rows)")
    st.dataframe(
        audience_df[["visitor_key", "recency_days", "frequency", "rf_segment"]].head(100),
        use_container_width=True
    )

    # Subsection: RF Segment Distribution (Bar Chart)
    st.subheader("RF Segment Distribution")
    segment_counts = audience_df["rf_segment"].value_counts().reset_index()
    segment_counts.columns = ["rf_segment", "count"]

    fig = px.bar(
        segment_counts,
        x="rf_segment",
        y="count",
        color="rf_segment",
        color_discrete_map={
            "Active": "#ff0000",
            "Infrequent": "#ffd034",
            "Dormant": "#077842"
        },
        title="Number of Users per RF Segment"
    )

    # Rotate x-axis labels and adjust layout
    fig.update_layout(
        xaxis_title="RF Segment",
        yaxis_title="Number of Users",
        bargap=0.5
    )

    st.plotly_chart(fig, use_container_width=True)

    # Subsection: Top Visitors by Frequency
    st.subheader("Top Visitors by Frequency")
    st.dataframe(
        audience_df.sort_values("frequency", ascending=False)
        .head(10)[["visitor_key", "frequency", "recency_days"]],
        use_container_width=True
    )


# =========================================================
# ✅ Footer
# =========================================================
st.markdown("---")
st.markdown("Built on **AWS | Streamlit**")
st.markdown("© 2026 Hadi Hosseini")
