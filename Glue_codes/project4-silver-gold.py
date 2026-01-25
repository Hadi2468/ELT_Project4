# =====================================================
# Project 4: Glue ETL Job (project4-silver-gold)
# =====================================================

import sys
import logging
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from pyspark.sql import functions as F
from pyspark.sql.types import *

# =====================================================
# Logging Configuration
# =====================================================
logger = logging.getLogger("project4-silver-gold")
logger.setLevel(logging.INFO)

handler = logging.StreamHandler(sys.stdout)
formatter = logging.Formatter(
    "%(asctime)s | %(levelname)s | %(name)s | %(message)s"
)
handler.setFormatter(formatter)

if not logger.handlers:
    logger.addHandler(handler)

# =====================================================
# Glue Context
# =====================================================
def get_glue_context():
    try:
        logger.info("⚙️ Initializing Glue & Spark context")
        sc = SparkContext()
        glue_context = GlueContext(sc)
        spark = glue_context.spark_session

        spark.conf.set(
            "spark.sql.sources.partitionOverwriteMode",
            "dynamic"
        )
        return spark

    except Exception as e:
        logger.exception("🚨 Failed to initialize Glue context")
        raise RuntimeError("GlueContext initialization failed") from e

# =====================================================
# Read Silver Tables
# =====================================================
def read_silver_tables(spark, silver_bucket):
    try:
        logger.info("📥 Reading Silver tables")

        media_df = spark.read.parquet(
            f"s3://{silver_bucket}/fact_media/"
        )
        engagement_df = spark.read.parquet(
            f"s3://{silver_bucket}/fact_media_engagement/"
        )
        visitors_df = spark.read.parquet(
            f"s3://{silver_bucket}/fact_visitors/"
        )

        logger.info("✅ Silver tables loaded successfully")
        return media_df, engagement_df, visitors_df

    except Exception as e:
        logger.exception("❌ Failed to read Silver tables")
        raise RuntimeError("Reading Silver layer failed") from e

# =====================================================
# 1️⃣ fact_media_performance
# =====================================================
def build_fact_media_performance(
    media_df, engagement_df, gold_bucket
):
    try:
        logger.info("🎬 Building fact_media_performance")

        media_engagement_agg = engagement_df.groupBy("media_id").agg(
            F.count("engagement_idx").alias("engagement_points"),
            F.sum("engagement_count").alias("total_engagement_count"),
            F.sum("rewatch_count").alias("total_rewatch_count"),
            F.avg("engagement_count").alias("avg_engagement_per_second")
        )

        media_perf = (
            media_df.join(media_engagement_agg, on="media_id", how="left")
            .withColumn(
                "play_rate_class",
                F.when(F.col("play_rate") >= 0.2, "High")
                 .when(F.col("play_rate") >= 0.1, "Medium")
                 .otherwise("Low")
            )
            .withColumn(
                "hours_class",
                F.when(F.col("hours_watched") >= 500, "High")
                 .when(F.col("hours_watched") >= 100, "Medium")
                 .otherwise("Low")
            )
        )

        media_perf.write.mode("overwrite").parquet(
            f"s3://{gold_bucket}/fact_media_performance/"
        )

        logger.info("✅ fact_media_performance written successfully")

    except Exception as e:
        logger.exception("❌ fact_media_performance failed")
        raise RuntimeError("Media performance aggregation failed") from e

# =====================================================
# 2️⃣ fact_audience_insights
# =====================================================
def build_fact_audience_insights(visitors_df, gold_bucket):
    try:
        logger.info("👥 Building fact_audience_insights")

        visitor_metrics = (
            visitors_df
            .withColumn(
                "recency_days",
                F.datediff(F.current_date(), F.to_date("last_active_at"))
            )
            .withColumn(
                "frequency",
                F.col("load_count") + F.col("play_count")
            )
            .withColumn(
                "visitor_segment",
                F.when(F.col("frequency") >= 5, "Frequent")
                 .when(F.col("frequency") >= 2, "Occasional")
                 .otherwise("Rare")
            )
        )

        visitor_metrics.write.mode("overwrite").parquet(
            f"s3://{gold_bucket}/fact_audience_insights/"
        )

        logger.info("✅ fact_audience_insights written successfully")

    except Exception as e:
        logger.exception("❌ fact_audience_insights failed")
        raise RuntimeError("Audience insights aggregation failed") from e

# =====================================================
# Main Orchestrator
# =====================================================
def main():
    logger.info("✅ ===== Project4 Silver → Gold Job Started =====")

    try:
        args = getResolvedOptions(
            sys.argv,
            ["SILVER_BUCKET", "GOLD_BUCKET"]
        )

        spark = get_glue_context()

        media_df, engagement_df, visitors_df = read_silver_tables(
            spark, args["SILVER_BUCKET"]
        )

        build_fact_media_performance(
            media_df, engagement_df, args["GOLD_BUCKET"]
        )

        build_fact_audience_insights(
            visitors_df, args["GOLD_BUCKET"]
        )

        logger.info("✅ ===== Project4 Silver → Gold Job Completed Successfully =====")

    except Exception:
        logger.exception("❌ Project4 Silver → Gold Job Failed")
        raise


if __name__ == "__main__":
    main()
