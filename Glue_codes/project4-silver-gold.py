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
# Main
# =====================================================
def main():
    try:
        logger.info("===== Project4 Silver → Gold Job Started =====")

        # -------------------------------------------------
        # Read Glue Arguments
        # -------------------------------------------------
        args = getResolvedOptions(
            sys.argv,
            ["SILVER_BUCKET", "GOLD_BUCKET"]
        )

        SILVER_BUCKET = args["SILVER_BUCKET"]
        GOLD_BUCKET = args["GOLD_BUCKET"]

        logger.info(f"Silver bucket: {SILVER_BUCKET}")
        logger.info(f"Gold bucket: {GOLD_BUCKET}")

        # -------------------------------------------------
        # Glue Context
        # -------------------------------------------------
        sc = SparkContext()
        glueContext = GlueContext(sc)
        spark = glueContext.spark_session

        spark.conf.set(
            "spark.sql.sources.partitionOverwriteMode",
            "dynamic"
        )

        # =================================================
        # 1️⃣ Read Silver Tables with Explicit Schemas
        # =================================================
        logger.info("Reading Silver tables with schema")

        media_df = spark.read.parquet(f"s3://{SILVER_BUCKET}/fact_media/")
        engagement_df = spark.read.parquet(f"s3://{SILVER_BUCKET}/fact_media_engagement/")
        visitors_df = spark.read.parquet(f"s3://{SILVER_BUCKET}/fact_visitors/")


        # =================================================
        # 2️⃣ fact_media_performance (Single Table)
        # =================================================
        logger.info("Building fact_media_performance")

        # Aggregate engagement metrics per media
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

        # ✅ Write single table (all media together)
        media_perf.write.mode("overwrite") \
            .parquet(f"s3://{GOLD_BUCKET}/fact_media_performance/")
        logger.info("fact_media_performance written successfully")

        # =================================================
        # 3️⃣ fact_audience_insights (Single Table)
        # =================================================
        logger.info("Building fact_audience_insights")

        # Compute visitor metrics
        visitor_metrics = visitors_df.withColumn(
            "recency_days",
            F.datediff(F.current_date(), F.to_date("last_active_at"))
        ).withColumn(
            "frequency",
            F.col("load_count") + F.col("play_count")
        ).withColumn(
            "visitor_segment",
            F.when(F.col("frequency") >= 5, "Frequent")
             .when(F.col("frequency") >= 2, "Occasional")
             .otherwise("Rare")
        )

        # ✅ Write single table
        visitor_metrics.write.mode("overwrite") \
            .parquet(f"s3://{GOLD_BUCKET}/fact_audience_insights/")
        logger.info("fact_audience_insights written successfully")

        logger.info("===== Project4 Silver → Gold Job Completed Successfully =====")

    except Exception:
        logger.exception("❌ Project4 Silver → Gold Job Failed")
        raise


if __name__ == "__main__":
    main()
