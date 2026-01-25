# =====================================================
# Project 4: Glue ETL Job (project4-bronze-silver)
# =====================================================

import sys
import logging
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark.sql.types import *

# =====================================================
# Logging Configuration
# =====================================================
logger = logging.getLogger("project4-bronze-silver")
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
# 1️⃣ MEDIA STATS
# =====================================================
def process_media_stats(spark, bronze_bucket, silver_bucket):
    try:
        logger.info("🎬 Processing MEDIA stats")

        media_schema = StructType([
            StructField("load_count", LongType()),
            StructField("play_count", LongType()),
            StructField("play_rate", DoubleType()),
            StructField("hours_watched", DoubleType()),
            StructField("engagement", DoubleType()),
            StructField("visitors", LongType())
        ])

        MEDIA_PATH = f"s3://{bronze_bucket}/raw-data/media/"

        media_df = (
            spark.read
            .schema(media_schema)
            .option("multiline", "true")
            .json(MEDIA_PATH)
            .withColumn(
                "media_id",
                F.regexp_extract(
                    F.input_file_name(),
                    "media_(.*?)_",
                    1
                )
            )
            .withColumn(
                "snapshot_ts",
                F.to_timestamp(
                    F.regexp_extract(
                        F.input_file_name(),
                        "media_.*?_(\\d{8}_\\d{6})",
                        1
                    ),
                    "yyyyMMdd_HHmmss"
                )
            )
        )

        window_media = Window.partitionBy("media_id").orderBy(
            F.col("snapshot_ts").desc()
        )

        media_latest = (
            media_df
            .withColumn("rn", F.row_number().over(window_media))
            .filter("rn = 1")
            .drop("rn")
        )

        media_latest.write.mode("overwrite").parquet(
            f"s3://{silver_bucket}/fact_media/"
        )

        logger.info("✅ MEDIA stats written successfully")

    except Exception as e:
        logger.exception("❌ MEDIA stats processing failed")
        raise RuntimeError("MEDIA stats job failed") from e

# =====================================================
# 2️⃣ MEDIA ENGAGEMENT
# =====================================================
def process_media_engagement(spark, bronze_bucket, silver_bucket):
    try:
        logger.info("📊 Processing MEDIA engagement")

        engagement_path = f"s3://{bronze_bucket}/raw-data/media_engagement/"

        # Step 1: Read all JSON files with multiline=True
        engagement_df = (
            spark.read.option("multiline", "true").json(engagement_path)
            .withColumn(
                "media_id",
                F.regexp_extract(F.input_file_name(), "media_engagement_(.*?)_", 1)
            )
            .withColumn(
                "snapshot_ts",
                F.to_timestamp(
                    F.regexp_extract(
                        F.input_file_name(),
                        "media_engagement_.*?_(\\d{8}_\\d{6})",
                        1
                    ),
                    "yyyyMMdd_HHmmss"
                )
            )
        )

        # Step 2: Keep only latest snapshot per media_id
        window_media = Window.partitionBy("media_id").orderBy(
            F.col("snapshot_ts").desc()
        )

        engagement_latest = (
            engagement_df
            .withColumn("rn", F.row_number().over(window_media))
            .filter("rn = 1")
            .drop("rn")
        )

        # Step 3: Explode engagement_data and rewatch_data into rows
        engagement_exploded = (
            engagement_latest
            .withColumn("bucket", F.expr("sequence(0, size(engagement_data)-1)"))
            .withColumn("bucket", F.explode("bucket"))
            .withColumn("engagement_count", F.expr("engagement_data[bucket]"))
            .withColumn("rewatch_count", F.expr("rewatch_data[bucket]"))
            .select(
                "media_id",
                "snapshot_ts",
                F.col("bucket").alias("engagement_idx"),
                "engagement_count",
                "rewatch_count",
                "engagement"
            )
        )

        # Step 4: Write one Parquet file
        engagement_exploded.write.mode("overwrite").parquet(
            f"s3://{silver_bucket}/fact_media_engagement/"
        )

        logger.info("✅ MEDIA engagement written successfully")

    except Exception as e:
        logger.exception("❌ MEDIA engagement processing failed")
        raise RuntimeError("MEDIA engagement job failed") from e

# =====================================================
# 3️⃣ VISITORS
# =====================================================
def process_visitors(spark, bronze_bucket, silver_bucket):
    try:
        logger.info("👥 Processing VISITORS")

        visitors_path = f"s3://{bronze_bucket}/raw-data/visitors/"

        visitors_df = (
            spark.read
            .option("multiline", "true")
            .json(visitors_path)
        )

        visitors_flat = visitors_df.select(
            "visitor_key",
            F.to_timestamp("created_at").alias("created_at"),
            F.to_timestamp("last_active_at").alias("last_active_at"),
            "last_event_key",
            "load_count",
            "play_count",
            "identifying_event_key",
            F.col("visitor_identity.name").alias("name"),
            F.col("visitor_identity.email").alias("email"),
            F.col("visitor_identity.org.name").alias("org_name"),
            F.col("visitor_identity.org.title").alias("org_title"),
            F.col("user_agent_details.browser").alias("browser"),
            F.col("user_agent_details.browser_version").alias("browser_version"),
            F.col("user_agent_details.platform").alias("platform"),
            F.col("user_agent_details.mobile").alias("mobile"),
            F.current_timestamp().alias("ingested_at")
        )

        window_visitors = Window.partitionBy("visitor_key").orderBy(
            F.col("last_active_at").desc_nulls_last()
        )

        visitors_dedup = (
            visitors_flat
            .withColumn("rn", F.row_number().over(window_visitors))
            .filter("rn = 1")
            .drop("rn")
        )

        visitors_dedup.write.mode("overwrite").parquet(
            f"s3://{silver_bucket}/fact_visitors/"
        )

        logger.info("✅ VISITORS written successfully")

    except Exception as e:
        logger.exception("❌ VISITORS processing failed")
        raise RuntimeError("VISITORS job failed") from e

# =====================================================
# Main Orchestrator
# =====================================================
def main():
    logger.info("✅ ===== Project4 Bronze → Silver Job Started =====")

    try:
        args = getResolvedOptions(
            sys.argv,
            ["BRONZE_BUCKET", "SILVER_BUCKET"]
        )

        spark = get_glue_context()

        process_media_stats(spark, args["BRONZE_BUCKET"], args["SILVER_BUCKET"])
        process_media_engagement(spark, args["BRONZE_BUCKET"], args["SILVER_BUCKET"])
        process_visitors(spark, args["BRONZE_BUCKET"], args["SILVER_BUCKET"])

        logger.info("✅ ===== Project4 Bronze → Silver Job Completed Successfully =====")

    except Exception:
        logger.exception("❌ Project4 Bronze → Silver Job Failed")
        raise


if __name__ == "__main__":
    main()
