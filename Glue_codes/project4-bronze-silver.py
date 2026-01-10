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
# Main
# =====================================================
def main():
    try:
        logger.info("===== Project4 Bronze → Silver Job Started =====")

        # -------------------------------------------------
        # Read Glue Arguments
        # -------------------------------------------------
        args = getResolvedOptions(
            sys.argv,
            ["BRONZE_BUCKET", "SILVER_BUCKET"]
        )

        BRONZE_BUCKET = args["BRONZE_BUCKET"]
        SILVER_BUCKET = args["SILVER_BUCKET"]

        logger.info(f"Bronze bucket: {BRONZE_BUCKET}")
        logger.info(f"Silver bucket: {SILVER_BUCKET}")

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

        # -------------------------------------------------
        # Paths
        # -------------------------------------------------
        MEDIA_PATH = f"s3://{BRONZE_BUCKET}/raw-data/media/"
        ENGAGEMENT_PATH = f"s3://{BRONZE_BUCKET}/raw-data/media_engagement/"
        VISITORS_PATH = f"s3://{BRONZE_BUCKET}/raw-data/visitors/"

        # =================================================
        # 1️⃣ MEDIA STATS (Current-State) — FIXED
        # =================================================
        logger.info("Processing MEDIA stats")

        media_schema = StructType([
            StructField("load_count", LongType()),
            StructField("play_count", LongType()),
            StructField("play_rate", DoubleType()),
            StructField("hours_watched", DoubleType()),
            StructField("engagement", DoubleType()),
            StructField("visitors", LongType())
        ])

        media_df = (
            spark.read
            .schema(media_schema)
            .option("multiline", "true")   # 🔑 critical fix
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
            f"s3://{SILVER_BUCKET}/fact_media/"
        )

        logger.info("MEDIA stats written successfully")

        # =================================================
        # 2️⃣ MEDIA ENGAGEMENT (Latest per media_id)
        # =================================================
        logger.info("Processing MEDIA engagement")
        
        # Step 1: Read all JSON files with multiline=True
        engagement_df = (
            spark.read.option("multiline", "true").json(ENGAGEMENT_PATH)
            .withColumn(
                "media_id",
                F.regexp_extract(F.input_file_name(), "media_engagement_(.*?)_", 1)
            )
            .withColumn(
                "snapshot_ts",
                F.to_timestamp(
                    F.regexp_extract(F.input_file_name(), "media_engagement_.*?_(\\d{8}_\\d{6})", 1),
                    "yyyyMMdd_HHmmss"
                )
            )
        )
        
        # Step 2: Keep only latest snapshot per media_id
        window_media = Window.partitionBy("media_id").orderBy(F.col("snapshot_ts").desc())
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
            f"s3://{SILVER_BUCKET}/fact_media_engagement/"
        )

        logger.info("MEDIA engagement written successfully")


        # =================================================
        # 3️⃣ VISITORS (Current-State)
        # =================================================
        logger.info("Processing VISITORS")

        visitors_df = (
            spark.read
            .option("multiline", "true")
            .json(VISITORS_PATH)
        )

        visitors_flat = (
            visitors_df
            .select(
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
            f"s3://{SILVER_BUCKET}/fact_visitors/"
        )

        logger.info("VISITORS written successfully")

        logger.info("===== Project4 Bronze → Silver Job Completed Successfully =====")

    except Exception:
        logger.exception("❌ Project4 Bronze → Silver Job Failed")
        raise


if __name__ == "__main__":
    main()
