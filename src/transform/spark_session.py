import os
import sys
from typing import Optional
from dotenv import load_dotenv

# 1. Load any environment variables from .env
load_dotenv()

# 2. Critical for Windows: Ensure PySpark worker and driver use the same Python interpreter
os.environ["PYSPARK_PYTHON"] = sys.executable
os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable
from pyspark.sql import SparkSession

def is_cloud_environment() -> bool:
    """Detects whether code is executing inside AWS Glue, EMR, or Lambda."""
    return any(
        var in os.environ
        for var in ["GLUE_COMMAND_CRITERIA", "AWS_EXECUTION_ENV", "JOB_NAME"]
    )
def get_spark_session(
    app_name: str = "SpotifyBronzeTransformation",
    master: Optional[str] = None
) -> SparkSession:
    """Create or retrieves an active SparkSession with optimized defaults."""
    # If in AWS Glue / Cloud, retrieve the managed session
    if is_cloud_environment():
        return SparkSession.builder.appName(app_name).getOrCreate()
    # Otherwise configure local SparkSession
    spark = (
        SparkSession.builder
        .appName(app_name)
        .master(master or "local[*]")
        .config("spark.driver.bindAddress", "127.0.0.1")
        .config("spark.ui.enabled", "false")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.parquet.compression.codec", "snappy")
        .config(
            "spark.sql.sources.partitionOverwriteMode", "dynamic"
        )  
        .getOrCreate()
    )

    # Clean terminal output by showing only WARN and ERROR logs
    spark.sparkContext.setLogLevel("WARN")

    return spark


if __name__ == "__main__":
    print("=" * 60)
    print("⚡ Testing PySpark Session Initialization...")
    print("=" * 60)

    spark = get_spark_session(app_name="Spotify_Spark_Test")
    print(f"✓ Spark App Name : {spark.sparkContext.appName}")
    print(f"✓ Spark Version  : {spark.version}")
    print(f"✓ Master Node    : {spark.sparkContext.master}")

    # Create a quick 1-row test DataFrame
    test_df = spark.createDataFrame([("06HL4z0CvFAxyc27GXpf02", "Taylor Swift")], ["artist_id", "artist_name"])
    print("\n✓ Sample DataFrame:")
    test_df.show(truncate=False)

    spark.stop()
    print("✓ SparkSession stopped cleanly.")
