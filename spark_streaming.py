import os

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    DoubleType,
    LongType,
    StringType,
    StructField,
    StructType,
)


def build_spark():
    default_packages = ",".join(
        [
            "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1",
            "org.apache.hadoop:hadoop-aws:3.3.4",
            "com.amazonaws:aws-java-sdk-bundle:1.12.569",
        ]
    )
    kafka_package = os.getenv("SPARK_PACKAGES", default_packages)
    return (
        SparkSession.builder.appName("EcommerceStreaming")
        .config("spark.jars.packages", kafka_package)
        .getOrCreate()
    )


def main():
    bootstrap_servers = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
    topic = os.getenv("KAFKA_TOPIC", "ecommerce_events")
    high_value_threshold = float(os.getenv("HIGH_VALUE_THRESHOLD", "750"))
    click_burst_threshold = int(os.getenv("CLICK_BURST_THRESHOLD", "20"))
    abandon_threshold = int(os.getenv("ABANDON_THRESHOLD", "50"))
    minio_endpoint = os.getenv("MINIO_ENDPOINT", "http://minio:9000")
    minio_access_key = os.getenv("MINIO_ACCESS_KEY", "minioadmin")
    minio_secret_key = os.getenv("MINIO_SECRET_KEY", "minioadmin")
    minio_bucket = os.getenv("MINIO_BUCKET", "ecommerce")
    minio_anomaly_path = os.getenv(
        "MINIO_ANOMALY_PATH", f"s3a://{minio_bucket}/anomalies"
    )
    checkpoint_dir = os.getenv(
        "CHECKPOINT_DIR", "/opt/spark-app/checkpoints/anomalies"
    )

    spark = build_spark()
    spark.sparkContext.setLogLevel("WARN")

    h_conf = spark._jsc.hadoopConfiguration()
    h_conf.set("fs.s3a.endpoint", minio_endpoint)
    h_conf.set("fs.s3a.access.key", minio_access_key)
    h_conf.set("fs.s3a.secret.key", minio_secret_key)
    h_conf.set("fs.s3a.path.style.access", "true")
    h_conf.set("fs.s3a.connection.ssl.enabled", "false")
    h_conf.set("fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
    h_conf.set(
        "fs.s3a.aws.credentials.provider",
        "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider",
    )

    schema = StructType(
        [
            StructField("event_id", StringType(), True),
            StructField("event_type", StringType(), True),
            StructField("user_id", StringType(), True),
            StructField("product_id", StringType(), True),
            StructField("category", StringType(), True),
            StructField("price", DoubleType(), True),
            StructField("quantity", LongType(), True),
            StructField("session_id", StringType(), True),
            StructField("device", StringType(), True),
            StructField("country", StringType(), True),
            StructField("ts_ms", LongType(), True),
        ]
    )

    raw = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", bootstrap_servers)
        .option("subscribe", topic)
        .option("startingOffsets", "latest")
        .load()
    )

    parsed = (
        raw.selectExpr("CAST(value AS STRING) as payload")
        .select(F.from_json("payload", schema).alias("data"))
        .select("data.*")
        .filter(F.col("event_id").isNotNull())
    )

    events = (
        parsed.withColumn(
            "event_ts", F.from_unixtime(F.col("ts_ms") / 1000).cast("timestamp")
        )
        .withColumn("amount", (F.col("price") * F.col("quantity")).cast("double"))
        .withWatermark("event_ts", "2 minutes")
    )

    high_value = events.filter(
        (F.col("event_type") == "purchase")
        & (F.col("amount") >= high_value_threshold)
    )

    burst_clicks = (
        events.filter(F.col("event_type") == "click")
        .groupBy(F.window("event_ts", "1 minute"), F.col("user_id"))
        .count()
        .filter(F.col("count") >= click_burst_threshold)
    )

    abandon_spike = (
        events.filter(F.col("event_type") == "abandon")
        .groupBy(F.window("event_ts", "5 minutes"))
        .count()
        .filter(F.col("count") >= abandon_threshold)
    )

    high_value_out = high_value.select(
        F.col("event_ts").cast("string").alias("anomaly_ts"),
        F.col("user_id"),
        F.col("event_id"),
        F.col("amount").alias("metric"),
        F.lit("high_value_purchase").alias("anomaly_type"),
    )

    burst_out = burst_clicks.select(
        F.col("window.start").cast("string").alias("anomaly_ts"),
        F.col("user_id"),
        F.lit(None).cast("string").alias("event_id"),
        F.col("count").cast("double").alias("metric"),
        F.lit("burst_clicks").alias("anomaly_type"),
    )

    abandon_out = abandon_spike.select(
        F.col("window.start").cast("string").alias("anomaly_ts"),
        F.lit(None).cast("string").alias("user_id"),
        F.lit(None).cast("string").alias("event_id"),
        F.col("count").cast("double").alias("metric"),
        F.lit("abandon_spike").alias("anomaly_type"),
    )

    anomalies = high_value_out.unionByName(burst_out).unionByName(abandon_out)

    console_query = (
        anomalies.writeStream.format("console")
        .outputMode("append")
        .option("truncate", "false")
        .start()
    )

    minio_query = (
        anomalies.writeStream.format("parquet")
        .outputMode("append")
        .option("path", minio_anomaly_path)
        .option("checkpointLocation", checkpoint_dir)
        .start()
    )

    spark.streams.awaitAnyTermination()


if __name__ == "__main__":
    main()
