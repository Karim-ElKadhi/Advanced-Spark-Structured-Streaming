import os
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from schema import EVENT_SCHEMA, add_parsed_timestamp, split_valid_invalid


# ─────────────────────────────────────────────
# Config
# ─────────────────────────────────────────────

KAFKA_BOOTSTRAP = "localhost:9092"
INPUT_TOPIC     = "sensor-events"
VALID_TOPIC     = "valid-events"
INVALID_TOPIC   = "invalid-events"

CHECKPOINT_BASE = "/spark_checkpoints"
OUTPUT_BASE     = "/spark_output"

WATERMARK_DELAY = "10 minutes"
WINDOW_DURATION = "10 minutes"
WINDOW_SLIDE    = "5 minutes"


def build_spark():
    return (
        SparkSession.builder
        .appName("SensorStreamLab")
        .master("local[*]")
        .config("spark.sql.shuffle.partitions", "4")
        .getOrCreate()
    )

def read_kafka(spark):
    return (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP)
        .option("subscribe", INPUT_TOPIC)
        .option("startingOffsets", "earliest")
        .load()
        .selectExpr("CAST(value AS STRING) AS raw_json")
    )


def parse_json(df):
    return (
        df.withColumn("data", F.from_json("raw_json", EVENT_SCHEMA))
        .select(
            "raw_json",
            F.col("data.device_id").alias("device_id"),
            F.col("data.event_time").alias("event_time"),
            F.col("data.temperature").alias("temperature"),
            F.col("data.country").alias("country"),
        )
    )


# Aggregations

def avg_temp_per_device(df):
    return (
        df.withWatermark("event_ts", WATERMARK_DELAY)
        .groupBy(
            F.window("event_ts", WINDOW_DURATION, WINDOW_SLIDE),
            "device_id"
        )
        .agg(
            F.round(F.avg("temperature"), 2).alias("avg_temperature"),
            F.count("*").alias("event_count")
        )
        .select(
            F.col("window.start").alias("window_start"),
            F.col("window.end").alias("window_end"),
            "device_id",
            "avg_temperature",
            "event_count"
        )
    )


def events_per_country(df):
    return (
        df.withWatermark("event_ts", WATERMARK_DELAY)
        .groupBy(
            F.window("event_ts", WINDOW_DURATION, WINDOW_SLIDE),
            "country"
        )
        .count()
        .select(
            F.col("window.start").alias("window_start"),
            F.col("window.end").alias("window_end"),
            "country",
            F.col("count").alias("event_count")
        )
    )


# Main

def main():
    spark = build_spark()
    spark.sparkContext.setLogLevel("WARN")

    # 1. Read
    raw_df = read_kafka(spark)

    # 2. Parse
    parsed_df = parse_json(raw_df)
    parsed_df = add_parsed_timestamp(parsed_df)

    # 3. Split
    valid_df, invalid_df = split_valid_invalid(parsed_df)


    valid_df.writeStream \
        .format("console") \
        .option("truncate", "false") \
        .option("checkpointLocation", f"{CHECKPOINT_BASE}/valid_console") \
        .start()

    valid_df.writeStream \
        .format("json") \
        .option("path", os.path.join(OUTPUT_BASE, "valid")) \
        .option("checkpointLocation", f"{CHECKPOINT_BASE}/valid_file") \
        .start()

    valid_df.select(F.to_json(F.struct("*")).alias("value")) \
        .writeStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP) \
        .option("topic", VALID_TOPIC) \
        .option("checkpointLocation", f"{CHECKPOINT_BASE}/valid_kafka") \
        .start()

    # Aggregations
    device_agg = avg_temp_per_device(valid_df)

    device_agg.writeStream \
        .outputMode("update") \
        .format("console") \
        .option("checkpointLocation", f"{CHECKPOINT_BASE}/device_console") \
        .start()

    device_agg.writeStream \
        .format("parquet") \
        .option("path", os.path.join(OUTPUT_BASE, "agg_device")) \
        .option("checkpointLocation", f"{CHECKPOINT_BASE}/device_file") \
        .start()

    country_agg = events_per_country(valid_df)

    country_agg.writeStream \
        .outputMode("update") \
        .format("console") \
        .option("checkpointLocation", f"{CHECKPOINT_BASE}/country_console") \
        .start()

    country_agg.writeStream \
        .format("parquet") \
        .option("path", os.path.join(OUTPUT_BASE, "agg_country")) \
        .option("checkpointLocation", f"{CHECKPOINT_BASE}/country_file") \
        .start()


    invalid_df.writeStream \
        .format("console") \
        .option("truncate", "false") \
        .option("checkpointLocation", f"{CHECKPOINT_BASE}/invalid_console") \
        .start()

    invalid_df.writeStream \
        .format("json") \
        .option("path", os.path.join(OUTPUT_BASE, "invalid")) \
        .option("checkpointLocation", f"{CHECKPOINT_BASE}/invalid_file") \
        .start()

    invalid_df.select(F.to_json(F.struct("*")).alias("value")) \
        .writeStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP) \
        .option("topic", INVALID_TOPIC) \
        .option("checkpointLocation", f"{CHECKPOINT_BASE}/invalid_kafka") \
        .start()

    spark.streams.awaitAnyTermination()


if __name__ == "__main__":
    main()