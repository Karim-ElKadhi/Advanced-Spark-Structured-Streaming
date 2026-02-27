
from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField,
    StringType, DoubleType,
)


EVENT_SCHEMA = StructType([
    StructField("device_id",   StringType(), nullable=True),
    StructField("event_time",  StringType(), nullable=True),
    StructField("temperature", DoubleType(), nullable=True),  
    StructField("country",     StringType(), nullable=True),
])


def _device_id_ok():
    return F.col("device_id").isNotNull() & (F.trim(F.col("device_id")) != "")


def _event_time_ok():
    return (
        F.col("event_time").isNotNull()
        & F.to_timestamp(F.col("event_time")).isNotNull()
    )


def _temperature_ok():
    return (
        F.col("temperature").isNotNull()
        & (F.col("temperature") != -999.0)
    )


def _country_ok():
    return (
        F.col("country").isNotNull()
        & (F.trim(F.col("country")) != "")
        & ~F.col("country").rlike("^[0-9]+$")
        & ~F.col("country").rlike("[^a-zA-Z\\s\\-]")
    )


def _is_valid():
    return _device_id_ok() & _event_time_ok() & _temperature_ok() & _country_ok()



def add_parsed_timestamp(df: DataFrame) -> DataFrame:
    return df.withColumn("event_ts", F.to_timestamp(F.col("event_time")))


def split_valid_invalid(df: DataFrame):
    valid_df = df.filter(_is_valid())

    reason = (
        F.when(~_device_id_ok(),  F.lit("bad_device_id"))
         .when(~_event_time_ok(), F.lit("bad_event_time"))
         .when(~_temperature_ok(),F.lit("bad_temperature"))
         .when(~_country_ok(),    F.lit("bad_country"))
         .otherwise(F.lit("unknown"))
    )

    invalid_df = df.filter(~_is_valid()).withColumn("invalid_reason", reason)

    return valid_df, invalid_df