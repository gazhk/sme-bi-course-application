# Databricks notebook source
from pyspark.sql.functions import col, unix_timestamp

# Read from the Bronze table as a streaming DataFrame
bronze_stream_df = spark.readStream.table(bronze_table)

# Enrich raw data from the Bronze stream with additional columns and filtering
silver_stream_df = bronze_stream_df.select(
    "VendorID",
    "tpep_pickup_datetime",
    "tpep_dropoff_datetime",
    "passenger_count",
    "trip_distance",
    "RatecodeID",
    "store_and_fwd_flag",
    "PULocationID",
    "DOLocationID",
    "payment_type",
    "fare_amount",
    "extra",
    "mta_tax",
    "tip_amount",
    "tolls_amount",
    "improvement_surcharge",
    "total_amount",
    "congestion_surcharge",
    "Airport_fee",
    current_timestamp().alias("arrival_time"),
    input_file_name().alias("source_file")
).filter(col("VendorID").isNotNull())  # Quality check for VendorID

