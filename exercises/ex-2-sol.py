# Databricks notebook source

from pyspark.sql.functions import col, current_timestamp, input_file_name, avg, count, sum, date_format

# Read from the Silver table as a streaming DataFrame
silver_stream_df = spark.readStream.table(silver_table)

# Aggregate data for the Gold layer (e.g., daily trip summaries)
gold_stream_df = silver_stream_df.groupBy(
    date_format(col("tpep_pickup_datetime"), "yyyy-MM-dd").alias("trip_date")
).agg(
    count("*").alias("total_trips"),
    sum("trip_distance").alias("total_distance"),
    sum("fare_amount").alias("total_fare"),
    avg("tip_amount").alias("average_tip")
)


# Write aggregated results to Gold Delta table
gold_query = (gold_stream_df.writeStream
              .format("delta")
              .outputMode("complete")  # Complete mode for aggregations
              .option("checkpointLocation", gold_checkpoint_path)
              .table(gold_table))


print(f"Streaming aggregated data into Gold table: {gold_table}")

display(spark.table(gold_table))
