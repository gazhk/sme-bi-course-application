# Virtual Machine (VM) Exercises



## 1st VM Exercise

#### Learning Objective

Students will be able to implement additional quality checks and create new columns in the Silver layer of a multi-hop architecture using Spark.

#### Context

Quality checks are vital in data processing to ensure that the data is accurate and reliable before it is used for analysis. In the Silver layer, you can enrich your dataset by adding new columns that provide more context or insights. For example, adding calculated fields like trip duration and total fare can significantly enhance the possibility of identifying invalid trips. This exercise will help students understand how to improve data integrity and prepare datasets for meaningful analysis.

#### Steps to be executed by the student (max 6)

- Add a new column named trip_duration that calculates the difference between tpep_dropoff_datetime and tpep_pickup_datetime in Silver stream dataframe.
- Create another column named total_fare that sums up fare_amount, tip_amount, and any additional charges (like extra).
- Implement a quality check by filtering out records where trip_duration is less than zero, indicating invalid trips.
- Write enriched data to Silver Delta table.
- Count number of records in Silver table.
  
#### Exercise question:
How many records have been loaded to silver table after removing invalid trip?

#### End goal:

Data loaded into Silver table: 999 records.

## 2nd VM Exercise


#### Learning Objective

Students will be able to create additional aggregated measures in the Gold layer of a multi-hop architecture using Spark.

#### Context

In the Gold layer, data aggregation is crucial for deriving insights that drive business decisions. By creating additional measures, such as average tip per day or total fare per day, organizations can better understand their operations and customer behavior. This exercise will help students learn how to enhance their reporting capabilities by implementing meaningful metrics that can inform strategic decisions.

#### Steps to be executed by the student (max 6)

*Each bulleted instruction is a complete sentence that describes a specific task.*

- Group the data by date using date_format(tpep_pickup_datetime, "yyyy-MM-dd") to prepare for daily summaries.
- Calculate total revenue per day by summing up total_amount and naming this measure total_revenue.
- Calculate average tip per day by taking average of tip_amount and naming this measure average_tip.
- Calculate total trip distance per day by by summing up trip_distance and naming this measure total_distance.

#### Exercise question:
What was the total fare on 2024-06-28 (Give us the figure rounded up to the whole number)?

#### End goal:

Total fare on 2024-06-28 was 731. 

