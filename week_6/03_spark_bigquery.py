#!/usr/bin/env python
# coding: utf-8

from pyspark.sql import SparkSession
import pyspark
from pyspark.sql import functions as F
import argparse


parser = argparse.ArgumentParser()


parser.add_argument("--input_green", required=True)
parser.add_argument("--input_yellow", required=True)
parser.add_argument("--output", required=True)

args = parser.parse_args()

input_green = args.input_green
input_yellow = args.input_yellow
output = args.output 

spark = SparkSession.builder \
        .getOrCreate()

spark.conf.set('temporaryGcsBucket', 'dataproc-temp-asia-south2-804887398925-tw6itu1y')

df_green = spark.read.parquet(input_green)
df_green.show()

df_yellow = spark.read.parquet(input_yellow)
df_yellow.show()
df_yellow.printSchema()


df_green = df_green.withColumnRenamed('lpep_pickup_datetime' , 'pickup_datetime') \
                    .withColumnRenamed('lpep_dropoff_datetime', 'dropoff_datetime')


df_yellow = df_yellow.withColumnRenamed('lpep_pickup_datetime' , 'pickup_datetime') \
                    .withColumnRenamed('lpep_dropoff_datetime', 'dropoff_datetime')
df_yellow.columns


# set(df_green.columns) & set(df_yellow.columns)

green_columns = set(df_yellow.columns)

common_columns = []
for col in df_yellow.columns:
    if col in green_columns: 
        common_columns.append(col)


df_green_sel = df_green \
    .select(common_columns) \
    .withColumn('service_type', F.lit('green')) 

df_yellow_sel = df_yellow \
    .select(common_columns) \
    .withColumn('service_type', F.lit('yellow')) 


df_tripdata = df_green_sel.unionAll(df_yellow_sel)
df_tripdata.groupBy('service_type').count().show()

df_tripdata.registerTempTable('trips_data')

spark.sql("""

select * from trips_data;
"""
         ).show()


df_result = spark.sql("""
SELECT 
    -- Reveneue grouping 
    PULocationID AS revenue_zone,
    date_trunc('month', pickup_datetime) AS revenue_month, 
    service_type, 
    -- Revenue calculation 
    SUM(fare_amount) AS revenue_monthly_fare,
    SUM(extra) AS revenue_monthly_extra,
    SUM(mta_tax) AS revenue_monthly_mta_tax,
    SUM(tip_amount) AS revenue_monthly_tip_amount,
    SUM(tolls_amount) AS revenue_monthly_tolls_amount,
    SUM(improvement_surcharge) AS revenue_monthly_improvement_surcharge,
    SUM(total_amount) AS revenue_monthly_total_amount,
    SUM(congestion_surcharge) AS revenue_monthly_congestion_surcharge,
    -- Additional calculations
    AVG(passenger_count) AS avg_montly_passenger_count,
    AVG(trip_distance) AS avg_montly_trip_distance
FROM
    trips_data
GROUP BY
    1, 2, 3
""")


df_result.write.format('bigquery') \
                .option('table',output) \
                .save()


# Everything is same except, we have to update how to result is written to bigquery.



# gcloud dataproc jobs submit pyspark \
#     --cluster=data-zoomcamp-cluster \
#     --region=asia-south2  \
#     --jars=gs://spark-lib/bigquery/spark-bigquery-with-dependencies_2.12-0.29.0.jar \
#     gs://dtc_data_lake_engaged-cosine-374921/code/03_spark_bigquery.py \
#     -- \
#     --input_green=gs://dtc_data_lake_engaged-cosine-374921/pq/green/2020/*/ \
#     --input_yellow=gs://dtc_data_lake_engaged-cosine-374921/pq/yellow/2020/*/ \
#     --output=spark_reports.report-2020