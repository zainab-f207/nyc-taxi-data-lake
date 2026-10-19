-- Athena queries for the NYC taxi data lake.
-- Run each statement as its own query, in this order. Database: nyc_taxi.
-- Replace the bucket name with your own.

-- 1. Silver table (one column per field, plus 7 quality flags)
CREATE EXTERNAL TABLE nyc_taxi.yellow_silver (
  VendorID int,
  tpep_pickup_datetime timestamp,
  tpep_dropoff_datetime timestamp,
  passenger_count bigint,
  trip_distance double,
  RatecodeID bigint,
  store_and_fwd_flag string,
  PULocationID int,
  DOLocationID int,
  payment_type bigint,
  fare_amount double,
  extra double,
  mta_tax double,
  tip_amount double,
  tolls_amount double,
  improvement_surcharge double,
  total_amount double,
  congestion_surcharge double,
  Airport_fee double,
  cbd_congestion_fee double,
  is_flex_fare boolean,
  invalid_trip_time boolean,
  negative_fare boolean,
  negative_total boolean,
  zero_distance boolean,
  distance_outlier boolean,
  pickup_outside_2025 boolean
)
PARTITIONED BY (pickup_month string)
STORED AS PARQUET
LOCATION 's3://zainab-nyc-taxi-lake-555/silver/yellow_taxi/';

-- 2. Tell Athena about the month folders
MSCK REPAIR TABLE nyc_taxi.yellow_silver;

-- 3. Check: should return 48722602
SELECT COUNT(*) FROM nyc_taxi.yellow_silver;

-- 4. Gold table (the gold folder in S3 must be empty first)
CREATE TABLE nyc_taxi.gold_monthly_summary
WITH (format='PARQUET',
      external_location='s3://zainab-nyc-taxi-lake-555/gold/monthly_summary/') AS
SELECT
  pickup_month,
  COUNT(*) AS trips,
  ROUND(SUM(CASE WHEN negative_total = false THEN total_amount ELSE 0 END), 2) AS gross_revenue,
  ROUND(SUM(total_amount), 2) AS net_revenue,
  ROUND(AVG(CASE WHEN distance_outlier = false THEN trip_distance END), 2) AS avg_distance_miles,
  ROUND(approx_percentile(trip_distance, 0.5), 2) AS median_distance_miles,
  ROUND(AVG(date_diff('minute', tpep_pickup_datetime, tpep_dropoff_datetime)), 1) AS avg_duration_min,
  approx_percentile(date_diff('minute', tpep_pickup_datetime, tpep_dropoff_datetime), 0.5) AS median_duration_min,
  ROUND(100.0 * SUM(CASE WHEN is_flex_fare THEN 1 ELSE 0 END) / COUNT(*), 1) AS flex_fare_pct
FROM nyc_taxi.yellow_silver
WHERE invalid_trip_time = false
  AND pickup_outside_2025 = false
GROUP BY pickup_month;

SELECT * FROM nyc_taxi.gold_monthly_summary ORDER BY pickup_month;

-- 5. Cost experiment (read "Data scanned" in Athena's Recent queries tab)
-- Query 1: one column, all months
SELECT SUM(total_amount) FROM nyc_taxi.yellow_silver;

-- Query 2: one column, filtered on the PARTITION column
SELECT SUM(total_amount) FROM nyc_taxi.yellow_silver
WHERE pickup_month = '2025_01';

-- Query 3: one column, same month, filtered on the TIMESTAMP column
SELECT SUM(total_amount) FROM nyc_taxi.yellow_silver
WHERE tpep_pickup_datetime >= TIMESTAMP '2025-01-01 00:00:00'
  AND tpep_pickup_datetime <  TIMESTAMP '2025-02-01 00:00:00';

-- Query 4: four columns, all months
SELECT SUM(total_amount), SUM(tip_amount), SUM(fare_amount), SUM(trip_distance)
FROM nyc_taxi.yellow_silver;
