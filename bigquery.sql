-- Creatr external table referring to the 2024 Parquet files
CREATE OR REPLACE EXTERNAL TABLE `taxi-rides-ny-486815.nytaxi.external_yellow_tripdata`
OPTIONS (
  format = 'PARQUET',
  uris = ['gs://data-warehouse-2026-nyc-taxi/yellow_tripdata_2024-*.parquet']
);

-- Create a regular (non-partitioned, non-clustered) table
CREATE OR REPLACE TABLE `taxi-rides-ny-486815.nytaxi.yellow_tripdata_2024_regular` AS
SELECT * FROM `taxi-rides-ny-486815.nytaxi.external_yellow_tripdata`;

-- Create a partitioned, clustered table
CREATE OR REPLACE TABLE `taxi-rides-ny-486815.nytaxi.yellow_tripdata_2024_optimized`
PARTITION BY DATE(tpep_dropoff_datetime)
CLUSTER BY VendorID AS
SELECT * FROM `taxi-rides-ny-486815.nytaxi.external_yellow_tripdata`;