CREATE EXTERNAL TABLE `dtc-de-course-410814.nytaxi2022.green_data`
OPTIONS (
  format = 'PARQUET',
  uris = ['gs://ny_taxi2022_hw3/green_tripdata_2022-*.parquet']
);

CREATE OR REPLACE TABLE `dtc-de-course-410814.nytaxi2022.green_data_non_partitoned` AS
SELECT * FROM `dtc-de-course-410814.nytaxi2022.green_data`;

select count(1) from dtc-de-course-410814.nytaxi2022.green_data_non_partitoned;

#external table
SELECT count(DISTINCT(PULocationID))
FROM `dtc-de-course-410814.nytaxi2022.green_data`;
#table
SELECT count(DISTINCT(PULocationID))
FROM `dtc-de-course-410814.nytaxi2022.green_data_non_partitoned`;

select count(1)
from `dtc-de-course-410814.nytaxi2022.green_data_non_partitoned`
where fare_amount = 0;

#Partition by lpep_pickup_datetime Cluster on PUlocationID,7s&114.11MB
CREATE OR REPLACE TABLE dtc-de-course-410814.nytaxi2022.green_data_partitoned_clustered
PARTITION BY date(lpep_pickup_datetime)
CLUSTER BY PUlocationID AS
SELECT * FROM dtc-de-course-410814.nytaxi2022.green_data;


SELECT DISTINCT PULocationID
FROM dtc-de-course-410814.nytaxi2022.green_data_non_partitoned
WHERE lpep_pickup_datetime >= TIMESTAMP('2022-06-01')
  AND lpep_pickup_datetime < TIMESTAMP('2022-07-01');

SELECT DISTINCT PULocationID
FROM dtc-de-course-410814.nytaxi2022.green_data_partitoned_clustered
WHERE lpep_pickup_datetime >= TIMESTAMP('2022-06-01')
  AND lpep_pickup_datetime < TIMESTAMP('2022-07-01');
