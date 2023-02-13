CREATE OR REPLACE EXTERNAL TABLE `my-rides-antonis.trips_data_all.external_fhv_tripdata`
OPTIONS (
  format = 'csv',
  uris = ['gs://dtc_data_lake_my-rides-antonis/data/fhv/*.csv.gz']
);


SELECT * FROM `my-rides-antonis.trips_data_all.external_fhv_tripdata` LIMIT 10;
#Q1
SELECT count('dispatching_base_num') FROM `my-rides-antonis.trips_data_all.external_fhv_tripdata`;

#Q2

SELECT COUNT ( DISTINCT dispatching_base_num )
FROM `my-rides-antonis.trips_data_all.external_fhv_tripdata`;

SELECT COUNT ( DISTINCT dispatching_base_num ) 
FROM `my-rides-antonis.trips_data_all.internal_fhv_tripdata`;

#Q3 How many records have both a blank (null) PUlocationID and DOlocationID in the entire dataset?
SELECT Count(PUlocationID) FROM `my-rides-antonis.trips_data_all.external_fhv_tripdata`
WHERE PUlocationID IS NULL;

SELECT Count(*) FROM `my-rides-antonis.trips_data_all.internal_fhv_tripdata`
WHERE PUlocationID IS NULL and DOlocationID IS NULL;


#Q4

CREATE OR REPLACE TABLE `my-rides-antonis.trips_data_all.internal_fhv_tripdata_partitioned`
PARTITION BY DATE(dropoff_datetime)
CLUSTER BY dispatching_base_num AS (
  SELECT * FROM `my-rides-antonis.trips_data_all.internal_fhv_tripdata`
);

#Q5

SELECT count(*) FROM  `my-rides-antonis.trips_data_all.internal_fhv_tripdata_partitioned`
WHERE dropoff_datetime BETWEEN '2019-03-01' AND '2019-03-31';


SELECT count(*) FROM `my-rides-antonis.trips_data_all.external_fhv_tripdata`
WHERE dropoff_datetime BETWEEN '2019-03-01' AND '2019-03-31';

