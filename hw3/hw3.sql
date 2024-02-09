CREATE OR REPLACE EXTERNAL TABLE green_taxi.green_taxi_external_table
  OPTIONS(
    FORMAT = 'PARQUET',
    URIS=['gs://de_zc/hw3/*.parquet']
  )
;

CREATE OR REPLACE TABLE green_taxi.green_taxi_int_table AS (
  SELECT *
  FROM green_taxi.green_taxi_external_table
)
;


/* QUESTION 1 */
SELECT COUNT(*)
FROM green_taxi.green_taxi_external_table;

SELECT COUNT(*)
FROM green_taxi.green_taxi_int_table;

/* QUESTION 2 */
SELECT COUNT(DISTINCT PULocationID)
FROM green_taxi.green_taxi_external_table;

SELECT COUNT(DISTINCT PULocationID)
FROM green_taxi.green_taxi_int_table;
 
/* QUESTION 3 */
SELECT COUNT(*)
FROM green_taxi.green_taxi_external_table
WHERE fare_amount = 0;

SELECT COUNT(*)
FROM green_taxi.green_taxi_int_table
WHERE fare_amount = 0;

/* QUESTION 4 
order by PUlocationID 
filter on lpep_pickup_datetime 
(reduce the amount of data filtered here by using lpep_pickup_datetime for partition)
*/
CREATE OR REPLACE TABLE green_taxi.green_taxi_partition
PARTITION BY DATE(lpep_pickup_datetime)
CLUSTER BY PULocationID AS (
  SELECT *
  FROM green_taxi.green_taxi_int_table
);

/* QUESTION 5 
06/01/2022 and 06/30/2022
*/

SELECT DISTINCT(PULocationID)
FROM green_taxi.green_taxi_int_table
WHERE DATE(lpep_pickup_datetime) BETWEEN '2022-06-01' AND '2022-06-30';


SELECT DISTINCT(PULocationID)
FROM green_taxi.green_taxi_partition
WHERE DATE(lpep_pickup_datetime) BETWEEN '2022-06-01' AND '2022-06-30';
