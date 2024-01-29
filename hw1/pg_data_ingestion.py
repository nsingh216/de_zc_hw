import pandas as pd
from sqlalchemy import create_engine
import warnings


def load_data():
    # establish connection to my pg instance (running on docker)
    engine = create_engine(url='postgresql://root:root@localhost:5432/ny_taxi')
    engine.connect()

    # create an iterator to read 100k lines at a time
    df_iter = pd.read_csv(
        "./green_tripdata_2019-09.csv.gz",
        parse_dates=True,
        iterator=True,
        chunksize=100000
    )

    while True:
        # keep getting next chunk while more records exist
        df_chunk = next(df_iter)
        df_chunk.lpep_pickup_datetime = pd.to_datetime(df_chunk.lpep_pickup_datetime)
        df_chunk.lpep_dropoff_datetime = pd.to_datetime(df_chunk.lpep_dropoff_datetime)

        df_chunk.to_sql(name='green_taxi_data_t2', con=engine, if_exists='append')

        print("A chuck has been added")


def load_borough_data():
    # establish connection to my pg instance (running on docker)
    engine = create_engine(url='postgresql://root:root@localhost:5432/ny_taxi')
    engine.connect()

    # create an iterator to read 100k lines at a time
    df = pd.read_csv(
        "./taxi+_zone_lookup.csv",
        parse_dates=True
    )

    df.to_sql(name='borough_map', con=engine, if_exists='append')

    print("Map table has been created.")


def get_record_count():
    """
    Question 3: Get the record count

    Tip: started and finished on 2019-09-18.

    """

    print("Running query for question #3...")

    sql_query = """
         SELECT COUNT(*) AS num_taxi_trips
         FROM public.green_taxi_data_t2 
         WHERE CAST(lpep_pickup_datetime AS DATE) = '2019-09-18'
           AND CAST(lpep_dropoff_datetime AS DATE) = '2019-09-18';
    """

    return sql_query


def get_longest_trip_each_data():
    """
    Pickup date with the longs trip

    2019-09-18
    2019-09-16
    2019-09-26
    2019-09-21

    """
    print("\nRunning query for question #4...")

    sql_query = """
         SELECT CAST(lpep_pickup_datetime AS DATE) AS pickup_day,
                MAX(trip_distance) AS max_daily_trip_distance
         FROM public.green_taxi_data_t2 
         WHERE CAST(lpep_pickup_datetime AS DATE) IN ('2019-09-18', '2019-09-16', '2019-09-26', '2019-09-21')
         GROUP BY pickup_day
         ORDER BY 2 DESC
         LIMIT 1;
    """

    return sql_query


def get_biggest_pickup_boroughs():
    """
    Which were the 3 pick up Boroughs that had a sum of total_amount superior to 50000
    on '2019-09-18' and ignoring Borough has Unknown


    """
    print("\nRunning query for question #5...")

    sql_query = """
			 SELECT "Borough" AS borough, 
		             SUM(total_amount) AS daily_total_amount
			   FROM green_taxi_data_t2 AS taxi
			  INNER JOIN borough_map AS mp -- join to map table to get the pick up borough from location id
			     ON taxi."PULocationID" = mp."LocationID"
			  WHERE CAST(lpep_pickup_datetime AS DATE) = ('2019-09-18')
			    AND mp."LocationID" < 264 -- ignore unknown borough
              GROUP BY borough -- want top boroughs
		     HAVING SUM(total_amount) > 50000 -- want sum of total_amount superior to 50k
             ORDER BY 2 DESC;
    """

    return sql_query


def get_largest_tip():

    """
    For the passengers picked up in September 2019 in the zone name Astoria
    which was the drop off zone that had the largest tip?
    We want the name of the zone, not the id.
    """

    print("\nRunning query for question #6...")

    sql_query = """
			 SELECT mdo."Zone", tip_amount
			   FROM green_taxi_data_t2 AS taxi
			  INNER JOIN borough_map AS mpu -- join to map table to get the pick up zone from location id
			     ON taxi."PULocationID" = mpu."LocationID"
			  INNER JOIN borough_map AS mdo -- join to map table to get the drop off zone from location id
			     ON taxi."DOLocationID" = mdo."LocationID"
			  WHERE CAST(lpep_pickup_datetime AS DATE) BETWEEN '2019-09-01' AND '2019-09-30' -- in Sept 2019
			    AND mpu."Zone" = 'Astoria' -- picked up in Zone Astoria
              ORDER BY 2 DESC
			  LIMIT 1;
        """

    return sql_query


def main(load=False):
    # just for clean output
    warnings.filterwarnings('ignore')

    # PREP: only run these loads once, commenting to run additional queries
    if load:
        load_data()
        load_borough_data()

    # establish connection to my pg instance (running on docker)
    engine = create_engine(url='postgresql://root:root@localhost:5432/ny_taxi')
    engine.connect()

    # Q3
    sql_query = get_record_count()
    print(pd.read_sql(sql_query, con=engine))

    # Q4
    sql_query = get_longest_trip_each_data()
    print(pd.read_sql(sql_query, con=engine))

    # Q5
    sql_query = get_biggest_pickup_boroughs()
    print(pd.read_sql(sql_query, con=engine))

    # Q6
    sql_query = get_largest_tip()
    print(pd.read_sql(sql_query, con=engine))


main()
