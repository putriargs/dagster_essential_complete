import requests
from dagster_essentials.defs.assets import constants
from dagster_duckdb import DuckDBResource
import dagster as dg
import duckdb
import os
from dagster._utils.backoff import backoff

@dg.asset
def taxi_trips_file() -> None:
    """
    Fetches the taxi trips data for a specific month and saves it to a local file."""
    month_to_fetch = '2023-03'
    raw_trips = requests.get(
        f"https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_{month_to_fetch}.parquet"
    )

    with open(constants.TAXI_TRIPS_TEMPLATE_FILE_PATH.format(month_to_fetch), "wb") as output_file:
        output_file.write(raw_trips.content)

@dg.asset(
    deps=["taxi_trips_file"]
)
def taxi_trips(database: DuckDBResource) -> None:
    """
    Reads the taxi trips data from the local file and creates a table in DuckDB."""
    query = """
        create or replace table trips as (
            select
                VendorID as vendor_id,
                PULocationID as pickup_zone_id,
                DOLocationID as dropoff_zone_id,
                RatecodeID as rate_code_id,
                payment_type as payment_type,
                tpep_dropoff_datetime as dropoff_datetime,
                tpep_pickup_datetime as pickup_datetime,
                trip_distance as trip_distance,
                passenger_count as passenger_count,
                total_amount as total_amount
            from 'data/raw/taxi_trips_2023-03.parquet'
        );
    """

    with database.get_connection() as conn:
        conn.execute(query)
    

@dg.asset
def taxi_zones_file() -> None:
    """
    Fetches the taxi zones data and saves it to a local file."""
    raw_zones = requests.get(
        f"https://community-engineering-artifacts.s3.us-west-2.amazonaws.com/dagster-university/data/taxi_zones.csv"
    )

    with open(constants.TAXI_ZONES_FILE_PATH, "wb") as output_file:
        output_file.write(raw_zones.content)

@dg.asset(
    deps=["taxi_zones_file"]
)
def taxi_zones(database: DuckDBResource) -> None:
    """
    Reads the taxi zones data from the local file and creates a table in DuckDB."""
    query = """
        create or replace table zones as (
            select
                LocationID as zone_id,
                zone as zone,
                borough as borough,
                the_geom as geometry
            from 'data/raw/taxi_zones.csv'
        );
    """

    with database.get_connection() as conn:
        conn.execute(query)