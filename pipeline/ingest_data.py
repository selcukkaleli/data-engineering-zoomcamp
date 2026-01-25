#!/usr/bin/env python
# coding: utf-8


import pandas as pd
from sqlalchemy import create_engine
from tqdm.auto import tqdm
import click
import os


yellow_dtype = {
    "VendorID": "Int64",
    "passenger_count": "Int64",
    "trip_distance": "float64",
    "RatecodeID": "Int64",
    "store_and_fwd_flag": "string",
    "PULocationID": "Int64",
    "DOLocationID": "Int64",
    "payment_type": "Int64",
    "fare_amount": "float64",
    "extra": "float64",
    "mta_tax": "float64",
    "tip_amount": "float64",
    "tolls_amount": "float64",
    "improvement_surcharge": "float64",
    "total_amount": "float64",
    "congestion_surcharge": "float64"
}

green_dtype = {
    "VendorID": "Int64",
    "passenger_count": "Int64",
    "trip_distance": "float64",
    "RatecodeID": "Int64",
    "store_and_fwd_flag": "string",
    "PULocationID": "Int64",
    "DOLocationID": "Int64",
    "payment_type": "Int64",
    "fare_amount": "float64",
    "extra": "float64",
    "mta_tax": "float64",
    "tip_amount": "float64",
    "tolls_amount": "float64",
    "improvement_surcharge": "float64",
    "total_amount": "float64",
    "congestion_surcharge": "float64"
}

yellow_parse_dates = [
    "tpep_pickup_datetime",
    "tpep_dropoff_datetime"
]

green_parse_dates = ["lpep_pickup_datetime", "lpep_dropoff_datetime"]

zones_dtype = {
    "LocationID": "Int64",
    "Borough": "string",
    "Zone": "string",
    "service_zone": "string",
}

@click.command()
@click.option('--pg-user', default='root', help='PostgreSQL username')
@click.option('--pg-pass', default='root', help='PostgreSQL password')
@click.option('--pg-host', default='localhost', help='PostgreSQL host')
@click.option('--pg-port', default=5432, type=int, help='PostgreSQL port')
@click.option('--pg-db', default='ny_taxi', help='PostgreSQL database name')
@click.option('--year', default=2021, type=int, help='Year of taxi data')
@click.option('--month', default=1, type=int, help='Month of taxi data')
@click.option('--target-table', default='yellow_taxi_data', help='Target table name')
@click.option('--chunksize', default=100000, type=int, help='Chunk size for reading CSV')
@click.option('--url', default=None, help='If provided, ingest this URL instead of year/month taxi data')
@click.option('--is-zones', is_flag=True, help='Use taxi_zone_lookup schema (dtype) instead of yellow trips')

def run(pg_user, pg_pass, pg_host, pg_port, pg_db, year, month, target_table, chunksize, url, is_zones):


    if url is None:
        prefix = "https://github.com/DataTalksClub/nyc-tlc-data/releases/download/yellow"
        url = f"{prefix}/yellow_tripdata_{year}-{month:02d}.csv.gz"

    is_parquet = url.endswith(".parquet") or os.path.isfile(url) and url.lower().endswith(".parquet")

    engine = create_engine(f'postgresql://{pg_user}:{pg_pass}@{pg_host}:{pg_port}/{pg_db}')

    url_lower = url.lower()

    if is_zones:
        dtype = zones_dtype
        parse_dates = None

    elif "green" in url_lower:
        dtype = green_dtype
        parse_dates = green_parse_dates

    else:
        dtype = yellow_dtype
        parse_dates = yellow_parse_dates


    if is_parquet:
        df =pd.read_parquet(url)
        
        # Optional: If you want, you can parse date columns here for green/yellow.
        # For homework, loading as-is is fine.

        # Create table schema first (no rows)
        df.head(0).to_sql(name=target_table, con=engine, if_exists='replace', index=False)

        # Append in batches
        for i in tqdm(range(0, len(df), chunksize)):
            df_chunk = df.iloc[i:i + chunksize]
            df_chunk.to_sql(name=target_table, con=engine, if_exists='append', index=False)
    else:
        df_iter = pd.read_csv(
            url,
            dtype=dtype,
            parse_dates=parse_dates,
            iterator=True,
            chunksize=chunksize
            )
        first = True
        for df_chunk in tqdm(df_iter):
            if first:
                df_chunk.head(n=0).to_sql(name=target_table, con=engine, if_exists='replace') 
                first = False
            
            df_chunk.to_sql(name=target_table, con=engine,if_exists='append')

if __name__ == '__main__':
    run()