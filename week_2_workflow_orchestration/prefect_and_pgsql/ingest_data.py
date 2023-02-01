import os
import argparse

from time import time
from datetime import timedelta

import pandas as pd
from sqlalchemy import create_engine
from prefect import flow,task
from prefect.tasks import task_input_hash


@task(log_prints=True,retries = 3, cache_key_fn=task_input_hash, cache_expiration=timedelta(days=1))
def extract_data(url):
    # the backup files are gzipped, and it's important to keep the correct extension
    # for pandas to be able to open the file
    print("Converting csv")

    if url.endswith('.csv.gz'):
        csv_name = 'output.csv.gz'
    else:
        csv_name = 'output.csv'

    os.system(f"wget {url} -O {csv_name}")

    df_iter = pd.read_csv(csv_name, iterator=True, chunksize=100000)

    df = next(df_iter)

    df.tpep_pickup_datetime = pd.to_datetime(df.tpep_pickup_datetime)
    df.tpep_dropoff_datetime = pd.to_datetime(df.tpep_dropoff_datetime)
    return df

@task(log_prints=True)
def transform_data(df):
    print(f"pre: missing passenger count: {df['passenger_count'].isin([0]).sum()}")
    df = df[df['passenger_count'] != 0]
    print(f"pre: missing passenger count after trasnformation: {df['passenger_count'].isin([0]).sum()}")
    return df

@task(log_prints=True,retries = 3)
def ingest_data(user, password, host, port, db, table_name, df):   

    engine = create_engine(f'postgresql://{user}:{password}@{host}:{port}/{db}')

    df.head(n=0).to_sql(name=table_name, con=engine, if_exists='replace')

    df.to_sql(name=table_name, con=engine, if_exists='append')


@flow(name="Ingest Flow")
def main_flow(table_name: str):
    user = "root"
    password = "root"
    host = "localhost"
    port = "5432"
    db = "ny_taxi"
    url = "https://github.com/DataTalksClub/nyc-tlc-data/releases/download/yellow/yellow_tripdata_2021-01.csv.gz"

    raw_data_df = extract_data(url)
    data = transform_data(raw_data_df)
    ingest_data(user, password, host, port, db, table_name, data)

if __name__ == '__main__':
    main_flow("yellow_taxi_trips")