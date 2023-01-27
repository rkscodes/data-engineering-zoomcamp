#!/usr/bin/env python
# coding: utf-8

import os
import argparse

from time import time

import pandas as pd
from sqlalchemy import create_engine
from prefect import flow, task

from prefect.blocks.system import String


@task(log_prints=True, retries=3, cache_result_in_memory=True)
def extract_data(params):
    url = params.url

    if url.endswith('.csv.gz'):
        csv_name = 'output.csv.gz'
    else:
        csv_name = 'output.csv'

    os.system(f"wget {url} -O {csv_name}")
    df = pd.read_csv(csv_name)

    df.lpep_pickup_datetime = pd.to_datetime(df.lpep_pickup_datetime)
    df.lpep_dropoff_datetime = pd.to_datetime(df.lpep_dropoff_datetime)
    return df


@task(log_prints=True)
def transform_data(params, df):
    print(
        f"pre: missing passenger count: {df['passenger_count'].isin([0]).sum()}")
    df = df[df['passenger_count'] != 0]
    print(
        f"post: misssing passenger count: {df['passenger_count'].isin([0]).sum()}")
    return df


@task()
def load_data(params, df):
    user = params.user
    password = params.password
    host = params.host
    port = params.port
    db = params.db
    table_name = params.table_name
    url = params.url

    engine = create_engine(
        f'postgresql://{user}:{password}@{host}:{port}/{db}')

    df.to_sql(name=table_name, con=engine, if_exists='append')


@flow(name="Subflow")
def log_subflow(params):
    table_name = params.table_name
    print(f"Logging Subflow for:{table_name}")


@flow(name="Ingest Data")
def main_flow(params):

    log_subflow(args)
    data_raw = extract_data(args)
    transformed_data = transform_data(args, data_raw)
    load_data(args, transformed_data)


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Ingest CSV data to Postgres')

    parser.add_argument('--user', required=True, help='user name for postgres')
    parser.add_argument('--password', required=True,
                        help='password for postgres')
    parser.add_argument('--host', required=True, help='host for postgres')
    parser.add_argument('--port', required=True, help='port for postgres')
    parser.add_argument('--db', required=True,
                        help='database name for postgres')
    parser.add_argument('--table_name', required=True,
                        help='name of the table where we will write the results to')
    parser.add_argument('--url', required=True, help='url of the csv file')

    argv = eval(String.load("parameter").value)

    args = parser.parse_args(argv)
    main_flow(args)

# URL="https://github.com/DataTalksClub/nyc-tlc-data/releases/download/green/green_tripdata_2019-01.csv.gz"

# python ingest_data_flow.py \
# [
# [--user=root \],
# [--password=root \],
# [--host=localhost \],
# [--port=5432 \],
# [--db=ny_taxi \],
# [--table_name=green_taxi_data \],
# [--url=${URL}]
# ]


# [
#     '--user=root',
#     '--password=root',
#     '--host=localhost',
#     '--port=5432',
#     '--db=ny_taxi',
#     '--table_name=green_taxi_data',
#     '--url=http://localhost:8000/output%20copy.csv.gz'
# ]
