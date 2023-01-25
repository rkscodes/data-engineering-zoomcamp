import os
import argparse

from time import time

import pandas as pd
from sqlalchemy import create_engine

import pathlib


def main(params):
    user = params.user
    password = params.password
    host = params.host 
    port = params.port 
    db = params.db
    table_name = params.table_name
    url = params.url

    csv_name = pathlib.Path(url).name
    os.system(f"wget {url}")

    engine = create_engine(f"postgresql://{user}:{password}@{host}:{port}/{db}")

    engine.connect()

    df = pd.read_csv(f"{csv_name}")

    if(csv_name=="green_tripdata_2019-01.csv.gz"):
        df_itr = pd.read_csv(f"{csv_name}", iterator=True, chunksize=10000)


        df = next(df_itr)



        df.lpep_pickup_datetime = pd.to_datetime(df.lpep_pickup_datetime)
        df.lpep_dropoff_datetime = pd.to_datetime(df.lpep_dropoff_datetime)


        df.head(0).to_sql(name=f"{table_name}", con=engine, if_exists='replace')
        df.to_sql(name=f"{table_name}", con=engine, if_exists='append')

        while True:
            begin_time = time()
            df = next(df_itr)
            df.lpep_pickup_datetime = pd.to_datetime(df.lpep_pickup_datetime)
            df.lpep_dropoff_datetime = pd.to_datetime(df.lpep_dropoff_datetime)
            df.to_sql(name=f"{table_name}", con=engine, if_exists='append')
            end_time = time()
            print(f"inserted 10,000 data in {end_time - begin_time}")
    df.to_sql(name=f"{table_name}", con=engine, if_exists='replace')
    # in the case of above stopiteration error this will not be exectued, so try to handle it
    os.system(f"rm {csv_name}")


if __name__ == '__main__' :
    parser = argparse.ArgumentParser("Description: Arguments parsers for data ingesting")

    # user
    # password
    # host
    # port
    # db_name
    # table_name
    # url 

    parser.add_argument("--user",required=True, help="username for postgres")
    parser.add_argument("--password",required=True, help="password for postgres")
    parser.add_argument("--host",required=True, help="address where postgress is hosted")
    parser.add_argument("--port",required=True, help="port at which postgress is running")
    parser.add_argument("--db",required=True, help="database name")
    parser.add_argument("--table_name",required=True, help="table name")
    parser.add_argument("--url",required=True, help="data download link")

    args = parser.parse_args()
    main(args)
