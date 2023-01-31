from pathlib import Path
import pandas as pd
from prefect import flow, task
from prefect_gcp.cloud_storage import GcsBucket
from prefect_gcp import GcpCredentials


@task(retries=3)
def extract_from_gcs(color: str, year: int, month: int) -> Path:
    "Download Trip data from GCS"
    gcs_path = f"data/{color}/{color}_tripdata_{year}-{month:02}.parquet"
    gcs_block = GcsBucket.load("taxi-gcs-data")
    gcs_block.get_directory(from_path=gcs_path, local_path=gcs_path)
    return Path(f"{gcs_path}")


@task(log_prints=True)
def transform(path: Path) -> pd.DataFrame:
    "Data cleaning example"
    df = pd.read_parquet(path)
    print(
        f"pre missing passenger count : {df['passenger_count'].isna().sum()}")
    df["passenger_count"].fillna(0, inplace=True)
    print(
        f"post missing passenger count : {df['passenger_count'].isna().sum()}")
    return df


@task(retries=3)
def write_bq(color: str, df: pd.DataFrame) -> None:
    df.to_gbq(
        destination_table=f"taxi_data_all.{color}_taxi_data",
        project_id="engaged-cosine-374921", chunksize=10000, if_exists="append",
        credentials=GcpCredentials.load("taxi-gcp-cred").get_credentials_from_service_account())


@flow()
def etl_gcs_to_bg():
    color = "yellow"
    year = 2021
    month = 1
    dataset_file = f"{color}_tripdata_{year}-{month:02}"
    # dataset_url = f"https://github.com/DataTalksClub/nyc-tlc-data/releases/download/{color}/{dataset_file}.csv.gz"
    # path = extract_from_gcs(color, year, month)
    path = f"data/yellow/{dataset_file}.parquet"
    clean_df = transform(path)
    write_bq(color, clean_df)


if __name__ == "__main__":
    etl_gcs_to_bg()
