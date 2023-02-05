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

    # make the required directory if not already
    # gcs_dir =  Path(f"data/{color}")
    # gcs_dir.mkdir(parents=True, exist_ok=True)

    gcs_block.get_directory(from_path=gcs_path, local_path=gcs_path)
    return Path(f"{gcs_path}")


@task(log_prints=True)
def transform(path: Path) -> pd.DataFrame:
    # "fixing dataframe date time scenario "
    df = pd.read_parquet(path)
    df.tpep_pickup_datetime = pd.to_datetime(df.tpep_pickup_datetime)
    df.tpep_dropoff_datetime = pd.to_datetime(df.tpep_dropoff_datetime)
    print(df.dtypes)
    return df



@task(retries=3)
def write_bq(color: str, df: pd.DataFrame) -> None:
    df.to_gbq(
        destination_table=f"taxi_data_all.{color}_taxi_data",
        project_id="engaged-cosine-374921", chunksize=10000, if_exists="append",
        credentials=GcpCredentials.load("taxi-gcp-cred").get_credentials_from_service_account())


@flow(log_prints=True)
def etl_gcs_to_bg(color: str, year: int, month: int) -> int:
    dataset_file = f"{color}_tripdata_{year}-{month:02}"
    dataset_url = f"https://github.com/DataTalksClub/nyc-tlc-data/releases/download/{color}/{dataset_file}.csv.gz"
    path = extract_from_gcs(color, year, month)

    clean_df = transform(path)
    # print(path)
    print(f"no of rows: {len(clean_df)}")
    write_bq(color, clean_df)
    return len(clean_df)

@flow(log_prints=True)
def etl_gcs_to_bg_parent(color:str = "yellow", year: int = 2019, months : list = [2,3]) -> None:
    ans = 0 
    for month in months:
        ans += etl_gcs_to_bg(color, year ,month)
    print(f"Total no of inserted values: {ans}")

if __name__ == "__main__":
    etl_gcs_to_bg_parent()
