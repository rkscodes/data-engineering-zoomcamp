from pathlib import Path
import pandas as pd
from prefect import flow, task
from prefect_gcp.cloud_storage import GcsBucket
from prefect.tasks import task_input_hash
from datetime import timedelta


@task(retries=3)
def fetch(dataset_url: str) -> pd.DataFrame:
    """Read data from web into pandas dataframe"""
    df = pd.read_csv(dataset_url)
    return df


@task(log_prints=True)
def clean(df=pd.DataFrame) -> pd.DataFrame:
    """Fix dtypes issues"""
    df.tpep_pickup_datetime = pd.to_datetime(df.tpep_pickup_datetime)
    df.tpep_dropoff_datetime = pd.to_datetime(df.tpep_dropoff_datetime)
    print(df.head(2))
    print(f"columns: {df.dtypes}")
    print(f"no of rows: {len(df)}")
    return df


@task()
def write_local(df: pd.DataFrame, colors: str, dataset_file: str) -> Path:
    """Write Dataframe out as a parquet file"""
    path = Path(f"data/{colors}/{dataset_file}.parquet")
    df.to_parquet(path, compression="gzip")
    return path


@task()
def write_gcs(path: Path) -> None:
    """Write data to GCS DataLake"""
    gcs_block = GcsBucket.load("taxi-gcs-data")
    gcs_block.upload_from_path(from_path=path, to_path=path)
    return


@flow()
def etl_web_to_gcs(color: str, year: int, month: int) -> None:
    """ The main ETL function"""
    dataset_file = f"{color}_tripdata_{year}-{month:02}"
    dataset_url = f"https://github.com/DataTalksClub/nyc-tlc-data/releases/download/{color}/{dataset_file}.csv.gz"

    # dataset_url = ""

    df = fetch(dataset_url)
    df_clean = clean(df)
    path = write_local(df_clean, color, dataset_file)
    write_gcs(path)


@flow()
def etl_parent_flow(color: str, year: int, months: list[int] = [1, 2]):
    for month in months:
        etl_web_to_gcs(color, year, month)


if __name__ == "__main__":
    color = "yellow"
    months = [1, 2, 3]
    year = 2021
    etl_parent_flow(color, year, months)
