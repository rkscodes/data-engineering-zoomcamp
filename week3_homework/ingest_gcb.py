from pathlib import Path
import pandas as pd
from prefect import flow, task
from prefect_gcp.cloud_storage import GcsBucket


@task(retries=3, log_prints=True)
def fetch(dataset_url: str) -> pd.DataFrame:
    """Read data from web into pandas dataframe"""
    df = pd.read_csv(dataset_url, compression="gzip")
    print(f"no of rows: {len(df)}")
    return df


@task(log_prints=True)
def write_local(df: pd.DataFrame, dataset_file: str, colors: str = "fhv") -> Path:
    """Write Dataframe out as a csv file"""
    path = Path(f"data/{colors}/{dataset_file}")
    directory = Path(f"data/{colors}/")
    directory.mkdir(parents=True, exist_ok=True)
    df.to_csv(path)
    return path


@task(log_prints=True)
def write_gcs(path: Path) -> None:
    """Write data to GCS DataLake"""
    gcs_block = GcsBucket.load("taxi-gcs-data")
    gcs_block.upload_from_path(from_path=path, to_path=path)
    return


# def etl_web_to_gcs(color:str = "green", year:int= 2019, month:int = 4) -> None:
#     """ The main ETL function"""
#     dataset_file = f"{color}_tripdata_{year}-{month:02}"
#     dataset_url = f"https://github.com/DataTalksClub/nyc-tlc-data/releases/download/{color}/{dataset_file}.csv.gz"

#     # dataset_url = "http://localhost:8000/yellow_tripdata_2021-01.csv.gz"
#     print(dataset_url)
#     df = fetch(dataset_url)
#     # df_clean = clean(df)
#     path = write_local(df, color, dataset_file)
#     write_gcs(path)

@flow(log_prints=True)
def etl_gcs_to_gcb(month: int) -> None:
    dataset_file = f"fhv_tripdata_2019-{month:02}.csv"
    dataset_url = f"https://github.com/DataTalksClub/nyc-tlc-data/releases/download/fhv/{dataset_file}.gz"
    # path = extract_from_gcs(color, year, month)
    # clean_df = transform(path)

    print(dataset_url)
    df = fetch(dataset_url)
    print(len(df))
    print(df.columns)
    path = write_local(df, dataset_file)
    write_gcs(path)


@flow(log_prints=True)
def etl_gcs_to_bg_parent() -> None:
    for month in range(1, 13):
        etl_gcs_to_gcb(month)


if __name__ == "__main__":
    etl_gcs_to_bg_parent()
