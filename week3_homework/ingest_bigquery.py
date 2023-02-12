from pathlib import Path
import pandas as pd
# from prefect import flow, task
# from prefect_gcp.cloud_storage import GcsBucket
# from prefect_gcp import GcpCredentials
from google.oauth2 import service_account



def write_bq(df: pd.DataFrame, dataset_file:str) -> None:
    print("Writing to gcs")
    credentials = service_account.Credentials.from_service_account_file('/home/ram/.config/gcloud/engaged-cosine-374921-75cecdfaf8e1.json',)
    df.to_gbq(
        destination_table=f"taxi_data_all.{dataset_file}",
        project_id="engaged-cosine-374921", chunksize=10000, if_exists="append",
        credentials=credentials)




def etl_gcs_to_bg(month: int) -> None:
    dataset_file = f"fhv_tripdata_2019-{month:02}"
    dataset_url = f"https://github.com/DataTalksClub/nyc-tlc-data/releases/download/fhv/{dataset_file}.csv.gz"
    # path = extract_from_gcs(color, year, month)
    # clean_df = transform(path)

    print(dataset_url)
    df = pd.read_csv(dataset_url, compression="gzip")
    print(len(df))
    print(df.columns)
    write_bq(df, dataset_file)

def etl_gcs_to_bg_parent() -> None:
    for month in range(1,2):
        etl_gcs_to_bg(month)


if __name__ == "__main__":
    etl_gcs_to_bg_parent()
