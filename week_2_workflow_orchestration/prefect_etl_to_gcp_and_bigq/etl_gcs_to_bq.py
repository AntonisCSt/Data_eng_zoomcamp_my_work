from pathlib import Path
import pandas as pd

from prefect import flow,task
from prefect_gcp.cloud_storage import GcsBucket
from prefect_gcp import GcpCredentials

@task(log_prints=True,retries=2)
def extract_from_gcs(color: str, year: int, month: int) -> Path:
    """Download trip data from GCS"""

    gcs_path = f"data/{color}/{color}_tripdata_{year}-{month:02}.parquet"
    gcs_block = GcsBucket.load("week-2-zoom")
    gcs_block.get_directory(from_path=gcs_path, local_path=f"./data/")
    print(f"path: ./data/{gcs_path}")

    return Path(f"./data/{gcs_path}")

@task(log_prints=True)
def transform(path: Path) -> pd.DataFrame:
    """Data cleaning example"""
    print(path)
    df = pd.read_parquet(path)
    print(f"pre: missing passenger count: {df['passenger_count'].isna().sum()}")
    df['passenger_count'].fillna(df['passenger_count'].mean(),inplace=True)
    print(f"pre: missing passenger count: {df['passenger_count'].isna().sum()}")
    return df

@task(log_prints=True,retries=2)
def write_bq(df: pd.DataFrame) -> None:
    """Write DataFrame to BigQuery"""
    gcp_credentials_block = GcpCredentials.load("zoom-service-account")
    df.to_gbq(destination_table="trips_data_all.rides",
        project_id="my-rides-antonis",
        credentials= gcp_credentials_block.get_credentials_from_service_account(),
        chunksize=500000,
        if_exists="append"
    )


    return
@flow(name=" ETL-TO-BIG-QUERY")
def etl_gcs_to_bq() -> None:
    """Main ETL flow to load data into Big Query"""
    
    color = "yellow"
    year = 2021
    month = 1
    
    path = extract_from_gcs(color, year, month)
    df = transform(path)
    write_bq(df)
if __name__ == '__main__':
    etl_gcs_to_bq()