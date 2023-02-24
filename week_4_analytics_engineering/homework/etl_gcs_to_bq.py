from pathlib import Path
import pandas as pd

from prefect import flow,task
from prefect_gcp.cloud_storage import GcsBucket
from prefect_gcp import GcpCredentials

@task(log_prints=True)
def extract_from_gcs(color: str, name: str) -> Path:
    """Download trip data from GCS"""

    gcs_path = f"{color}/{name}"
    gcs_block = GcsBucket.load("zoom-gcs")
    gcs_block.get_directory(from_path=gcs_path, local_path=f"./data/")
    print(f"path: ./data/{gcs_path}")

    return Path(f"./data/{gcs_path}")

@task(log_prints=True)
def transform(path: Path) -> pd.DataFrame:
    """Data cleaning example"""
    print(path)
    df = pd.read_parquet(path)
    print("############################################")
    print(len(df))
    #print(f"pre: missing passenger count: {df['passenger_count'].isna().sum()}")
    #df['passenger_count'].fillna(df['passenger_count'].mean(),inplace=True)
    #print(f"pre: missing passenger count: {df['passenger_count'].isna().sum()}")
    return df

@task(log_prints=True)
def write_bq(df: pd.DataFrame,color: str,file_name: str) -> None:
    """Write DataFrame to BigQuery"""
    gcp_credentials_block = GcpCredentials.load("gcp-credentials")
    df.to_gbq(destination_table=f"trips_data_all.{file_name}",
        project_id="my-rides-antonis",
        credentials= gcp_credentials_block.get_credentials_from_service_account(),
        chunksize=500000,
        if_exists="append"
    )


    return
@flow(name=" ETL-TO-BIG-QUERY")
def etl_gcs_to_bq(color: str,name: str,file_name: str) -> None:
    """Main ETL flow to load data into Big Query"""

    path = extract_from_gcs(color,name)
    df = transform(path)
    write_bq(df,color,file_name)
if __name__ == '__main__':
    #etl_gcs_to_bq("green","green_tripdata.parquet")
    #etl_gcs_to_bq("fhv","fhv_tripdata.parquet")
    etl_gcs_to_bq("yellow","yellow_tripdata_1_2.parquet","yellow_tripdata_1_2")
    etl_gcs_to_bq("yellow","yellow_tripdata_3_4.parquet","yellow_tripdata_3_4")
    etl_gcs_to_bq("yellow","yellow_tripdata_5_6.parquet","yellow_tripdata_5_6")
    etl_gcs_to_bq("yellow","yellow_tripdata_7_8.parquet","yellow_tripdata_7_8")
    etl_gcs_to_bq("yellow","yellow_tripdata_9_10.parquet","yellow_tripdata_9_10")
    etl_gcs_to_bq("yellow","yellow_tripdata_11.parquet","yellow_tripdata_11")
    etl_gcs_to_bq("yellow","yellow_tripdata_12.parquet","yellow_tripdata_12")

