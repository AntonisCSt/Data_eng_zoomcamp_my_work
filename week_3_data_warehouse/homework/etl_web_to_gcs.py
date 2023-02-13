from pathlib import Path
import pandas as pd

from prefect import flow,task
from prefect_gcp.cloud_storage import GcsBucket


@task(log_prints=True,retries=3)
def fetch(dataset_url: str) -> pd.DataFrame:
    """Read taxi data from web into pandas DataFrame"""

    df = pd.read_csv(dataset_url)

    return df

@task(log_prints=True,retries=3)
def clean(df: pd.DataFrame) -> pd.DataFrame:
    """fix dtype issues"""

    df['lpep_pickup_datetime']  = pd.to_datetime(df['lpep_pickup_datetime'])
    df['lpep_dropoff_datetime'] = pd.to_datetime(df['lpep_dropoff_datetime'])

    print(df.head(2))
    print(f"columns: {df.dtypes}")
    print(f"number of rows: {len(df)}")

    return df

@task(log_prints=True)
def write_local(df: pd.DataFrame, color: str, dataset_file: str) -> Path:
    """write locally data"""
    path = Path(f"data/{color}/{dataset_file}.csv.gz")
    #df.to_parquet(path, compression="gzip")
    df.to_csv(path, compression="gzip")
    return path

@task(log_prints=True)
def write_gcs(path: Path) -> None:
    """Upload file to Gcs"""

    gcp_cloud_storage_bucket_block = GcsBucket.load("week-2-zoom")
    gcp_cloud_storage_bucket_block.upload_from_path(
        from_path=f"{path}",
        to_path=path
    )
    return


@flow(name=" ETL-TO-GCS-BUCKET")
def etl_web_to_gcs() -> None:
    """the main ETL function"""
    color = "green"
    year = 2019
    month = 4
    dataset_file = f"{color}_tripdata_{year}-{month:02}"
    dataset_url  = f"https://github.com/DataTalksClub/nyc-tlc-data/releases/download/{color}/{dataset_file}.csv.gz"

    df = fetch(dataset_url)
    df_clean = clean(df)
    path = write_local(df_clean,color,dataset_file)
    write_gcs(path)
if __name__ == '__main__':
    etl_web_to_gcs()