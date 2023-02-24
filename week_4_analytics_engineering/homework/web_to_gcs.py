from pathlib import Path
import pandas as pd

from prefect import flow,task
from prefect_gcp.cloud_storage import GcsBucket

@task(log_prints=True,retries=3)
def fetch(dataset_url: str) -> pd.DataFrame:
    """Read taxi data from web into pandas DataFrame"""
    
    df = pd.read_csv(dataset_url,engine='pyarrow')

    return df

@task(log_prints=True,retries=3)
def clean(df: pd.DataFrame) -> pd.DataFrame:
    """fix dtype issues"""

    #df['tpep_pickup_datetime']  = pd.to_datetime(df['tpep_pickup_datetime'])
    #df['tpep_dropoff_datetime'] = pd.to_datetime(df['tpep_dropoff_datetime'])
    pass
    #print(df.head(2))
    #print(f"columns: {df.dtypes}")
    #print(f"number of rows: {len(df)}")

    return df

@task(log_prints=True)
def write_local(df: pd.DataFrame, color: str, dataset_file_years: str) -> Path:
    """write locally data"""
    path = Path(f"data/{color}/{dataset_file_years}.parquet")
    df.to_parquet(path, compression="gzip")
    return path

@task(log_prints=True)
def write_gcs(path: Path) -> None:
    """Upload file to Gcs"""

    gcp_cloud_storage_bucket_block = GcsBucket.load("zoom-gcs")
    gcp_cloud_storage_bucket_block.upload_from_path(
        from_path=f"{path}",
        to_path=path
    )
    return

@flow(name="ETL-TO-GCS-BUCKET")
def etl_web_to_gcs(color : str, year : int, month : int) -> None:
    """the main ETL function"""
    dataset_file = f"{color}_tripdata_{year}-{month:02}"
    dataset_url  = f"https://github.com/DataTalksClub/nyc-tlc-data/releases/download/{color}/{dataset_file}.csv.gz"

    df = fetch(dataset_url)
    df_clean = clean(df)
    return df_clean

@flow(name='parent flow',log_prints=True)
def etl_parent_flow(color : str, years : list[int], months : list[int],year: str,name: str)-> None:
    """Parent flow for running multiple months and years"""
    dataset_file_years = f"{color}_tripdata_{name}"
    df_years = pd.DataFrame()
    for year in years:
        for month in months:
            df = etl_web_to_gcs(color,year,month)
            df_years = pd.concat([df_years,df],ignore_index=True)
            del df
    print(f"length of {dataset_file_years} is {len(df_years)}")

    path = write_local(df_years,color,dataset_file_years)
    write_gcs(path)
    
    return

#@flow(name=" ETL-TO-GCS-BUCKET")
#def etl_web_to_gcs() -> None:
#    """the main ETL function"""
#    color = "yellow"
#    year = 2019
#    month = 3
#    dataset_file = f"{color}_tripdata_{year}-{month:02}"
#    dataset_url  = f"https://github.com/DataTalksClub/nyc-tlc-data/releases/download/{color}/{dataset_file}.csv.gz"
#
#    df = fetch(dataset_url)
#    df_clean = clean(df)
#    #path = write_local(df_clean,color,dataset_file)
#    #write_gcs(path)
if __name__ == '__main__':
    months = [1,2,3,4,5,6,7,8,9,10,11,12]
    months_1 = [1,2,3,4,5,6]
    months_2 = [7,8,9,10,11,12]
    
    #etl_parent_flow('yellow', [2020], months,'2020')
    #etl_parent_flow('fhv', [2019], months,'2019')
    #etl_parent_flow('green', [2019,2020], months,'2019_2020')
    #etl_parent_flow('yellow', [2019], [1,2],'2019','1_2')
    #etl_parent_flow('yellow', [2019], [3,4],'2019','3_4')
    #etl_parent_flow('yellow', [2019], [5,6],'2019','5_6')
    #etl_parent_flow('yellow', [2019], [7,8],'2019','7_8')
    #etl_parent_flow('yellow', [2019], [9,10],'2019','9_10')
    etl_parent_flow('yellow', [2019], [11],'2019','11')
    etl_parent_flow('yellow', [2019], [12],'2019','12')


    #etl_parent_flow('green', [2020], months,'2020')
    #etl_parent_flow('yellow', [2019], months_1,'2019_1')
    #etl_parent_flow('yellow', [2019], months_2,'2019_2')