from sqlalchemy import create_engine
from time import time
import pandas as pd

df = pd.read_csv('yellow_tripdata_2021-01.csv', nrows=100)

#first generate schema
df.tpep_pickup_datetime = pd.to_datetime(df.tpep_pickup_datetime)
df.tpep_dropoff_datetime = pd.to_datetime(df.tpep_dropoff_datetime)
engine = create_engine('postgresql://root:root@localhost:5432/ny_taxi')

engine.connect()

print(pd.io.sql.get_schema(df,name='yellow_taxi_data', con=engine))
df_iter = pd.read_csv('yellow_tripdata_2021-01.csv', iterator=True, chunksize=100000)
df = next(df_iter)


df.tpep_pickup_datetime = pd.to_datetime(df.tpep_pickup_datetime)
df.tpep_dropoff_datetime = pd.to_datetime(df.tpep_dropoff_datetime)


df.head(n=0).to_sql(name='yellow_taxi_data',con=engine, if_exists='replace')

get_ipython().run_line_magic('time', "df.to_sql(name='yellow_taxi_data',con=engine, if_exists='append')")


while True:
    t_start = time()
    
    df = next(df_iter)
    df.tpep_pickup_datetime = pd.to_datetime(df.tpep_pickup_datetime)
    df.tpep_dropoff_datetime = pd.to_datetime(df.tpep_dropoff_datetime)
    df.to_sql(name='yellow_taxi_data',con=engine, if_exists='append')
    
    t_end = time()
    
    print('inserting another chunk..., took %.3f seconds' %(t_end-t_start))




