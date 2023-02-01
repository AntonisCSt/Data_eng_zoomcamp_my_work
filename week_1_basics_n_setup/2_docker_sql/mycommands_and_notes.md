
#run postgresql
```
docker run -it \
  -e POSTGRES_USER="root" \
  -e POSTGRES_PASSWORD="root" \
  -e POSTGRES_DB="ny_taxi" \
  -v $(pwd)/ny_taxi_postgres_data:/var/lib/postgresql/data \
  -p 5432:5432 \
  postgres:13
```

  #run pgadmin
```
docker run -it \
  -e PGADMIN_DEFAULT_EMAIL="admin@admin.com" \
  -e PGADMIN_DEFAULT_PASSWORD="root" \
  -p 8080:80 \
  dpage/pgadmin4
```

  ok if we run them separetly and then try to conncet the pgAdmin to the postgre, it will fail! because they have different enviroments. So the localhost of the docker of pgadmin is the one in docker.

  We going to use networks in docker to solve this!!

  this command: `docker network create pg-network`

```
docker run -it \
  -e POSTGRES_USER="root" \
  -e POSTGRES_PASSWORD="root" \
  -e POSTGRES_DB="ny_taxi" \
  -v $(pwd)/ny_taxi_postgres_data:/var/lib/postgresql/data \
  -p 5432:5432 \
  --network=pg-network\
  --name pg-database\
  postgres:13
```

```
docker run -it \
  -e PGADMIN_DEFAULT_EMAIL="admin@admin.com" \
  -e PGADMIN_DEFAULT_PASSWORD="root" \
  -p 8080:80 \
  --network=pg-network\
  --name pgadmin\
  dpage/pgadmin4
```

#for ingesting data from ingest_data.py

URL="https://github.com/DataTalksClub/nyc-tlc-data/releases/download/yellow/yellow_tripdata_2021-01.csv.gz"

python ingest_data.py\
    --user=root \
    --password=root \
    --host=localhost \
    --port=5432\
    --db=ny_taxi \
    --table_name=yellow_taxi_trips \
    --url=${URL}


  docker build -it --network=pg-network taxi_ingest:v001 \
      --user=root \
      --password=root \
      --host=localhost \
      --port=5432 \
      --db=ny_taxi \
      --table_name=yellow_taxi_trips \
      --url=${URL}

URL="https://github.com/DataTalksClub/nyc-tlc-data/releases/download/yellow/yellow_tripdata_2021-01.csv.gz"

docker run -it \
  --network=pg-network \
  taxi_ingest:v001 \
    --user=root \
    --password=root \
    --host=pg-database3 \
    --port=5432 \
    --db=ny_taxi \
    --table_name=yellow_taxi_trips \
    --url=https://github.com/DataTalksClub/nyc-tlc-data/releases/download/yellow/yellow_tripdata_2021-01.csv.gz