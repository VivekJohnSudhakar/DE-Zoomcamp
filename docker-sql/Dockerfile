FROM python:3.9

RUN pip install pandas sqlalchemy psycopg2
RUN apt-get install wget
WORKDIR /app

COPY pipeline.py pipeline.py
ENTRYPOINT [ "python","pipeline.py" ]

docker run -it --network=pg-network --name pg-database -e POSTGRES_USER="root" -e POSTGRES_PASSWORD="root" -e POSTGRES_DB="ny_taxi" -v $(pwd)/ny_taxi_postgress_data:/var/lib/postgresql/data -p 5432:5432 postgres:13

pgcli -h localhost -p 5432 -u root -d ny_taxi

docker run -it \
-e PGADMIN_DEFAULT_EMAIL="admin@admin.com"  \
-e PGADMIN_DEFAULT_PASSWORD="root"  \
-p 8080:80  \
--network=pg-network    \
--name pgadmin   \
dpage/pgadmin4

docker network create pg-network

URL=""
python pipeline.py \
--user=root \
--pwd=root \
--host=localhost \
--port=5432 \
--db=ny_taxi \
--table_name=yellow_taxi_data \
--data_path=${URL} \