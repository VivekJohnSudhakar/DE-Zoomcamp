import pandas as pd
import sys
from sqlalchemy import create_engine
import argparse
import os
#
print(sys.argv)
day = sys.argv[1]
parser = argparse.ArgumentParser(
                    prog='pipeline',
                    description='ingest data from csv to postgres',
                    epilog='Text at the bottom of help')
#username,pwd,host,port,db,data path,table name,
parser.add_argument('--user',help='username of postgres')           # positional argument
parser.add_argument('--pwd',help='pwd of postgres')   
parser.add_argument('--host',help='host of postgres')   
parser.add_argument('--port',help='port of postgres')   
parser.add_argument('--db',help='db name of postgres')   
parser.add_argument('--table_name',help='table name of postgres')   
parser.add_argument('--data_path',help='data path of csv')   
args = parser.parse_args()


engine = create_engine(f'postgresql://{args.user}:{args.pwd}@{args.host}:{args.port}/{args.db}')
#download csv
csv_name = 'output.csv'
os.system(f"wget {args.data_path} -O {csv_name}")
df_iter = pd.read_csv(csv_name,iterator=True,chunksize=100000)
i = 0
try:
    while True:
        df = next(df_iter)
        if i == 0:
            df.tpep_pickup_datetime = pd.to_datetime(df.tpep_pickup_datetime)
            df.tpep_dropoff_datetime = pd.to_datetime(df.tpep_dropoff_datetime)
            df.head(n=0).to_sql(name=args.table_name,con=engine,if_exists='replace')
            df.to_sql(name=args.table_name,con=engine,if_exists='append')
            print("finished chunk ",i)
            i += 1
        else:
            df.tpep_pickup_datetime = pd.to_datetime(df.tpep_pickup_datetime)
            df.tpep_dropoff_datetime = pd.to_datetime(df.tpep_dropoff_datetime)
            df.to_sql(name=args.table_name,con=engine,if_exists='append')
            print("finished chunk ",i)
            i +=1
except :
    print("Complteted succeefully for ",day)