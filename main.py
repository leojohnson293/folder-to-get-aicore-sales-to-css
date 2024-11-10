import yaml 
import pandas as pd
import psycopg2
from sqlalchemy import create_engine as ce
from sqlalchemy import inspect



local_type ='postgresql'
local_api ='psycopg2'
local_host = 'localhost'
local_password = 'password'
local_user = 'postgres'
local_database = 'Sales_Data'
local_port = '5432'
#engine = ce(f"{local_type}+{local_api}://{local_user}:{local_password}@{local_host}:{local_port}/{local_database}")
# db_clean.to_sql('dim_products',tosql, if_exists = 'replace')
#db_clean.to_sql('order_table',tosql, if_exists = 'replace')

conn = psycopg2.connect(
    host = local_host,
    database = local_database,
    user = local_user,
    password = local_password
)

cursor = conn.cursor()
cursor.execute("SELECT table_name FROM information_schema.tables WHERE table_schema = 'public';")
tables = cursor.fetchall()

for table in tables:
    table = table[0]
    query = f"SELECT * FROM {table}"
    df = pd.read_sql(query, conn)
    csv_filename = f"{table}.csv"
    df.to_csv(csv_filename, index=False)
    print(f"Exported {table} to csv")

cursor.close()
conn.close()

    

