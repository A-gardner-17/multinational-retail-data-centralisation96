import yaml
from sqlalchemy import create_engine, text, MetaData
import psycopg2
import pandas as pd

DATABASE_TYPE = 'postgresql'
DBAPI = 'psycopg2'
HOST = 'localhost'
USER = 'postgres'
PASSWORD = 'kingsley'
DATABASE = 'sales_data'
PORT = 5432
engine = create_engine(f"{DATABASE_TYPE}+{DBAPI}://{USER}:{PASSWORD}@{HOST}:{PORT}/{DATABASE}")
users = pd.read_sql_table('dim_card_details', engine)
users.head(1)
print(users)

#users.to_csv('new_card_details.csv', index=False)


