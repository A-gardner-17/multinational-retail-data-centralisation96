from sqlalchemy import create_engine, text, MetaData
import psycopg2
import yaml

with open("db_creds_local.yaml", 'r') as file:
    config = yaml.safe_load(file)
    
DATABASE_TYPE = config['DATABASE_TYPE']
DBAPI = config['DBAPI']
HOST = config['HOST']
USER = config['USER']
PASSWORD = config['PASSWORD']
DATABASE = config['DATABASE']
PORT = config['PORT']
engine = create_engine(f"{DATABASE_TYPE}+{DBAPI}://{USER}:{PASSWORD}@{HOST}:{PORT}/{DATABASE}")

with engine.connect() as conn:
    result = conn.execute(text("""
        SELECT country_code, count (*) AS total_no_stores
        FROM dim_store_details
        WHERE store_type != 'Web Portal'
        GROUP BY country_code;
    """))
    for row in result:
        print(f"Country Code: {row[0]}, Total Stores: {row[1]}")
