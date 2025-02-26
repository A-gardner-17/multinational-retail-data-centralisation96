from sqlalchemy import create_engine, text, MetaData
import psycopg2

DATABASE_TYPE = 'postgresql'
DBAPI = 'psycopg2'
HOST = 'localhost'
USER = 'postgres'
PASSWORD = 'kingsley'
DATABASE = 'sales_data'
PORT = 5432
engine = create_engine(f"{DATABASE_TYPE}+{DBAPI}://{USER}:{PASSWORD}@{HOST}:{PORT}/{DATABASE}")

with engine.connect() as conn:
    result = conn.execute(text("""
        SELECT locality, count (*) AS total_no_stores
        FROM dim_store_details
        GROUP BY locality
        HAVING COUNT(*) >= 10
        ORDER BY total_no_stores DESC;
    """))
    for row in result:
        print(f"Country Code: {row[0]}, Total Stores: {row[1]}")