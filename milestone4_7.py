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
        SELECT dim_store_details.country_code, count(dim_users.user_uuid) AS total_staff_number                   
        FROM (SELECT DISTINCT country_code FROM dim_store_details) dim_store_details
        JOIN dim_users ON dim_store_details.country_code = dim_users.country_code
        GROUP BY dim_store_details.country_code
        ORDER BY total_staff_number DESC;
    """))
    for row in result:
        print(f"Total Staff Numbers: {row[1]}, Country Code: {row[0]}")