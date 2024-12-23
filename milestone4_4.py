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

with engine.connect() as conn:
    result = conn.execute(text("""
        SELECT 
        CASE 
            WHEN dim_store_details.store_type = 'Web Portal' THEN 'Web'
            ELSE 'Offline'
        END AS location,
        COUNT(orders_table.index) AS number_of_sales,
        SUM(orders_table.product_quantity) AS product_quantity_count
        FROM orders_table
        JOIN dim_store_details ON dim_store_details.store_code = orders_table.store_code
        GROUP BY location;
    """))
    for row in result:
        print(f"Number of Sales: {row[1]}, Product Quantity Count: {row[2]}, Location: {row[0]}")