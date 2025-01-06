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
        SELECT dim_date_times.month, sum(orders_table.product_quantity * dim_products.product_price) AS total_sales
        FROM orders_table
        JOIN dim_products ON orders_table.product_code = dim_products.product_code
        JOIN dim_date_times ON orders_table.date_uuid = dim_date_times.date_uuid
        GROUP BY dim_date_times.month
        ORDER BY total_sales DESC;
    """))
    for row in result:
        print(f"Month: {row[0]}, Total Sales: {row[1]}")