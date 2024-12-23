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
        SELECT dim_date_times.year, dim_date_times.month, sum(orders_table.product_quantity * dim_products.product_price) AS total_sales 
        FROM orders_table
        JOIN dim_products ON orders_table.product_code = dim_products.product_code
        JOIN dim_date_times ON dim_date_times.date_uuid = orders_table.date_uuid
        GROUP BY dim_date_times.year, dim_date_times.month
        ORDER BY total_sales DESC
        LIMIT 10;
    """))
    for row in result:
        print(f"Total Sales: {row[2]}, Year: {row[0]}, Month: {row[1]}")