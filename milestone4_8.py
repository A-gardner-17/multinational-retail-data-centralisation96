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
        SELECT dim_store_details.store_type, dim_store_details.country_code, sum(orders_table.product_quantity * dim_products.product_price) AS total_sales
        FROM orders_table
        JOIN dim_products ON orders_table.product_code = dim_products.product_code
        JOIN dim_store_details ON dim_store_details.store_code = orders_table.store_code
        WHERE dim_store_details.country_code = 'DE'
        GROUP BY dim_store_details.country_code, dim_store_details.store_type
        ORDER BY total_sales;
    """))
    for row in result:
        print(f"Total Sales: {row[2]}, Store Type: {row[0]}, Country Code: {row[1]}")