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
        SELECT dim_store_details.store_type, sum(orders_table.product_quantity * dim_products.product_price) AS total_sales,
              ROUND((SUM(orders_table.product_quantity * dim_products.product_price) * 100.0) / 
                    SUM(SUM(orders_table.product_quantity * dim_products.product_price)) OVER (), 2) 
        FROM orders_table
        JOIN dim_products ON orders_table.product_code = dim_products.product_code
        JOIN dim_store_details ON dim_store_details.store_code = orders_table.store_code
        GROUP BY dim_store_details.store_type
        ORDER BY total_sales DESC;
    """))
    for row in result:
        print(f"Store Type: {row[0]}, Total Sales: {row[1]}, Sales Made %: {row[2]}")