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

#with engine.connect() as conn:
#    conn.execute(text("""
#        UPDATE dim_products
#        SET product_price = REPLACE(product_price, '£', '');
#    """))
#    conn.commit()

#with engine.connect() as conn:
#    conn.execute(text("""
#        ALTER TABLE dim_products
#        ADD COLUMN weight_class VARCHAR(14);
#    """))
#    conn.commit()

#with engine.connect() as conn:
#    conn.execute(text("""
#        UPDATE dim_products
#        SET weight_class = CASE
#            WHEN weight < 2 THEN 'Light'
#            WHEN weight < 40 THEN 'Mid_Sized'
#            WHEN weight < 140 THEN 'Heavy'
#            WHEN weight >= 140 THEN 'Truck_Required'
#            ELSE 'NULL'
#        END;
#    """))
#    conn.commit()