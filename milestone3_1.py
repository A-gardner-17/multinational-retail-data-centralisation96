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
#        ALTER TABLE orders_table
#        ALTER COLUMN date_uuid TYPE UUID USING date_uuid::UUID;
#    """))
#    conn.commit()

#with engine.connect() as conn:
#    conn.execute(text("""
#        ALTER TABLE orders_table
#        ALTER COLUMN user_uuid TYPE UUID USING user_uuid::UUID;
#    """))
#    conn.commit()

#with engine.connect() as conn:
#    conn.execute(text("""
#        ALTER TABLE orders_table
#        ALTER COLUMN card_number TYPE VARCHAR(19);
#    """))
#    conn.commit()

#with engine.connect() as conn:
#    conn.execute(text("""
#        ALTER TABLE orders_table
#        ALTER COLUMN store_code TYPE VARCHAR(12);
#    """))
#    conn.commit()

#with engine.connect() as conn:
#    conn.execute(text("""
#        ALTER TABLE orders_table
#        ALTER COLUMN product_code TYPE VARCHAR(11);
#    """))
#    conn.commit()

# check range of vlaues
# with engine.connect() as conn:
#    conn.execute(text("""
#        SELECT product_quantity
#        FROM orders_table
#        WHERE product_quantity < -32768 OR product_quantity > 32767;              
#    """))

#with engine.connect() as conn:
#    conn.execute(text("""
#       ALTER TABLE orders_table
#        ALTER COLUMN product_quantity TYPE SMALLINT USING product_quantity::SMALLINT;              
#    """))
#    conn.commit()

