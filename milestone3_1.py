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
#with engine.connect() as conn:
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

