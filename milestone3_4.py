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
    conn.execute(text("""
        UPDATE dim_products
        SET product_price = REPLACE(product_price, '£', '');
    """))
    conn.commit()

with engine.connect() as conn:
    conn.execute(text("""
        ALTER TABLE dim_products
        ADD COLUMN weight_class VARCHAR(14);
    """))
    conn.commit()

with engine.connect() as conn:
    conn.execute(text("""
        UPDATE dim_products
        SET weight_class = CASE
            WHEN weight < 2 THEN 'Light'
            WHEN weight < 40 THEN 'Mid_Sized'
            WHEN weight < 140 THEN 'Heavy'
            WHEN weight >= 140 THEN 'Truck_Required'
            ELSE 'NULL'
        END;
    """))
    conn.commit()