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
        ALTER TABLE dim_date_times
        ADD CONSTRAINT pk_date_times PRIMARY KEY (date_uuid);
    """))
    conn.commit()

with engine.connect() as conn:
    conn.execute(text("""
        ALTER TABLE dim_card_details
        ADD CONSTRAINT pk_card_details PRIMARY KEY (card_number);
    """))
    conn.commit()

with engine.connect() as conn:
    conn.execute(text("""
        ALTER TABLE dim_products
        ADD CONSTRAINT pk_products PRIMARY KEY (product_code);
    """))
    conn.commit()

with engine.connect() as conn:
    conn.execute(text("""
        ALTER TABLE dim_store_details
        ADD CONSTRAINT pk_store_details PRIMARY KEY (store_code);
    """))
    conn.commit()

with engine.connect() as conn:
    conn.execute(text("""
        ALTER TABLE dim_users
        ADD CONSTRAINT pk_users PRIMARY KEY (user_uuid);
    """))
    conn.commit()