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
        ALTER TABLE dim_users
        ALTER COLUMN first_name TYPE VARCHAR(255);
    """))
    conn.commit()

with engine.connect() as conn:
    conn.execute(text("""
        ALTER TABLE dim_users
        ALTER COLUMN last_name TYPE VARCHAR(255);
    """))
    conn.commit()

with engine.connect() as conn:
    conn.execute(text("""
        ALTER TABLE dim_users
        ALTER COLUMN country_code TYPE VARCHAR(3);
    """))
    conn.commit()

with engine.connect() as conn:
    conn.execute(text("""
        ALTER TABLE dim_users
        ALTER COLUMN date_of_birth TYPE DATE USING date_of_birth::date;
    """))
    conn.commit()


with engine.connect() as conn:
    conn.execute(text("""
        ALTER TABLE dim_users
        ALTER COLUMN user_uuid TYPE UUID USING user_uuid::UUID;
    """))
    conn.commit()

with engine.connect() as conn:
    conn.execute(text("""
        ALTER TABLE dim_users
        ALTER COLUMN join_date TYPE DATE USING join_date::date;
    """))
    conn.commit()
