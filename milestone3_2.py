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
#        ALTER TABLE dim_users
#        ALTER COLUMN first_name TYPE VARCHAR(255);
#    """))
#    conn.commit()

#with engine.connect() as conn:
#    conn.execute(text("""
#        ALTER TABLE dim_users
#        ALTER COLUMN last_name TYPE VARCHAR(255);
#    """))
#    conn.commit()

#with engine.connect() as conn:
#    conn.execute(text("""
#        ALTER TABLE dim_users
#        ALTER COLUMN country_code TYPE VARCHAR(3);
#    """))
#    conn.commit()

#with engine.connect() as conn:
#    conn.execute(text("""
#        ALTER TABLE dim_users
#        ALTER COLUMN date_of_birth TYPE DATE USING date_of_birth::date;
#    """))
#    conn.commit()


#with engine.connect() as conn:
#    conn.execute(text("""
#        ALTER TABLE dim_users
#        ALTER COLUMN user_uuid TYPE UUID USING user_uuid::UUID;
#    """))
#    conn.commit()

#with engine.connect() as conn:
#    conn.execute(text("""
#        ALTER TABLE dim_users
#        ALTER COLUMN join_date TYPE DATE USING join_date::date;
#    """))
#    conn.commit()
