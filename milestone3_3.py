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
        UPDATE dim_store_details  
        SET latitude = CONCAT(lat, latitude);
    """))
    conn.commit()

with engine.connect() as conn:
    conn.execute(text("""
        ALTER TABLE dim_store_details
        DROP COLUMN lat;
    """))
    conn.commit()

with engine.connect() as conn:
    conn.execute(text("""
        UPDATE dim_store_details
        SET latitude = NULL
        WHERE latitude = 'N/A';
    """))
    conn.commit()

with engine.connect() as conn:
    conn.execute(text("""
        ALTER TABLE dim_store_details
        ALTER COLUMN latitude TYPE NUMERIC USING latitude::numeric;
    """))
    conn.commit()

with engine.connect() as conn:
    conn.execute(text("""
        UPDATE dim_store_details
        SET longitude = NULL
        WHERE longitude = 'N/A';
    """))
    conn.commit()

with engine.connect() as conn:
    conn.execute(text("""
        ALTER TABLE dim_store_details
        ALTER COLUMN longitude TYPE NUMERIC USING longitude::numeric;
    """))
    conn.commit()

with engine.connect() as conn:
    conn.execute(text("""
        ALTER TABLE dim_store_details
        ALTER COLUMN locality TYPE VARCHAR(255);
    """))
    conn.commit()

#query = text("SELECT MAX(LENGTH(store_code::TEXT)) AS max_length FROM dim_store_details")

# Execute the query
#with engine.connect() as conn:
#    result = conn.execute(query).scalar()
#    print(f"Maximum length: {result}")

with engine.connect() as conn:
    conn.execute(text("""
        ALTER TABLE dim_store_details
        ALTER COLUMN store_code TYPE VARCHAR(12);
    """))
    conn.commit()

with engine.connect() as conn:
    conn.execute(text("""
       ALTER TABLE dim_store_details
        ALTER COLUMN staff_numbers TYPE SMALLINT USING staff_numbers::SMALLINT;              
    """))
    conn.commit()

with engine.connect() as conn:
    conn.execute(text("""
        ALTER TABLE dim_store_details
        ALTER COLUMN opening_date TYPE DATE USING opening_date::date;
    """))
    conn.commit()

with engine.connect() as conn:
    conn.execute(text("""
        ALTER TABLE dim_store_details
        ALTER COLUMN store_type TYPE VARCHAR(255),
        ALTER COLUMN store_type DROP NOT NULL;
    """))
    conn.commit()

#query = text("SELECT MAX(LENGTH(country_code::TEXT)) AS max_length FROM dim_store_details")

# Execute the query
#with engine.connect() as conn:
#    result = conn.execute(query).scalar()
#    print(f"Maximum length: {result}")

with engine.connect() as conn:
    conn.execute(text("""
        ALTER TABLE dim_store_details
        ALTER COLUMN country_code TYPE VARCHAR(2);
    """))
    conn.commit()

with engine.connect() as conn:
    conn.execute(text("""
        ALTER TABLE dim_store_details
        ALTER COLUMN continent TYPE VARCHAR(255);
    """))
    conn.commit()