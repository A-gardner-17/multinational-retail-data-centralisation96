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
        ALTER TABLE dim_products
        RENAME COLUMN removed to still_available;
    """))
    conn.commit()

with engine.connect() as conn:
    conn.execute(text("""
        ALTER TABLE dim_products
        ALTER COLUMN product_price TYPE NUMERIC USING product_price::numeric;
    """))
    conn.commit()

with engine.connect() as conn:
    conn.execute(text("""
        ALTER TABLE dim_products
        ALTER COLUMN weight TYPE NUMERIC USING weight::numeric;
    """))
    conn.commit()

#query = text("SELECT MAX(LENGTH(product_code::TEXT)) AS max_length FROM dim_products")

# Execute the query
#with engine.connect() as conn:
#    result = conn.execute(query).scalar()
#    print(f"Maximum length: {result}")

with engine.connect() as conn:
    conn.execute(text("""
        ALTER TABLE dim_products
        ALTER COLUMN "EAN" TYPE VARCHAR(17);
    """))
    conn.commit()

with engine.connect() as conn:
    conn.execute(text("""
        ALTER TABLE dim_products
        ALTER COLUMN product_code TYPE VARCHAR(11);
    """))
    conn.commit()

with engine.connect() as conn:
    conn.execute(text("""
        ALTER TABLE dim_products
        ALTER COLUMN date_added TYPE DATE USING date_added::date;
    """))
    conn.commit()

with engine.connect() as conn:
    conn.execute(text("""
        ALTER TABLE dim_products
        ALTER COLUMN uuid TYPE UUID USING uuid::UUID;
    """))
    conn.commit()

with engine.connect() as conn:
    conn.execute(text("""
        UPDATE dim_products
        SET still_available = CASE
            WHEN still_available = 'Still_avaliable' THEN 'true'
            ELSE 'false'
        END;
    """))
    conn.commit()

with engine.connect() as conn:
    conn.execute(text("""
        ALTER TABLE dim_products
        ALTER COLUMN still_available TYPE BOOLEAN USING still_available::boolean;
    """))
    conn.commit()