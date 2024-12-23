from sqlalchemy import create_engine, text, MetaData
import psycopg2

DATABASE_TYPE = 'postgresql'
DBAPI = 'psycopg2'
HOST = 'localhost'
USER = 'postgres'
PASSWORD = 'kingsley'
DATABASE = 'sales_data'
PORT = 5432
engine = create_engine(f"{DATABASE_TYPE}+{DBAPI}://{USER}:{PASSWORD}@{HOST}:{PORT}/{DATABASE}")

#query = text("SELECT MAX(LENGTH(time_period::TEXT)) AS max_length FROM dim_date_times")

# Execute the query
#with engine.connect() as conn:
#    result = conn.execute(query).scalar()
#    print(f"Maximum length: {result}")

with engine.connect() as conn:
    conn.execute(text("""
        ALTER TABLE dim_date_times
        ALTER COLUMN month TYPE VARCHAR(2);
    """))
    conn.commit()

with engine.connect() as conn:
    conn.execute(text("""
        ALTER TABLE dim_date_times
        ALTER COLUMN year TYPE VARCHAR(4);
    """))
    conn.commit()

with engine.connect() as conn:
    conn.execute(text("""
        ALTER TABLE dim_date_times
        ALTER COLUMN day TYPE VARCHAR(2);
    """))
    conn.commit()

with engine.connect() as conn:
    conn.execute(text("""
        ALTER TABLE dim_date_times
        ALTER COLUMN time_period TYPE VARCHAR(10);
    """))
    conn.commit()

with engine.connect() as conn:
    conn.execute(text("""
        ALTER TABLE dim_date_times
        ALTER COLUMN date_uuid TYPE UUID USING date_uuid::UUID;
    """))
    conn.commit()