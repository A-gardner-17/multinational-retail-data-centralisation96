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

#query = text("SELECT MAX(LENGTH(card_number::TEXT)) AS max_length FROM dim_card_details")

# Execute the query
#with engine.connect() as conn:
#    result = conn.execute(query).scalar()
#    print(f"Maximum length: {result}")

with engine.connect() as conn:
    conn.execute(text("""
        ALTER TABLE dim_card_details
        ALTER COLUMN card_number TYPE VARCHAR(19);
    """))
    conn.commit()

## Resolve issue with casting
##with engine.connect() as conn:
##    conn.execute(text("""
##        UPDATE dim_card_details
##        SET card_number = TRIM(TO_CHAR(CAST(card_number AS NUMERIC), '9999999999999999999999'));
##    """))
##    conn.commit()

with engine.connect() as conn:
    conn.execute(text("""
        ALTER TABLE dim_card_details
        ALTER COLUMN expiry_date TYPE VARCHAR(5);
    """))
    conn.commit()

with engine.connect() as conn:
    conn.execute(text("""
        ALTER TABLE dim_card_details
        ALTER COLUMN date_payment_confirmed TYPE DATE USING date_payment_confirmed::date;
    """))
    conn.commit()