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

# Decided to add a TIMESTAMP column to simplify the process

#with engine.connect() as conn:
#    result = conn.execute(text("""
#        ALTER TABLE dim_date_times
#        ADD COLUMN timestamp_column TIMESTAMP;
#    """))
#    conn.commit()

# Using Cast and LPAD - left padding as MAKE_TIMESTAMP didn't work with current data - casted as a TIMESTAMP

#with engine.connect() as conn:
#    result = conn.execute(text("""  
#        UPDATE dim_date_times SET timestamp_column = CAST(
#        year || '-' || LPAD(month, 2, '0') || '-' || LPAD(day, 2, '0') || ' ' || timestamp 
#        AS TIMESTAMP);                               
#    """))
#    conn.commit()

# Add difference column to the database - INTERVAL data type

#with engine.connect() as conn:
#    result = conn.execute(text("""
#        ALTER TABLE dim_date_times
#        ADD COLUMN difference INTERVAL;
#    """))
#    conn.commit()

# Using LEAD to find difference in each sale and ordering over the timestamp_column
# This is then stored in the difference column in the database

#with engine.connect() as conn:
#    result = conn.execute(text("""  
#        WITH calculated_differences AS (
#            SELECT date_uuid, LEAD(timestamp_column) OVER (ORDER BY timestamp_column) - timestamp_column AS difference
#        FROM 
#            dim_date_times
#        )
#        UPDATE dim_date_times
#        SET difference = calculated_differences.difference
#        FROM calculated_differences
#        WHERE dim_date_times.date_uuid = calculated_differences.date_uuid;               
#    """))
#    conn.commit()

# Final step is to use the difference data to calculate the average difference over each year

with engine.connect() as conn:
    result = conn.execute(text("""  
        SELECT EXTRACT(YEAR FROM timestamp_column) AS year, AVG(EXTRACT(EPOCH FROM difference)) AS average_difference_in_seconds
        FROM dim_date_times
        WHERE difference IS NOT NULL
        GROUP BY EXTRACT(YEAR FROM timestamp_column)
        ORDER BY average_difference_in_seconds DESC;               
    """))

    for row in result:
        print(f"Year: {row[0]}, Hours: {row[1]//3600}, Minutes: {(row[1]%3600)//60}, Seconds: {int(row[1]%60)}, Milliseconds: {int(row[1]%1*100)}")
