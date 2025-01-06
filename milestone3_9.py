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
        ALTER TABLE orders_table
        ADD CONSTRAINT fk_users FOREIGN KEY (user_uuid) REFERENCES dim_users(user_uuid);
        ALTER TABLE orders_table
        ADD CONSTRAINT fk_store FOREIGN KEY (store_code) REFERENCES dim_store_details(store_code);
        ALTER TABLE orders_table
        ADD CONSTRAINT fk_product FOREIGN KEY (product_code) REFERENCES dim_products(product_code);

        ALTER TABLE orders_table
        ADD CONSTRAINT fk_dates FOREIGN KEY (date_uuid) REFERENCES dim_date_times(date_uuid);
    """))
    conn.commit()

# Issue with Key (card_number)=(3529023891650490) is not present in table "dim_card_details"
#        ALTER TABLE orders_table
#        ADD CONSTRAINT fk_card FOREIGN KEY (card_number) REFERENCES dim_card_details(card_number);

with engine.connect() as conn:
    conn.execute(text("""
        ALTER TABLE orders_table
        ADD CONSTRAINT fk_card FOREIGN KEY (card_number) REFERENCES dim_card_details(card_number);
    """))
    conn.commit()