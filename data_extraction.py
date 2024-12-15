from database_utils import DatabaseConnector
from sqlalchemy import create_engine, text
import pandas as pd


class DataExtractor():
    def __init__(self):        
        pass
    def read_rds_table(self, dbconnector, table):
        engine = dbconnector.engine
        query = f"SELECT * FROM {table}"
        df = pd.read_sql(query, con=engine)
        return df


dbconnector = DatabaseConnector('db_creds.yaml')

extractor = DataExtractor()
df = extractor.read_rds_table(dbconnector, "legacy_users")
print(df.head())
#read_rds_table(connExtract)
