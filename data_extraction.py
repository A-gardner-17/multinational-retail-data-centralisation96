from database_utils import DatabaseConnector
from sqlalchemy import create_engine, text, MetaData


class DataExtractor():
    def __init__(self):        
        pass
    def read_rds_table(self, dbconnector, table):
        engine = dbconnector.get_engine()
        query = f"SELECT * FROM {table}"
        df = pd.read_sql(query, con=connExtract.engine)
        return df


connExtract = DatabaseConnector('db_creds.yaml')

extractor = DataExtractor()
extractor.read_rds_table(connExtract, "legacy_users")
#read_rds_table(connExtract)
