import yaml
from sqlalchemy import create_engine, text, MetaData
import psycopg2
import pandas as pd


class DatabaseConnector():
    def __init__(self,fileName):
        self.fileName = fileName
        self.credentials = self.read_db_creds()
        self.engine = self.init_db_engine()
        self.listDb = self.list_db_tables()

    def read_db_creds(self):
        with open(self.fileName, 'r') as file:
            credentials = yaml.safe_load(file) 
        return credentials
    
    def init_db_engine(self):
        # WILL NEED TO UPDATE THIS TO READ FROM THE DICTIONARY
        dialect = "postgresql"
        db_url = (
                f"{dialect}://{self.credentials['RDS_USER']}:{self.credentials['RDS_PASSWORD']}"
                f"@{self.credentials['RDS_HOST']}:{self.credentials['RDS_PORT']}/{self.credentials['RDS_DATABASE']}"
            )
            # Create and return the SQLAlchemy engine
        dbEngine = create_engine(db_url)

        # try:
        #     with dbEngine.connect() as connection:
        #         print("Database connection successful")
        # except Exception as e:
        #     print(f"Database connection failed: {e}")

        return dbEngine

    def list_db_tables(self):
        #Alternatively, SQLAlchemy's ORM layer has a metadata reflection feature to load table information automatically:
        #Metadata Reflection: Use MetaData.reflect to retrieve information about all tables in the database.
        #Table Names: The metadata.tables dictionary contains all table names as keys.
        metadata = MetaData()
        metadata.reflect(bind=self.engine)
        tables = metadata.tables.keys()
        print("Tables in the database:")
        for table in tables:
            print(table)
        return tables

if __name__ == '__main__':
    
    connect = DatabaseConnector('db_creds.yaml')

#try:
#    with dbEngine.connect() as connection:
#        print("Database connection successful!")
#except Exception as e:
#d    print(f"Database connection failed: {e}")