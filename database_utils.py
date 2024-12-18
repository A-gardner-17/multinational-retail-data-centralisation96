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

        return dbEngine

    def list_db_tables(self):

        metadata = MetaData()
        metadata.reflect(bind=self.engine)
        tables = metadata.tables.keys()
        print("Tables in the database:")
        for table in tables:
            print(table)
        return tables
    
    def upload_to_db(self, df, tableName):
        # Connection details
        DATABASE_TYPE = 'postgresql'
        DBAPI = 'psycopg2'
        HOST = 'localhost'
        USER = 'postgres'
        PASSWORD = 'kingsley'
        DATABASE = 'sales_data'
        PORT = 5432
        engine = create_engine(f"{DATABASE_TYPE}+{DBAPI}://{USER}:{PASSWORD}@{HOST}:{PORT}/{DATABASE}")

        df.to_sql(
            name=tableName,      # Name of the new table
            con=engine,                # SQLAlchemy engine
            if_exists='replace',       # Options: 'fail', 'replace', 'append'
            index=False                # Do not write the DataFrame index as a column
        )

        print("DataFrame saved to PostgreSQL.")
        

if __name__ == '__main__':
    
    connect = DatabaseConnector('db_creds.yaml')
