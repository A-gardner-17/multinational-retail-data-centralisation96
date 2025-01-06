import pandas as pd
import psycopg2
from sqlalchemy import create_engine, text, MetaData
import yaml

class DatabaseConnector():
    def __init__(self,fileName):
        """
        Initialises an instance of the class, setting up database connection properties.

        Args:
            fileName (str): The name of the file containing database credentials.

        Attributes:
            fileName (str): Stores the provided file name for database credential lookup.
            credentials (dict): The database credentials read from the specified file.
            engine (sqlalchemy.engine.base.Engine): The database engine initialised using the credentials.
            listDb (list): A list of table names in the connected database.
        """
        self.fileName = fileName
        self.credentials = self.read_db_creds()
        self.engine = self.init_db_engine()
        self.listDb = self.list_db_tables()

    def read_db_creds(self):
        """
        
        Method reads database credentials from a YAML file,
        and returns the credentials as a dictionary.

        Returns:
            credentials (dict): A dictionary containing the database credentials.

        """
    
        with open(self.fileName, 'r') as file:
            credentials = yaml.safe_load(file) 
        return credentials
    
    def init_db_engine(self):
        """
        Method Initialises and returns a SQLAlchemy database engine.

        Returns:
            sqlalchemy.engine.base.Engine: A SQLAlchemy engine instance for database operations.
        """ 
        dialect = "postgresql"
        db_url = (
                f"{dialect}://{self.credentials['RDS_USER']}:{self.credentials['RDS_PASSWORD']}"
                f"@{self.credentials['RDS_HOST']}:{self.credentials['RDS_PORT']}/{self.credentials['RDS_DATABASE']}"
            )
        # Create and return the SQLAlchemy engine
        dbEngine = create_engine(db_url)

        return dbEngine

    def list_db_tables(self):
        """
        Method to return the print and return the tables in the RDS database.
        Uses the MetaData class from SQLAlchemy to examine the database schema  

        Returns:
            tables: List of tables in the RDS database.
        """
        metadata = MetaData()
        metadata.reflect(bind=self.engine)
        tables = metadata.tables.keys()
        print("Tables in the database:")
        for table in tables:
            print(table)
        return tables
    
    def upload_to_db(self, df, tableName, config_file = "db_creds_local.yaml"):
        """
        Uploads a Pandas DataFrame to a PostgreSQL database as a table.
        Establishes a connection to a PostgreSQL database using SQLAlchemy and
        uploads the given DataFrame to a specified table. If the table
        already exists, it replaces the existing table.

        Args:
            df (pandas.DataFrame): The DataFrame to be uploaded to the database.
            tableName (str): The name of the table to be created or replaced in the database.
            config_file (str): Path to the YAML configuration file.
        """
        # Connection details  
     
        with open(config_file, 'r') as file:
            config = yaml.safe_load(file)
    
        DATABASE_TYPE = config['DATABASE_TYPE']
        DBAPI = config['DBAPI']
        HOST = config['HOST']
        USER = config['USER']
        PASSWORD = config['PASSWORD']
        DATABASE = config['DATABASE']
        PORT = config['PORT']

        engine = create_engine(f"{DATABASE_TYPE}+{DBAPI}://{USER}:{PASSWORD}@{HOST}:{PORT}/{DATABASE}")

        df.to_sql(
            name=tableName,      # Name of the new table
            con=engine,                # SQLAlchemy engine
            if_exists='replace',       # Options: 'fail', 'replace', 'append'
            index=False                # Do not write the DataFrame index as a column
        )

        print("DataFrame saved to PostgreSQL", tableName)

    def connect_db_local(self,config_file = "db_creds_local.yaml"):
        # Connection details  
     
        with open(config_file, 'r') as file:
            config = yaml.safe_load(file)
    
        DATABASE_TYPE = config['DATABASE_TYPE']
        DBAPI = config['DBAPI']
        HOST = config['HOST']
        USER = config['USER']
        PASSWORD = config['PASSWORD']
        DATABASE = config['DATABASE']
        PORT = config['PORT']

        engine = create_engine(f"{DATABASE_TYPE}+{DBAPI}://{USER}:{PASSWORD}@{HOST}:{PORT}/{DATABASE}")

        return engine