from database_utils import DatabaseConnector
from data_cleaning import DataCleaning
import boto3
from datetime import datetime
from io import StringIO
import numpy as np
import pandas as pd
import requests
from sqlalchemy import create_engine, text
import tabula

class DataExtractor():
    def __init__(self):        
        pass

    def read_rds_table(self, dbconnector, table):
        """
        Method that will read in data from database into a Pandas DataFrame  

        Args:
            dbconnector: connection details for RDS database.
            table (str): Name of table

        Returns:
            df (pandas.DataFrame): DataFrame contain data from specified table.
        """
        try:
            engine = dbconnector.engine
            query = f"SELECT * FROM {table}"
            df = pd.read_sql(query, con=engine)
            return df
        except Exception as e:
            print(f"Error reading table '{table}': {e}")
            return None
    
    def retrieve_pdf_data(self, pdf_path):
        """
        Method that will read in data from a pdf file into a Pandas DataFrame  

        Args:
            pdf_path: path for pdf file.

        Returns:
            df (pandas.DataFrame): DataFrame contain data from specified pdf file.
        """
        # Extract table from a pdf file using the Tabula library - all pages, multiple tables
        # Guess set to false to stop tabula guessing the table structure
        try:
            dfs = tabula.read_pdf(pdf_path, pages='all', multiple_tables=True, guess = False)
            # multiple tables combined into a DataFrame with indexes ignored 
            combined_df = pd.concat(dfs, ignore_index=True)
            return combined_df
        except Exception as e:
            print(f"Error processing PDF file: {e}")
            return None
    
    def list_number_of_stores(self, numStores, api):
        """
        Method that will find the number of stores using an API  

        Args:
            numStores (str/link): number of stores endpoints.
            API (dict): API Key dictionary

        Returns:
            data (int): Number of stores.
        """
        # HTTP Get request using the requests library with URL and dictionary for the headers
        response = requests.get(numStores, headers=api)
        if response.status_code == 200:
        # Parse the JSON response
            data = response.json()
            print("Data fetched successfully!")
        else:
            print(f"Failed to fetch data. HTTP Status Code: {response.status_code}")
            
        return data
    
    def retrieve_stores_data(self, retrieve, stores, api):
        """
        Method that will read in store data using an API  

        Args:
            retrieve (str): stores endpoint.
            stores (int): Number of stores.
            API (dict): API Key dictionary

        Returns:
            df (pandas.DataFrame): DataFrame containing store data.
        """
        # Creation of a blank list and using the number of stores to iterate through the data
        # HTTP Get request using the requests library with URL and dictionary for the header
        # Return data appended to the list and converted to a DataFrame once all stores retrieved
        storeData = []
        for store_number in range(stores):
            base_url = f"{retrieve}{store_number}"
            response = requests.get(base_url, headers=api)
            if response.status_code == 200:
                storeData.append(response.json())
        df = pd.DataFrame(storeData)
        return df
    
    def extract_from_s3(self, bucket, key, s3Type):
        """
        Method that will read in data from a S3 bucket on AWS  

        Args:
            bucket (str): bucket name.
            key (str): Filename.
            S3Type (str): file type.

        Returns:
            df (pandas.DataFrame): DataFrame contain data from specified file.
        """
        # boto3 library used to retrieve file from a S3 bucket on AWS
        # get_object retrieves the file from the specified bucket
        # Key holds the path to the file
        # Reads and decodes data before creating the DataFrame 
        try:
            s3 = boto3.client('s3')
            if s3Type == "csv":
                csv_obj = s3.get_object(Bucket=bucket, Key=key)
                csv_data = csv_obj['Body'].read().decode('utf-8')
                df = pd.read_csv(StringIO(csv_data))
            elif s3Type == "json":
                # Create an S3 client
                s3 = boto3.client("s3", region_name="eu-west-1")

                # Download the JSON file as a string
                json_obj = s3.get_object(Bucket=bucket, Key=key)
                json_data = json_obj["Body"].read().decode("utf-8")

                # Load the JSON data into a Pandas DataFrame
                df = pd.read_json(StringIO(json_data))
            return df
        except Exception as e:
            print(f"Unexpected error while extracting from S3: {e}")
