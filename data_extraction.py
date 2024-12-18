from database_utils import DatabaseConnector
from sqlalchemy import create_engine, text
import pandas as pd
from data_cleaning import DataCleaning
import numpy as np
from datetime import datetime
import tabula
import requests
import boto3
from io import StringIO

class DataExtractor():
    def __init__(self):        
        pass

    def read_rds_table(self, dbconnector, table):
        engine = dbconnector.engine
        query = f"SELECT * FROM {table}"
        df = pd.read_sql(query, con=engine)
        return df
    
    def retrieve_pdf_data(self, pdf_path):
        dfs = tabula.read_pdf(pdf_path, pages='all', multiple_tables=True, guess = False)
        combined_df = pd.concat(dfs, ignore_index=True)
        return combined_df
    
    def list_number_of_stores(self, numStores, api):
        response = requests.get(numStores, headers=api)
        if response.status_code == 200:
        # Parse the JSON response
            data = response.json()
            print("Data fetched successfully!")
        else:
            print(f"Failed to fetch data. HTTP Status Code: {response.status_code}")
            
        return data
    
    def retrieve_stores_data(self, retrieve, stores):
        storeData = []
        for store_number in range(stores):
            base_url = f"{retrieve}{store_number}"
            response = requests.get(base_url, headers=api)
            if response.status_code == 200:
                storeData.append(response.json())
        df = pd.DataFrame(storeData)
        return df
    
    def extract_from_s3(self, bucket, key, s3Type):
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
    


dbconnector = DatabaseConnector('db_creds.yaml')

extractor = DataExtractor()

#get users
df = extractor.read_rds_table(dbconnector, "legacy_users")

cleaning = DataCleaning()

#clean users
df = cleaning.clean_user_data(df)


#upload users
dbconnector.upload_to_db(df, "dim_users")

#get card details
pdf_path = "https://data-handling-public.s3.eu-west-1.amazonaws.com/card_details.pdf"

dfs = extractor.retrieve_pdf_data(pdf_path)

#print(dfs.head())

#dfs.to_csv('credit.csv', index=False)

#clean card details
dfs = cleaning.clean_card_data(dfs)

#upload card details
dbconnector.upload_to_db(dfs, "dim_card_details")

#Get number of stores
numStores = "https://aqj7u5id95.execute-api.eu-west-1.amazonaws.com/prod/number_stores"
api = {'x-api-key':'yFBQbwXe9J3sd6zWVAMrK6lcxxr0q1lr2PT6DDMX'}
stores = extractor.list_number_of_stores(numStores,api)

# Retrieve Stores data
retrieve = "https://aqj7u5id95.execute-api.eu-west-1.amazonaws.com/prod/store_details/"
dfsd = extractor.retrieve_stores_data(retrieve, stores["number_stores"])

# Clean Stores data
dfsd = cleaning.clean_store_data(dfsd)

#dfsd.to_csv('stores.csv', index=False)

#upload store data
dbconnector.upload_to_db(dfsd, "dim_store_details")

#product details s3
# Bucket is data-handling-public
# Key is products.csv
s3Type = "csv"
bucket = "data-handling-public"
key = "products.csv"

#Retrieve s3 file
dfpd = extractor.extract_from_s3(bucket,key,s3Type)
#print(dfpd.head())
print(dfpd.shape)
#dfpd.to_csv('products.csv', index=False)

#cleaning product details
dfpd = cleaning.convert_product_weights(dfpd)

print(dfpd.shape)

#upload product data
dbconnector.upload_to_db(dfpd, "dim_products")

# Get product orders
dfpo = extractor.read_rds_table(dbconnector, "orders_table")

print(dfpo.shape)
#print(dfpo.head())

#cleaning product orders
dfpo = cleaning.clean_orders_data(dfpo)

print(dfpo.head())

#upload orders
dbconnector.upload_to_db(dfpo, "orders_table")

#Get date events data
s3Type = "json"
bucket = "data-handling-public"
key = "date_details.json"
dfde = extractor.extract_from_s3(bucket,key,s3Type)


#cleaning date events data
dfde = cleaning.clean_date_events(dfde)

#upload date events data
dbconnector.upload_to_db(dfde, "dim_date_times")