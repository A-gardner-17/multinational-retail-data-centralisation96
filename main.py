import pandas as pd

from data_extraction import DataExtractor
from database_utils import DatabaseConnector
from data_cleaning import DataCleaning

def users():
    #get users
    df = extractor.read_rds_table(dbconnector, "legacy_users")

    #clean users
    df = cleaning.clean_user_data(df)


    #upload users
    dbconnector.upload_to_db(df, "dim_users")

def card_details():
    #get card details
    pdf_path = "https://data-handling-public.s3.eu-west-1.amazonaws.com/card_details.pdf"
    dfs = extractor.retrieve_pdf_data(pdf_path)

    #clean card details
    dfs = cleaning.clean_card_data(dfs)

    #upload card details
    dbconnector.upload_to_db(dfs, "dim_card_details")

def stores():
    #Get number of stores
    numStores = "https://aqj7u5id95.execute-api.eu-west-1.amazonaws.com/prod/number_stores"
    api = {'x-api-key':'yFBQbwXe9J3sd6zWVAMrK6lcxxr0q1lr2PT6DDMX'}
    stores = extractor.list_number_of_stores(numStores,api)

    # Retrieve Stores data
    retrieve = "https://aqj7u5id95.execute-api.eu-west-1.amazonaws.com/prod/store_details/"
    dfsd = extractor.retrieve_stores_data(retrieve, stores["number_stores"], api)

    # Clean Stores data
    dfsd = cleaning.clean_store_data(dfsd)

    #upload store data
    dbconnector.upload_to_db(dfsd, "dim_store_details")

def products():
    # product details s3
    s3Type = "csv"
    bucket = "data-handling-public"
    key = "products.csv"

    #Retrieve s3 file
    dfpd = extractor.extract_from_s3(bucket,key,s3Type)

    #cleaning product details
    dfpd = cleaning.convert_product_weights(dfpd)


    #upload product data
    dbconnector.upload_to_db(dfpd, "dim_products")

def orders():
    # Get product orders
    dfpo = extractor.read_rds_table(dbconnector, "orders_table")

    #cleaning product orders
    dfpo = cleaning.clean_orders_data(dfpo)

    #upload orders
    dbconnector.upload_to_db(dfpo, "orders_table")

def events():
    #Get date events data
    s3Type = "json"
    bucket = "data-handling-public"
    key = "date_details.json"
    dfde = extractor.extract_from_s3(bucket,key,s3Type)


    #cleaning date events data
    dfde = cleaning.clean_date_events(dfde)

    #upload date events data
    dbconnector.upload_to_db(dfde, "dim_date_times")


if __name__ == '__main__':
    
    dbconnector = DatabaseConnector('db_creds.yaml')

    extractor = DataExtractor()
    cleaning = DataCleaning()

    users()
    card_details()
    stores()
    products()
    orders()
    events()