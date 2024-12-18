# Multinational Retail Data Centralisation
This example project is for a multinational company that sells various goods across the globe.

Currently, their sales data is spread across many different data sources making it not easily accessible or analysable by current members of the team.

In an effort to become more data-driven, your organisation would like to make its sales data accessible from one centralised location.

The first goal will be to produce a system that stores the current company data in a database so that it's accessed from one centralised location and acts as a single source of truth for sales data.

The central database will be used to get up-to-date metrics for the business. 

As part of this project I set up a GitHub repo, set up a new database to hold the cleaned data in using PostgreSQL, set up three Python scripts to extract different data sources, clean the data extracted and connect and upload data to the database. Details of these scripts can be found below.

# Table of Contents
 1. [Project Description](#project-description)
 2. [Installation Instructions](#installation-instructions)
 3. [Usage Instructions](#usage-instructions)
 4. [File Structure of the Project](#file-structure-of-the-project)
 5. [License Information](#license-information)

# Project Description
This project focuses on the extraction of data from a variety of data sources, including an AWS database in the cloud, a PDF document in an AWS S3 bucket, extraction of data using an API, a CSV and JSON file in an AWS S3 bucket.

Each extraction process was setup and tested to ensure the correct data was stored in the pandas DataFrame. This process reinforced my understanding of the different data sources and how these can be accessed correctly in Python.

For each of the extracted DataFrames the data was cleaned using the cleaning requirement document. This process enabled me to test a variety of cleaning techniques.

Once each cleaning process was complete for each dataset the number of rows were checked and the data was uploaded to the specified tables in the PostgreSQL database.

XXX complete other aspects here

# Installation Instructions
The project uses the standard Python installation.

# Usage Instructions
The project can be tested using the XXXXXXXX.py

# File Structure of the Project
**database_utils.py** DatabaseConnector Class which initialises fileName, database credentials loaded from a local yaml file (read_db_creds), connection to the RDS database (init_db_engine) and list of tables included in the database (list_db_tables). The upload_to_db function connects to the local database and creates the various tables updating each with the cleaned data from the various dataframes.

**data_extraction.py** DataExtractor Class this contains the following methods:
1. read_rds_table: accepts the connection details for the RDS database and table name and reads the related table from the database into a Pandas DataFrame.
2. retrieve_pdf_data: accepts the path for a PDF file and reads in the data into a Pandass DataFrame.
3. list_number_of_stores: API. Accepts the the number of stores endpoint and header dictionary and returns the number of stores to extract.
4. retrieve_stores_data: API. Accepts the store endpoint and the total number of stores (returned from list_number_of_stores). Extracts data for each store and appends detail to a list before converting to a Pandas DataFrame and returning.
5. extract_from_s3: S3 bucket on AWS - extracts CSV file for product details and JSON file for date events data. In both cases reading and returning a Pandas DataFrame.

**data_cleaning.py** DataCleaning Class this contains the following methods:
1. clean_user_data: accepts the user DataFrame. Removes NULL values and converts the join_date column in to a datetime data type. Returns the cleaned dataset.
2. clean_card_data: accepts the card details DataFrame. Removes NULL values, removes duplicate card numbers, removes non-numerical card numbers and converts the date_payment_confirmed column in to a datetime data type. Returns the cleaned dataset.
3. clean_store_data: accepts the store DataFrame. Removes NULL values, converts the opening_date in to a datetime data type, strips away symbols, letters, and white spaces from staff_number column. Returns the cleaned dataset.
4. convert_product_weights: accepts the product details DataFrame. Removes NULL values and calls the convert_to_kg method. Returns the cleaned dataset.
5. convert_to_kg: accepts the individual weight values and converts each into kg. Returns the converted weight.
6. clean_orders_data: accepts the product orders DataFrame. Removes unwanted columns and returns the cleaned dataset.
7. clean_date_events: accepts the sale details DataFrame. Removes NULL values and convert values in columns "day", "month", and "year" into numeric values. Returns the cleaned dataset.

# License Information
This program has been developed as part of the AiCore training programme.