# Multinational Retail Data Centralisation
This example project is for a multinational company that sells various goods across the globe.

Currently, their sales data is spread across many different data sources making it not easily accessible or analysable by current members of the team.

In an effort to become more data-driven, your organisation would like to make its sales data accessible from one centralised location.

The first goal will be to produce a system that stores the current company data in a database so that it's accessed from one centralised location and acts as a single source of truth for sales data.

The central database will be used to get up-to-date metrics for the business. 

As part of this project I set up a GitHub repo, set up a new database to hold the cleaned data in using PostgreSQL, set up three Python scripts to extract different data sources, clean the data extracted and connect and upload data to the database. Details of these scripts can be found below.

Milestone 3 involved creating the database schema, updating data types and making sure all the table relationships were set up with the appropriate primary and foreign keys. Details of this can be accessed from the **File Structure of the Project - Milestone 3** from the contents.

# Table of Contents
 1. [Project Description](#project-description)
 2. [Installation Instructions](#installation-instructions)
 3. [Usage Instructions](#usage-instructions)
 4. [File Structure of the Project - Milestone 2](#file-structure-of-the-project-milestone-2)
 5. [File Structure of the Project - Milestone 3](#file-structure-of-the-project-milestone-3)
 6. [File Structure of the Project - Milestone 4](#file-structure-of-the-project-milestone-4)
 7. [License Information](#license-information)

# Project Description
This project focuses on the extraction of data from a variety of data sources, including an AWS database in the cloud, a PDF document in an AWS S3 bucket, extraction of data using an API, a CSV and JSON file in an AWS S3 bucket.

Each extraction process was setup and tested to ensure the correct data was stored in the pandas DataFrame. This process reinforced my understanding of the different data sources and how these can be accessed correctly in Python.

For each of the extracted DataFrames the data was cleaned using the cleaning requirement document. This process enabled me to test a variety of cleaning techniques.

Once each cleaning process was complete for each dataset the number of rows were checked and the data was uploaded to the specified tables in the PostgreSQL database.

Once all the tables had been uploaded the next step involved changing the data types of the columns in each of the tables. This was completed by running SQL statements, including finding the maximum length of fields and setting appropriate data types. After these updates the primary keys were set in the tables with a dim prefix and then the foreign keys were set in the orders_table. A number of issues were found during this process which required a revisit to some of the cleaning methods, in particular with data cleaning and a new method specifically for the cleaning of dates was created. There were also issues when setting the foreign key for card_number as there were records in the orders_table that did not exist in the dim_card_details table. This was resolved by updating the cleaning method for the card_details_table.

# Installation Instructions
The project uses the standard Python installation.

# Usage Instructions
The project can be tested using the main.py

# File Structure of the Project Milestone 2
**database_utils.py** DatabaseConnector Class which initialises fileName, database credentials loaded from a local yaml file (read_db_creds), connection to the RDS database (init_db_engine) and list of tables included in the database (list_db_tables). The upload_to_db function connects to the local database and creates the various tables updating each with the cleaned data from the various dataframes.

**data_extraction.py** DataExtractor Class this contains the following methods:
1. **read_rds_table**: accepts the connection details for the RDS database and table name and reads the related table from the database into a Pandas DataFrame.
2. **retrieve_pdf_data**: accepts the path for a PDF file and reads in the data into a Pandas DataFrame.
3. **list_number_of_stores**: API. Accepts the the number of stores endpoint and header dictionary and returns the number of stores to extract.
4. **retrieve_stores_data**: API. Accepts the store endpoint and the total number of stores (returned from list_number_of_stores). Extracts data for each store and appends detail to a list before converting to a Pandas DataFrame and returning.
5. **extract_from_s3**: S3 bucket on AWS - extracts CSV file for product details and JSON file for date events data. In both cases reading and returning a Pandas DataFrame.

**data_cleaning.py** DataCleaning Class this contains the following methods:
1. **clean_user_data**: accepts the user DataFrame. Removes NULL values and converts the join_date column in to a datetime data type. Returns the cleaned dataset.
2. **clean_card_data**: accepts the card details DataFrame. Removes NULL values, removes duplicate card numbers, removes non-numerical card numbers and converts the date_payment_confirmed column in to a datetime data type. Returns the cleaned dataset.
3. **clean_store_data**: accepts the store DataFrame. Removes NULL values, converts the opening_date in to a datetime data type, strips away symbols, letters, and white spaces from staff_number column. Returns the cleaned dataset.
4. **convert_product_weights**: accepts the product details DataFrame. Removes NULL values and calls the convert_to_kg method. Returns the cleaned dataset.
5. **convert_to_kg**: accepts the individual weight values and converts each into kg. Returns the converted weight.
6. **clean_orders_data**: accepts the product orders DataFrame. Removes unwanted columns and returns the cleaned dataset.
7. **clean_date_events**: accepts the sale details DataFrame. Removes NULL values and convert values in columns "day", "month", and "year" into numeric values. Returns the cleaned dataset.
8. **date_checking**: accepts individual dates and converts into datetime data type. Returns the convered data.

# File Structure of the Project Milestone 3
**milestone3_1 - milestone3_9.py**
SQL Statements to set datatypes for columns in each table plus the setting of primary and foreign key fields.
1. **milestone3_1.py**: Casting for the orders_table. This required finding out the maximum length of some columns. This was achieved using the following and was used across a number of different columns in different tables:

> query = text("SELECT MAX(LENGTH(card_number::TEXT)) AS max_length FROM orders_table")

> with engine.connect() as conn:

>     result = conn.execute(query).scalar()

>     print(f"Maximum length: {result}")

2. **milestone3_2.py**: Casting for the dim_users table.
3. **milestone3_3.py**: Casting for the dim_store_details table.
4. **milestone3_4.py**: Cleaning of the dim_products table. This included removal of the £ sign:

> UPDATE dim_products SET product_price = REPLACE(product_price, '£', '')

Adding of a new column called weight_class:

> ALTER TABLE dim_products ADD COLUMN weight_class VARCHAR(14)

Then populate this new column based on the weight range:

> UPDATE dim_products
        SET weight_class = CASE
            WHEN weight < 2 THEN 'Light'
            WHEN weight < 40 THEN 'Mid_Sized'
            WHEN weight < 140 THEN 'Heavy'
            WHEN weight >= 140 THEN 'Truck_Required'
            ELSE 'NULL'
        END

5. **milestone3_5.py**: Casting for the dim_products table. The removed column renamed to still_available before changing the data type:

> UPDATE dim_products
        SET still_available = CASE
            WHEN still_available = 'Still_avaliable' THEN 'true'
            ELSE 'false'
        END

6. **milestone3_6.py**: Casting for the dim_date_times table.
7. **milestone3_7.py**: Casting for the dim_card_details table. To resolve an issue with the casting of the card_number column the following SQL was executed:

> UPDATE dim_card_details
       SET card_number = TRIM(TO_CHAR(CAST(card_number AS NUMERIC), '9999999999999999999999'))

8. **milestone3_8.py**: Creation of primary keys. The various primary keys were set using the format shown below:

> ALTER TABLE dim_date_times
        ADD CONSTRAINT pk_date_times PRIMARY KEY (date_uuid)

9. **milestone3_9.py**: Creation of foreign keys in the orders_table. The foreign keys were set using the format shown below:

> ALTER TABLE orders_table
        ADD CONSTRAINT fk_users FOREIGN KEY (user_uuid) REFERENCES dim_users(user_uuid)

# File Structure of the Project Milestone 4
Once the database structure was finalised a number of queries were run the details of which can be seen below (these queries can be found in **milestone4_1.py - milestone4_9.py** and have also been included in the queries folder).
1. **milestone4_1.py**: Number of stores each country has:

> SELECT country_code, count (*) AS total_no_stores
FROM dim_store_details
GROUP BY country_code;

Output:
Country Code: DE, Total Stores: 141
Country Code: US, Total Stores: 34
Country Code: GB, Total Stores: 266

GB has one additional store as it includes the web portal record. The SQL was updated as follows:

> SELECT country_code, count (*) AS total_no_stores
        FROM dim_store_details
        WHERE store_type != 'Web Portal'
        GROUP BY country_code;

Output:  
Country Code: DE, Total Stores: 141  
Country Code: US, Total Stores: 34  
Country Code: GB, Total Stores: 265  

2. **milestone4_2.py**: Which locations currently have the most stores:

> SELECT locality, count (*) AS total_no_stores
FROM dim_store_details
GROUP BY locality
HAVING COUNT(*) >= 10
ORDER BY total_no_stores DESC;

Output:  
Country Code: Chapletown, Total Stores: 14  
Country Code: Belper, Total Stores: 13  
Country Code: Bushey, Total Stores: 12  
Country Code: Exeter, Total Stores: 11  
Country Code: Arbroath, Total Stores: 10  
Country Code: High Wycombe, Total Stores: 10  
Country Code: Rutherglen, Total Stores: 10  

3. **milestone4_3.py**:
4. **milestone4_4.py**:
5. **milestone4_5.py**:
6. **milestone4_6.py**:
7. **milestone4_7.py**:
8. **milestone4_8.py**:
9. **milestone4_9.py**:

# License Information
This program has been developed as part of the AiCore training programme.