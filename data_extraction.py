from database_utils import DatabaseConnector
from sqlalchemy import create_engine, text
import pandas as pd
from data_cleaning import DataCleaning
import numpy as np


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
print(df.shape)
#rows_with_nulls = df[df.isnull().any(axis=1)]
#print(rows_with_nulls)
rows_with_nulls = df[df.isnull().any(axis=1)]
print(rows_with_nulls)
print(df.isnull().values.any())
# Replace "NULL" with NaN (pandas recognizes NaN as the null data type)
df.replace("NULL", np.nan, inplace=True)
print(df.isnull().values.any())

# Count null values in each column
print(df.isnull().sum())

# Get rows with null values
rows_with_nulls = df[df.isnull().any(axis=1)]
print(rows_with_nulls)

# Drop rows with any null values
df_cleaned = df.dropna()

print(df_cleaned.shape)

# Convert 'join_data' column to datetime
df_cleaned['join_date'] = pd.to_datetime(df_cleaned['join_date'], format='%Y-%m-%d', errors='coerce')

#df_cleaned['join_date'] = pd.to_datetime(df_cleaned['join_date'], errors='coerce')
invalid_dates = df_cleaned[df_cleaned['join_date'].isna()]
print("Rows with invalid dates:")
print(invalid_dates)

df_cleaned2 = df_cleaned.dropna()

print(df_cleaned2.shape)
input()

# Validate 'join_date' column
#df_cleaned['date_of_birth'] = pd.to_datetime(df_cleaned['date_of_birth'], format='%Y-%m-%d', errors='coerce')
#df_cleaned['date_of_birth'] = pd.to_datetime(df_cleaned['date_of_birth'], errors='coerce')

# Find invalid dates
invalid_dates = df_cleaned[df_cleaned['date_of_birth'].isna()]
print("Rows with invalid dates:")
print(invalid_dates)

cleaning = DataCleaning()

df = cleaning.clean_user_data(df)

