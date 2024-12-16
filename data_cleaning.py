import numpy as np
import pandas as pd

class  DataCleaning():
    def __init__(self):
        pass

    def clean_user_data(self,df):
        #find and remove null values
        #rows_with_nulls = df[df.isnull().any(axis=1)]

        df.replace("NULL", np.nan, inplace=True)
        df = df.dropna()
        #print(df.shape)

        #drop rows with invalid data using country code
        valid_codes = ["US", "GB", "DE", "GGB"]

        # Drop rows where 'country_code' is not in the valid_codes list
        df = df[df['country_code'].isin(valid_codes)]

        #Dates
        df['join_date'] = pd.to_datetime(df['join_date'], format='mixed')

        return df
    
    def clean_card_data(self, dfs):
        #find and remove null values
        #rows_with_nulls = dfs[dfs.isnull().any(axis=1)]

        dfs.replace("NULL", np.nan, inplace=True)

        #remove duplicates
        dfs = dfs.drop_duplicates(subset=['card_number'])

        #remove non numerical card numbers - need to remove ? first
        # Convert 'value' to numeric and drop rows with non-numeric values

        dfs['card_number'] = dfs['card_number'].apply(str)
        dfs['card_number'] = dfs['card_number'].str.replace('?','')
        dfs['card_number'] = pd.to_numeric(dfs['card_number'], errors='coerce')
  
        dfs.dropna(how='any',inplace= True)

        #Dates
        dfs['date_payment_confirmed'] = pd.to_datetime(dfs['date_payment_confirmed'], format='mixed')

        return dfs
    
    def called_clean_store_data(self,dfsd):
        #find and remove null values

        #find nulls
        dfsd.replace("NULL", np.nan, inplace=True)

        dfsd.dropna(how='all', inplace= True,  ignore_index=True)

        #drop rows with invalid data using country code
        valid_codes = ["US", "GB", "DE"]

        # Drop rows where 'country_code' is not in the valid_codes list
        dfsd = dfsd[dfsd['country_code'].isin(valid_codes)]

        #Convert date
        dfsd['opening_date'] = pd.to_datetime(dfsd['opening_date'], format='mixed')

        # Remove non numeric characters from staff number column
        # Matches any character that is not a digit (\d), ^ inside [] negates the character set
        dfsd["staff_numbers"] = dfsd["staff_numbers"].str.replace(r"[^\d]", "", regex=True)

        return dfsd

