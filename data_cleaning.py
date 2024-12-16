import numpy as np
import pandas as pd

class  DataCleaning():
    def __init__(self):
        pass

    def clean_user_data(self,df):
        #remove nulls
        rows_with_nulls = df[df.isnull().any(axis=1)]
        #print(rows_with_nulls)
        #print(df.isnull().values.any())
        df.replace("NULL", np.nan, inplace=True)
        df = df.dropna()
        print(df.shape)

        #drop rows with invalid data using country code
        valid_codes = ["US", "GB", "DE", "GGB"]

        # Drop rows where 'country_code' is not in the valid_codes list
        df = df[df['country_code'].isin(valid_codes)]

        #Dates
        #invalid_dates = df_filtered[df_filtered['join_date'].isna()]
        df['join_date'] = pd.to_datetime(df['join_date'], format='mixed')

        print(df)
        input()
        return df

