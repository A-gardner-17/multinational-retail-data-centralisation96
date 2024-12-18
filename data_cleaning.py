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
    
    #clean product details
    def convert_product_weights(self, dfpd):
        
        #drop data with invalid category
        valid_codes = ["toys-and-games", "pets", "homeware", "sports-and-leisure", "health-and-beauty", "food-and-drink", "diy"]
        dfpd = dfpd[dfpd['category'].isin(valid_codes)]
        #print(dfpd.shape)
        
        # convert weights

        dfpd["weight"] = dfpd["weight"].apply(self.convert_to_kg)
        return dfpd

    def convert_to_kg(self, weight):
        weight = weight.rstrip(".") # strip full stop
        if "kg" in weight:
            return float(weight.replace("kg",""))
        elif "g" in weight:
            weight = weight.replace("g","")
            if "x" in weight:
                val = weight.split("x") # split to list
                weight = int(val[0]) * int(val[1]) # multiply values
            return float(weight)/1000
        elif "ml" in weight:
            return float(weight.replace("ml",""))/1000
    
    def clean_orders_data(self,dfpo):
        # remove unwanted columns
        dfpo.drop(columns=["first_name", "last_name", "1"], inplace = True)

        return dfpo
    
    def clean_date_events(self,dfde):
        #drop data with invalid time_period
        valid_codes = ["Evening","Morning","Midday","Late_Hours"]
        dfde = dfde[dfde['time_period'].isin(valid_codes)]

        #print(dfde.shape)

        #convert day month year into numeric values

        dfde["day"] = pd.to_numeric(dfde["day"], errors="coerce")
        dfde["month"] = pd.to_numeric(dfde["month"], errors="coerce")
        dfde["year"] = pd.to_numeric(dfde["year"], errors="coerce")

        return dfde
        



