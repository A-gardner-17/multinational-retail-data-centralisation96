import numpy as np
import pandas as pd
from datetime import datetime

class  DataCleaning():
    def __init__(self):
        pass

    def clean_user_data(self,df):
        """
        Method that will clean the user dataset.
        Removes NULL values and converts the join_date column in to a datetime data type.

        Args:
            df (Pandas DataFrame): User dataset.
        
        Returns:
            df: Cleaned dataset.

        """
        #find and remove null values

        df.replace("NULL", np.nan, inplace=True)
        df = df.dropna()

        #drop rows with invalid data using country code
        valid_codes = ["US", "GB", "DE", "GGB"]

        # Drop rows where 'country_code' is not in the valid_codes list
        df = df[df['country_code'].isin(valid_codes)]

        # Convert join_date to datetime datatype after checking
        df['join_date'] = df['join_date'].apply(self.date_checking)

        return df
    
    def clean_card_data(self, dfs):
        """
        Method that will clean the card details dataset.
        Removes NULL values, removes duplicate card numbers, removes non-numerical card numbers 
        and converts the date_payment_confirmed column in to a datetime data type.

        Args:
            dfs (Pandas DataFrame): Card Details dataset.
        
        Returns:
            dfs: Cleaned dataset.

        """
        #find and remove null values

        dfs.replace("NULL", np.nan, inplace=True)

        #remove duplicates
        dfs = dfs.drop_duplicates(subset=['card_number'])

        # Remove non numerical card numbers - need to remove ? first

        dfs['card_number'] = dfs['card_number'].apply(str)
        dfs['card_number'] = dfs['card_number'].str.replace('?','')


        # Convert join_date to datetime datatype after checking
        dfs['date_payment_confirmed'] = dfs['date_payment_confirmed'].apply(self.date_checking)

        dfs.dropna(how='any',inplace= True)
        return dfs
    
    def clean_store_data(self,dfsd):
        """
        Method that will clean the store dataset.
        Removes NULL values, converts the opening_date in to a datetime data type, 
        strips away symbols, letters, and white spaces from staff_number column. 

        Args:
            dfsd (Pandas DataFrame): Store dataset.
        
        Returns:
            dfsd: Cleaned dataset.

        """
        #find and remove null values

        dfsd.replace("NULL", np.nan, inplace=True)

        dfsd.dropna(how='all', inplace= True,  ignore_index=True)

        #drop rows with invalid data using country code
        valid_codes = ["US", "GB", "DE"]

        # Drop rows where 'country_code' is not in the valid_codes list
        dfsd = dfsd[dfsd['country_code'].isin(valid_codes)]

        # Convert join_date to datetime datatype handling a mix of different date formats
        dfsd['opening_date'] = pd.to_datetime(dfsd['opening_date'], format='mixed')

        # Remove non numeric characters from staff number column
        # Matches any character that is not a digit (\d), ^ inside [] negates the character set
        dfsd["staff_numbers"] = dfsd["staff_numbers"].str.replace(r"[^\d]", "", regex=True)

        return dfsd
    
    def convert_product_weights(self, dfpd):
        """
        Method that will clean the product details dataset.
        Removes NULL values by dropping invalid categories, calls the method convert_to_kg 

        Args:
            dfpd (Pandas DataFrame): Product Details dataset.

        Returns:
            dfpd: Cleaned dataset.
        """
        #drop data with invalid category
        valid_codes = ["toys-and-games", "pets", "homeware", "sports-and-leisure", "health-and-beauty", "food-and-drink", "diy"]
        dfpd = dfpd[dfpd['category'].isin(valid_codes)]
        
        # convert weights - calls convert_to_kg method

        dfpd["weight"] = dfpd["weight"].apply(self.convert_to_kg)
        return dfpd

    def convert_to_kg(self, weight):
        """
        Method that will convert the weight column to kg. 

        Args:
            weight (str): Weight values.

        Returns:
            weight: weight values in kg.
        """
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
        """
        Method that will clean the product orders data frame by removing unwanted columns. 

        Args:
            dfpo (Pandas DataFrame): Product Order dataset.

        Returns:
            dfpo: Cleaned dataset.
        """
        # remove unwanted columns
        dfpo.drop(columns=["first_name", "last_name", "1"], inplace = True)

        return dfpo
    
    def clean_date_events(self,dfde):
        """
        Method that will clean the sale details dataset.
        Removes NULL values and convert values in columns "day", "month", and "year" into numeric values.  

        Args:
            dfde (Pandas DataFrame): Sale Details dataset.

        Returns:
            dfde: Cleaned dataset.
        """
        #drop data with invalid time_period
        valid_codes = ["Evening","Morning","Midday","Late_Hours"]
        dfde = dfde[dfde['time_period'].isin(valid_codes)]

        # convert day month year into numeric values
        # errors='coerce' to convert invalid entries to NaT
        dfde["day"] = pd.to_numeric(dfde["day"], errors="coerce")
        dfde["month"] = pd.to_numeric(dfde["month"], errors="coerce")
        dfde["year"] = pd.to_numeric(dfde["year"], errors="coerce")

        return dfde
        
    def date_checking(self, date):
        """
        Method that will check the date formats.

        Args:
            date (str): Dates in text format.

        Returns:
            date (date/time): Converted using pd.to_datetime.
        """
        try:
            return pd.to_datetime(date, format='%B %Y %d')  # First format
        except ValueError:
            try:
                return pd.to_datetime(date, format='%Y %B %d')  # Another format
            except ValueError:
                try:
                    return pd.to_datetime(date, format='%Y-%m-%d')  # Another format
                except ValueError:
                    try: 
                        return pd.to_datetime(date, format='%Y/%m/%d')
                    except ValueError:
                        return pd.NaT  # Fallback for invalid dates

