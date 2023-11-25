import data_extraction
from dateutil.parser import parse
import pandas as pd


class DataCleaning(data_extraction.DataExtractor):
    
    def clean_user_data(self):
        ### step 1 - cleaning legacy_users table
        legacy_users_data = self.read_rds_table("legacy_users")

        legacy_users_data["country_code"].replace({'GGB':'GB'}, inplace=True) # (replaces GGB w GB)
        legacy_users_data = legacy_users_data[legacy_users_data.country_code.isin(["GB", "DE", "US"])] # removes junk data

        # makes date_time columns conform
        legacy_users_data["date_of_birth"] =  legacy_users_data["date_of_birth"].apply(parse)
        legacy_users_data["date_of_birth"] = pd.to_datetime(legacy_users_data["date_of_birth"], errors='coerce')

        legacy_users_data["join_date"] =  legacy_users_data["join_date"].apply(parse)
        legacy_users_data["join_date"] = pd.to_datetime(legacy_users_data["join_date"], errors='coerce')
        
        legacy_users_data = legacy_users_data.drop("index", axis=1)
        
        return legacy_users_data
    
    def clean_other_data(self):
        # NB here temporarily bc user data only needed atm

        ### step 2 - cleaning legacy_store_details table

        legacy_stores_data = self.read_rds_table("legacy_store_details")

        legacy_stores_data = legacy_stores_data.drop("lat", axis=1) # drops a column w only 11 entries
        legacy_stores_data = legacy_stores_data[legacy_stores_data["store_type"].isin(["Local", "Super Store", "Mall Kiosk", "Outlet", "Web Portal"])]
        legacy_stores_data["continent"].replace({'eeEurope':'Europe', 'eeAmerica': 'America'}, inplace=True)

        legacy_stores_data["opening_date"] =  legacy_stores_data["opening_date"].apply(parse)
        legacy_stores_data["opening_date"] = pd.to_datetime(legacy_stores_data["opening_date"], errors='coerce')
        legacy_stores_data = legacy_stores_data.dropna() # drops a row where latitude is empty

        legacy_stores_data = legacy_stores_data.drop("index", axis=1)

        orders_data = self.read_rds_table("orders_table")

        orders_data = orders_data.drop("1", axis=1)

    
    def clean_card_data(self):
        pass