import data_extraction
from dateutil.parser import parse
import pandas as pd


class DataCleaning(data_extraction.DataExtractor):
    
    def clean_user_data(self):
        legacy_users_data = self.read_rds_table("legacy_users")

        legacy_users_data.country_code.replace({'GGB':'GB'}, inplace=True) # (replaces GGB w GB)
        legacy_users_data = legacy_users_data[legacy_users_data.country_code.isin(["GB", "DE", "US"])] # removes junk data

        # makes date_time columns conform
        legacy_users_data["date_of_birth"] =  legacy_users_data["date_of_birth"].apply(parse)
        legacy_users_data["date_of_birth"] = pd.to_datetime(legacy_users_data["date_of_birth"], errors='coerce')

        legacy_users_data["join_date"] =  legacy_users_data["join_date"].apply(parse)
        legacy_users_data["join_date"] = pd.to_datetime(legacy_users_data["join_date"], errors='coerce')
            
        return legacy_users_data
    
    def clean_card_data(self):
        pass