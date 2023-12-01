import data_extraction
import pandas as pd
import numpy as np
from dateutil.parser import parse

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
    

    
    def clean_card_data(self):
        card_data = self.retrieve_pdf_data('https://data-handling-public.s3.eu-west-1.amazonaws.com/card_details.pdf')
        card_data = card_data[0]    # extract dataframe from list

        card_data["date_payment_confirmed"] = pd.to_datetime(card_data["date_payment_confirmed"], errors='coerce')

        card_data = card_data.dropna()  # drops all rows w empty data and makes columns equal

        card_data = card_data.reset_index(drop=True)

        return card_data
    


    def clean_stores_data(self):
        stores_data = self.retrieve_stores_data()

        stores_data = stores_data.drop("lat", axis=1)

        stores_data["continent"].replace({'eeEurope':'Europe', 'eeAmerica': 'America'}, inplace=True)
        stores_data = stores_data[stores_data["continent"].isin(["Europe", "America"])]
        
        stores_data["opening_date"] =  stores_data["opening_date"].apply(parse)
        stores_data["opening_date"] = pd.to_datetime(stores_data["opening_date"], errors='coerce')

        stores_data = stores_data.dropna()

        stores_data = stores_data.drop("index", axis=1)
        stores_data = stores_data.reset_index(drop=True)

        return stores_data
    


    def convert_product_weights(self):
        product_weights = self.extract_from_s3()
        
        def convert(weight):
            if type(weight) == float:
                return weight
            if weight[-2:] == "kg":
                return float(weight[:-2])
            elif weight[-1] == "g" and "x" in weight:
                weight_split = weight.split("x")
                return float(convert(weight_split[-2])) * float(convert(weight_split[-1]))
            elif weight[-1] == "g":
                return float(weight[:-1]) / 1000
            elif weight[-2:] == "ml":
                return float(weight[:-2]) / 1000
            elif weight[-1] == ".":
                return float(convert(weight[:-1]))
            elif weight[-1] == " ":
                return float(convert(weight[:-1]))
            elif weight[-2:] == "oz":
                return float(weight[:-2]) * 0.034
            else:
                return weight
            
        product_weights["weight"] = product_weights["weight"].apply(convert)

        return product_weights



    def clean_products_data(self):
        products_data = self.convert_product_weights()

        products_data = products_data.drop("Unnamed: 0", axis=1)

        products_data["removed"].replace({"Still_avaliable": "Still_available"}, inplace=True)
        products_data = products_data[products_data["removed"].isin(["Still_available", "Removed"])]

        products_data["date_added"] =  products_data["date_added"].apply(parse)
        products_data["date_added"] = pd.to_datetime(products_data["date_added"], errors='coerce')

        return products_data
    

    def clean_orders_data(self):
        orders_data = self.read_rds_table("orders_table")

        orders_data = orders_data.drop(["1", "first_name", "last_name", "level_0", "index"], axis=1)

        return orders_data
    
    def clean_sales_data(self):
        sales_data = self.extract_sales_file()

        sales_data = sales_data[sales_data["time_period"].isin(["Evening", "Midday", "Morning", "Late_Hours"])]
        sales_data = sales_data.reset_index(drop=True)

        return sales_data

    
if __name__ == "__main__":
    cleaner = DataCleaning()
    sales_data = cleaner.clean_sales_data()
    cleaner.upload_to_db(sales_data, "dim_date_times")