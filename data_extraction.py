import database_utils
import pandas as pd
import tabula
import requests


class DataExtractor(database_utils.DatabaseConnector):

    api_key = {"X-API-key": "yFBQbwXe9J3sd6zWVAMrK6lcxxr0q1lr2PT6DDMX"}
    num_stores_url = "https://aqj7u5id95.execute-api.eu-west-1.amazonaws.com/prod/number_stores"
    store_url = "https://aqj7u5id95.execute-api.eu-west-1.amazonaws.com/prod/store_details/{store_number}"

    def __init__(self):
        self.init_db_engine()
        
    
    def read_rds_table(self, table_name):
        data = pd.read_sql_table(f"{table_name}", self.engine)
        return data
    
## open the above with python in command line
# need to import pandas, put option to show all columns

    def retrieve_pdf_data(self, link):
        pdf_data = tabula.read_pdf(link, output_format='dataframe', stream=True, pages='all')
        return pdf_data

    def list_number_of_stores(self, url=num_stores_url, headers=api_key):
        response = requests.get(url, headers=headers)
        return response.json() # NB number is 451
    

    def retrieve_stores_data(self, url=store_url, headers=api_key):
        json_data = []
        number_stores = range(DataExtractor().list_number_of_stores()["number_stores"])
        for num in number_stores:
            response = requests.get(url.format(store_number=num), headers=headers)
            json_data.append(response.json())
        stores_data = pd.DataFrame(json_data, index=number_stores)
        return stores_data
    
    def retrieve_one_store(self, url=store_url, headers=api_key):
        response = requests.get(url.format(store_number=1), headers=headers)
        return response.json()