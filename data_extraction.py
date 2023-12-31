import database_utils
import pandas as pd
import tabula
import requests
import boto3


class DataExtractor(database_utils.DatabaseConnector):

    # variables to be used for api
    api_key = {"X-API-key": "yFBQbwXe9J3sd6zWVAMrK6lcxxr0q1lr2PT6DDMX"}
    num_stores_url = "https://aqj7u5id95.execute-api.eu-west-1.amazonaws.com/prod/number_stores"
    store_url = "https://aqj7u5id95.execute-api.eu-west-1.amazonaws.com/prod/store_details/{store_number}"
    s3_address = "s3://data-handling-public/products.csv"

    # this step may not be necessary but i used it when setting up and since
    def __init__(self):
        self.init_db_engine()
        
    # reads database from AWS
    # this one is for dim_users
    def read_rds_table(self, table_name):
        data = pd.read_sql_table(f"{table_name}", self.engine)
        return data

    # this one is used for dim_card_details
    # reads a pdf from internet using tabula
    def retrieve_pdf_data(self, link):
        pdf_data = tabula.read_pdf(link, output_format='dataframe', pages='all')
        return pdf_data

    # this one lists the number of stores from an API and serves the next step
    def list_number_of_stores(self, url=num_stores_url, headers=api_key):
        response = requests.get(url, headers=headers)
        return response.json() # NB number is 451
    
    # this one makes multiple requests to an API and puts them in a dataframe
    # for dim_store_details
    def retrieve_stores_data(self, url=store_url, headers=api_key):
        json_data = []
        number_stores = range(DataExtractor().list_number_of_stores()["number_stores"])
        for num in number_stores:
            response = requests.get(url.format(store_number=num), headers=headers)
            json_data.append(response.json())
        stores_data = pd.DataFrame(json_data, index=number_stores)
        return stores_data
    
    # this one connects directly to an AWS bucket using boto3
    # for dim_products
    def extract_from_s3(self, address=s3_address):
        split_ad = address.split("/")
        bucket = split_ad[2]
        file = split_ad[3]
        s3 = boto3.client('s3')
        s3.download_file(bucket, file, "/home/plainsman/retail_data_centralization" + "/" + file)    
        with open("/home/plainsman/retail_data_centralization/" + "/" + file) as data:
            products_data = pd.read_csv(data)
            return products_data
        
    # this one downloads a json from a link
    # for orders_table
    def extract_sales_file(self):
        response = requests.get("https://data-handling-public.s3.eu-west-1.amazonaws.com/date_details.json")
        sales_data = pd.DataFrame(response.json())
        return sales_data