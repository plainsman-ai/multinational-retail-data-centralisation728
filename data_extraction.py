import database_utils
import pandas as pd

class DataExtractor(database_utils.DatabaseConnector):

    def __init__(self):
        self.init_db_engine()

    def read_rds_table(self, table_name):
        self.data = pd.read_sql_table(f"{table_name}", self.engine)
        return self.data

    
extractor = DataExtractor()
extractor.list_db_tables()
table = input("Enter table name: ")
data = extractor.read_rds_table(table)
print(data.head())



