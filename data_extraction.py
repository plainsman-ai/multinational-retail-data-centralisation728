import database_utils
import pandas as pd
import tabula

class DataExtractor(database_utils.DatabaseConnector):

    # def __init__(self):
        # self.init_db_engine()

    def read_rds_table(self, table_name):
        data = pd.read_sql_table(f"{table_name}", self.engine)
        self.engine.close()
        return data
    
## open the above with python in command line
# need to import pandas, put option to show all columns

    def retrieve_pdf_data(self, link):
        pass






