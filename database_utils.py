import yaml
import pandas as pd
from sqlalchemy import create_engine, inspect


class DatabaseConnector:

    # credentials are stored in a yaml file so this opens it
    def read_db_cred():
        with open('db_creds.yaml', 'r') as credentials:
            db_cred = yaml.safe_load(credentials)
            return db_cred
    
    # connects to a database on AWS  and makes an engine object for requests
    def init_db_engine(self):
        creds = DatabaseConnector.read_db_cred()
        self.DATABASE_TYPE = 'postgresql'
        self.DBAPI = 'psycopg2'
        self.USER = creds['RDS_USER']
        self.PASSWORD = creds['RDS_PASSWORD']
        self.HOST = creds['RDS_HOST']
        self.PORT = creds['RDS_PORT']
        self.DATABASE = creds['RDS_DATABASE']
        self.engine = create_engine(f"{self.DATABASE_TYPE}+{self.DBAPI}://{self.USER}:{self.PASSWORD}@{self.HOST}:{self.PORT}/{self.DATABASE}")
        self.engine.execution_options(isolation_level='AUTOCOMMIT').connect()
    
    # lists table names from database in previous step
    def list_db_tables(self):
        self.init_db_engine()
        inspector = inspect(self.engine)
        table_names = inspector.get_table_names()
        print(table_names)

    # reads my yaml file 
    def read_my_db_cred():
         with open('pword_yaml.yaml', 'r') as my_creds:
            db_cred = yaml.safe_load(my_creds)
            return db_cred
         
    # used to upload to local postgresql database
    # NB i use this to upload directly from terminal
    def upload_to_db(self, my_dframe, table_name):
        my_pword = DatabaseConnector.read_my_db_cred()
        self.engine = create_engine(f"{self.DATABASE_TYPE}+{self.DBAPI}://{'postgres'}:{my_pword}@{'localhost'}:{5432}/{'sales_data'}")
        my_dframe.to_sql(f'{table_name}', self.engine, if_exists='replace')
