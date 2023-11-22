import yaml
import psycopg2
from sqlalchemy import create_engine, inspect

class DatabaseConnector:

    def read_db_cred():
        with open('db_creds.yaml', 'r') as credentials:
            db_cred = yaml.safe_load(credentials)
            return db_cred
    

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
    

    def list_db_tables(self):
        self.init_db_engine()
        with psycopg2.connect(host=self.HOST, user=self.USER, password=self.PASSWORD, dbname=self.DATABASE, port=self.PORT) as conn:
            with conn.cursor() as cur:
                cur.execute("""SELECT table_name FROM information_schema.tables
                                WHERE table_schema = 'public'""")
                for table in cur.fetchall():
                    print(table)

    def upload_to_db(data_frame, table_name):
        pass
