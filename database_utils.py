import yaml
import psycopg2
from sqlalchemy import create_engine

class DatabaseConnector:
    def read_db_cred():
        with open('db_creds.yaml', 'r') as credentials:
            db_cred = yaml.safe_load(credentials)
            return db_cred
    
    def init_db_engine(self):
        creds = DatabaseConnector.read_db_cred()
        DATABASE_TYPE = 'postgresql'
        DBAPI = 'psycopg2'
        USER = creds['RDS_USER']
        PASSWORD = creds['RDS_PASSWORD']
        HOST = creds['RDS_HOST']
        PORT = creds['RDS_PORT']
        DATABASE = creds['RDS_DATABASE']
        engine = create_engine(f"{DATABASE_TYPE}+{DBAPI}://{USER}:{PASSWORD}@{HOST}:{PORT}/{DATABASE}")
    
    def list_db_tables(self):
        self.init_db_engine()
        with psycopg2.connect(host=HOST, user=USER, password=PASSWORD, dbname=DATABASE, port=PORT) as conn:
            with conn.cursor() as cur:
                cur.execute('''SELECT table_name FROM information_schema.tables
                                WHERE table_schema = public''')
                for table in cur.fetchall():
                    print(table)




connector = DatabaseConnector()

