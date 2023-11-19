import yaml

class DatabaseConnector:
    def read_db_creds():
        with open('db_creds.yaml', 'r') as credentials:
            db_cred = yaml.safe_load(credentials)
            return db_cred
    
    