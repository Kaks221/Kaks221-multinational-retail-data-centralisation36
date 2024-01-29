import yaml
from sqlalchemy import create_engine
from sqlalchemy.engine import reflection

class DatabaseConnector:
    def __init__(self, yaml_file_path):
        self.yaml_file_path = yaml_file_path
        self.engine = self.init_db_engine()

    def read_db_creds(self):
        with open(self.yaml_file_path, 'r') as file:
            credentials = yaml.safe_load(file)
        return credentials

    def init_db_engine(self):
        creds = self.read_db_creds() #readss the database credentials from the YAML file
        database_url = f"postgresql://{creds['RDS_USER']}:{creds['RDS_PASSWORD']}@{creds['RDS_HOST']}:{creds['RDS_PORT']}/{creds['RDS_DATABASE']}"
        engine = create_engine(database_url)
        return engine
    
    def list_db_tables(self):
        inspector = reflection.Inspector.from_engine(self.engine) #creates a reflection inspector object
        table_names = inspector.get_table_names() #gets a list of table names
        return table_names
    
    def get_engine(self):
        return self.engine