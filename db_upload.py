import yaml
import pandas as pd
from sqlalchemy import create_engine

class DatabaseUploader:
    def __init__(self, yaml_file_path):
        with open(yaml_file_path, 'r') as file:
            self.credentials = yaml.safe_load(file)
        self.engine = self.create_engine()

    def create_engine(self):
        user = self.credentials['RDS_USER']
        password = self.credentials['RDS_PASSWORD']
        host = self.credentials['RDS_HOST']
        port = self.credentials['RDS_PORT']
        database = self.credentials['RDS_DATABASE']
        return create_engine(f'postgresql://{user}:{password}@{host}:{port}/{database}')

    def upload_to_db(self, df, table_name):
        df.to_sql(name=table_name, con=self.engine, if_exists='replace', index=False)

