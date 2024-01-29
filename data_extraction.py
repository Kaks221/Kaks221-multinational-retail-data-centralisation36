import pandas as pd
import tabula
import requests
import boto3

class DataExtractor:
    def __init__(self, db_connector):
        self.db_connector = db_connector #initialises with an instance of DatabaseConnector

    def read_rds_table(self, table_name):
        engine = self.db_connector.get_engine() #uses the database connector to get the engine
        data_frame = pd.read_sql_table(table_name, engine)
        return data_frame 

    def retrieve_pdf_data(self, pdf_url):
        df_list = tabula.read_pdf(pdf_url, pages='all', multiple_tables=True) #uses tabula to read the PDF file at the given URL
        combined_df = pd.concat(df_list, ignore_index=True) #combines all tables into a single DataFrame
        return combined_df
    
    def list_number_of_stores(self, api_key):
        url = 'https://aqj7u5id95.execute-api.eu-west-1.amazonaws.com/prod/number_stores'
        headers = {'x-api-key': api_key}
        response = requests.get(url, headers=headers)
        if response.status_code == 200:
            return response.json()
        else:
            response.raise_for_status() #raises an HTTPError if the API request failed

    def extract_from_s3_csv(self, s3_path):
        bucket_name = s3_path.split('/')[2]
        file_key = '/'.join(s3_path.split('/')[3:]) #parses the bucket name and file key from the S3 path
        s3_client = boto3.client('s3') #initialises the S3 client
        obj = s3_client.get_object(Bucket=bucket_name, Key=file_key) #downloads the file object from S3
        df = pd.read_csv(obj['Body'])
        return df

    def extract_from_s3_json(self, json_url):
        response = requests.get(json_url)
        response.raise_for_status()
        df = pd.read_json(response.text)
        return df
