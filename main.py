from database_utils import DatabaseConnector
from data_extraction import DataExtractor
from data_cleaning import DataCleaning
from db_upload import DatabaseUploader

#GENERAL
local_creds_path = r'C:\Users\KBrob\OneDrive\AiCore Data Engineering\multinational-retail-data-centralisation36\local_creds.yaml'
db_uploader = DatabaseUploader(local_creds_path) #initialises DatabaseUploader with local PostgreSQL credentials

aws_creds_path = r'C:\Users\KBrob\OneDrive\AiCore Data Engineering\multinational-retail-data-centralisation36\db_creds.yaml'
db_connector = DatabaseConnector(aws_creds_path) #instance of DatabaseConnector with the path to the YAML file

#AWS SECTION
table_names = db_connector.list_db_tables() #uses the instance to list all the tables in the RDS database
print(table_names)

user_data_table = 'legacy_users'
data_extractor = DataExtractor(db_connector) #creates an instance of DataExtractor, passing in the db_connector
user_data_df = data_extractor.read_rds_table(user_data_table) #uses the DataExtractor instance to read the user data table
cleaned_user_data_df = DataCleaning.clean_user_data(user_data_df)
print(len(cleaned_user_data_df))
print(cleaned_user_data_df.head())
db_uploader.upload_to_db(cleaned_user_data_df, 'dim users')

#PDF SECTION
pdf_url = 'https://data-handling-public.s3.eu-west-1.amazonaws.com/card_details.pdf'
pdf_extractor = DataExtractor(None)
card_df = pdf_extractor.retrieve_pdf_data(pdf_url)
cleaned_card_df = DataCleaning.clean_card_data(card_df)
print(len(cleaned_card_df))
print(cleaned_card_df.head())
db_uploader.upload_to_db(cleaned_card_df, 'dim_card_details')

#S3 CSV BUCKET SECTION
s3_csv = 's3://data-handling-public/products.csv'
data_extractor = DataExtractor(None) 
products_df = data_extractor.extract_from_s3_csv(s3_csv)
kg_products_df = DataCleaning.convert_product_weights(products_df)
cleaned_products_df = DataCleaning.clean_products_data(kg_products_df)
print(len(cleaned_products_df))
print(cleaned_products_df.head())
db_uploader.upload_to_db(cleaned_products_df, 'dim_products')

#ORDERS SECTION
orders_table = 'orders_table'
orders_extractor = DataExtractor(db_connector) 
orders_data_df = orders_extractor.read_rds_table(orders_table)
cleaned_orders_df = DataCleaning.clean_orders_data(orders_data_df)
print(len(cleaned_orders_df))
print(cleaned_orders_df.head())
db_uploader.upload_to_db(cleaned_orders_df, 'orders_table')

#S3 JSON BUCKET SECTION
s3_json = 'https://data-handling-public.s3.eu-west-1.amazonaws.com/date_details.json'
data_extractor = DataExtractor(None)
sales_df = data_extractor.extract_from_s3_json(s3_json)
cleaned_sales_df = DataCleaning.clean_sales_data(sales_df)
print(cleaned_sales_df.head())
db_uploader.upload_to_db(cleaned_sales_df, 'dim_date_times')

#API SECTION
api_key = 'yFBQbwXe9J3sd6zWAMrK6lcxxr0q1lr2PT6DDMX'
api_extractor = DataExtractor(None)
number_of_stores = api_extractor.list_number_of_stores(api_key)
print(number_of_stores)