# Data Centralization Project

## Project Overview
This project is centered on extracting, transforming, and loading data from various sources into a centralized database.

## Data Sources
- AWS RDS for relational data storage.
- S3 Buckets for storage of CSV and JSON files.
- Public APIs for fetching real-time data.
- PDF Documents for data extraction from reports.

## Project Structure
- `data_extraction.py`
- `data_cleaning.py`
- `db_upload.py`
- `database_utils.py`
- `main.py`
- `README.md`

## Python Libraries
- pandas
- numpy
- sqlalchemy
- requests
- boto3
- tabula-py

To run the entire project, run main.py